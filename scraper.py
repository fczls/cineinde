#!/usr/bin/env python3
from __future__ import annotations

"""
scraper.py — Le Comedia, Lyon
Scrape le programme depuis https://www.cinema-comedia.fr/programme-accessible/
Produit un fichier programme.json consommé par le site frontend.

Usage : python scraper.py [--debug] [--output /chemin/vers/programme.json]
Cron  : 0 1 * * 3  /usr/bin/python3 /srv/comedia/scraper.py >> /var/log/comedia-scraper.log 2>&1
"""

import re
import json
import logging
import argparse
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from html.parser import HTMLParser

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
# cinema-comoedia.com (orthographe officielle) — fallback cinema-comedia.fr
URL_PROGRAMME    = "https://www.cinema-comoedia.com/programme-accessible/"
URL_OMDB_BASE    = "https://www.omdbapi.com/"
OMDB_API_KEY     = "822f09ad"   # https://www.omdbapi.com/apikey.aspx

OUTPUT_DEFAULT   = Path(__file__).parent / "programme.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; ComediaBot/1.0; "
        "+https://www.cinema-comedia.fr)"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9",
}

# Jours en français → numéro ISO (lundi=1)
JOURS_FR = {
    "lundi": 1, "mardi": 2, "mercredi": 3, "jeudi": 4,
    "vendredi": 5, "samedi": 6, "dimanche": 7,
}

MOIS_FR = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("comedia")


# ─────────────────────────────────────────────
# HTTP HELPER
# ─────────────────────────────────────────────
def fetch(url: str, timeout: int = 15) -> str:
    """Télécharge une URL et retourne le contenu texte (UTF-8)."""
    req = Request(url, headers=HEADERS)
    try:
        with urlopen(req, timeout=timeout) as r:
            charset = "utf-8"
            ct = r.headers.get_content_charset()
            if ct:
                charset = ct
            return r.read().decode(charset, errors="replace")
    except HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} en fetchant {url}") from e
    except URLError as e:
        raise RuntimeError(f"Erreur réseau pour {url}: {e.reason}") from e


# ─────────────────────────────────────────────
# HTML PARSER — structure cinema-comoedia.com (Gatsby)
# ─────────────────────────────────────────────
# Structure réelle (inspectée mars 2026) :
#   <h2>Du 4 au 10 mars 2026</h2>  — période de la semaine
#   <div class="widgetWrapper"> alternance :
#     - <h5>Titre du film</h5> (titre seul)
#     - <div class="widgetWrapper css-4itxma"><div><p>Réalisé par...</p>
#       <p>Film produit en X en YYYY, d'une durée de 1h39. Film en version française.</p>
#       <p>Synopsis...</p>
#       <p><strong>Séances prévues mercredi, samedi 11h15, 13h35...</strong></p>
#
# Les séances sont en texte inline : "Séances prévues [jours] à [heures]"
# Ex: "mercredi, samedi 11h15, 13h35" ou "tous les jours à 20h50"
# ─────────────────────────────────────────────

class SimpleHTMLParser(HTMLParser):
    """
    Parser HTML minimaliste basé sur la stdlib.
    Construit un arbre de nœuds {tag, attrs, text, children}.
    """

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = {"tag": "root", "attrs": {}, "text": "", "children": []}
        self._stack = [self.root]
        # Tags vides (pas de fermeture)
        self._void = {
            "area", "base", "br", "col", "embed", "hr", "img",
            "input", "link", "meta", "param", "source", "track", "wbr",
        }

    def handle_starttag(self, tag, attrs):
        node = {
            "tag": tag,
            "attrs": dict(attrs),
            "text": "",
            "children": [],
        }
        self._stack[-1]["children"].append(node)
        if tag not in self._void:
            self._stack.append(node)

    def handle_endtag(self, tag):
        if len(self._stack) > 1:
            # Dépile jusqu'au bon tag (tolérant aux erreurs)
            for i in range(len(self._stack) - 1, 0, -1):
                if self._stack[i]["tag"] == tag:
                    self._stack = self._stack[:i]
                    break

    def handle_data(self, data):
        if self._stack:
            self._stack[-1]["text"] += data


def parse_html(html: str) -> dict:
    p = SimpleHTMLParser()
    p.feed(html)
    return p.root


def find_nodes(node: dict, *, tag: str = None, cls: str = None,
               id_: str = None) -> list:
    """Recherche récursive de nœuds par tag/class/id."""
    results = []
    needle_tag = tag.lower() if tag else None
    needle_cls = cls.lower() if cls else None

    def _walk(n):
        match_tag = (needle_tag is None) or (n["tag"] == needle_tag)
        node_cls  = n["attrs"].get("class", "").lower()
        match_cls = (needle_cls is None) or (needle_cls in node_cls)
        node_id   = n["attrs"].get("id", "").lower()
        match_id  = (id_ is None) or (id_.lower() in node_id)
        if match_tag and match_cls and match_id:
            results.append(n)
        for child in n["children"]:
            _walk(child)

    _walk(node)
    return results


def text_of(node: dict) -> str:
    """Extrait tout le texte (récursif) d'un nœud."""
    parts = [node["text"]]
    for c in node["children"]:
        parts.append(text_of(c))
    return " ".join(p.strip() for p in parts if p.strip())


# ─────────────────────────────────────────────
# PARSERS SPÉCIFIQUES AU SITE
# ─────────────────────────────────────────────

def parse_date_fr(s: str) -> date | None:
    """
    Tente de parser une date en français, ex :
      "Mercredi 12 mars", "12 mars 2025", "12/03/2025"
    """
    s = s.strip().lower()

    # Format JJ/MM/AAAA
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))

    # Format "12 mars 2025" ou "12 mars"
    m = re.search(r"(\d{1,2})\s+(\w+)(?:\s+(\d{4}))?", s)
    if m:
        jour_n = int(m.group(1))
        mois_s = m.group(2)
        annee  = int(m.group(3)) if m.group(3) else date.today().year
        mois_n = MOIS_FR.get(mois_s)
        if mois_n:
            try:
                return date(annee, mois_n, jour_n)
            except ValueError:
                pass

    # Format "Mercredi 12 mars" → on ignore le nom du jour
    m = re.search(
        r"(?:lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)"
        r"\s+(\d{1,2})\s+(\w+)(?:\s+(\d{4}))?",
        s,
    )
    if m:
        jour_n = int(m.group(1))
        mois_s = m.group(2)
        annee  = int(m.group(3)) if m.group(3) else date.today().year
        mois_n = MOIS_FR.get(mois_s)
        if mois_n:
            try:
                return date(annee, mois_n, jour_n)
            except ValueError:
                pass

    return None


def parse_heure(s: str) -> str | None:
    """
    Normalise une heure vers HH:MM, ex :
      "14h30", "14h", "14:30", "14H30", "14 h 30"
    """
    s = s.strip().lower().replace(" ", "")
    m = re.search(r"(\d{1,2})[h:](\d{2})", s)
    if m:
        return f"{int(m.group(1)):02d}:{m.group(2)}"
    m = re.search(r"(\d{1,2})h$", s)
    if m:
        return f"{int(m.group(1)):02d}:00"
    return None


def detect_version(s: str) -> str:
    """Détecte VF / VOSTFR / VO dans une chaîne."""
    s = s.upper()
    if "VOSTFR" in s or "VOST" in s:
        return "VOSTFR"
    if "VO" in s:
        return "VO"
    if "VF" in s:
        return "VF"
    return "VF"  # défaut


def _parse_week_period(html: str) -> dict[int, date] | None:
    """Extrait 'Du 4 au 10 mars 2026' et retourne {jour_iso: date} pour la semaine."""
    m = re.search(r"Du\s+(\d+)\s+au\s+(\d+)\s+(\w+)\s+(\d{4})", html, re.I)
    if not m:
        return None
    deb, fin, mois_s, annee = int(m.group(1)), int(m.group(2)), m.group(3), int(m.group(4))
    mois_n = MOIS_FR.get(mois_s.lower())
    if not mois_n:
        return None
    try:
        first = date(annee, mois_n, deb)
    except ValueError:
        return None
    # Map isoweekday (1=lundi..7=dimanche) -> date
    result = {}
    for i in range(7):
        d = first + timedelta(days=i)
        result[d.isoweekday()] = d
        if d.day == fin:
            break
    return result


def _parse_seances_texte(texte: str, week_dates: dict[int, date], version: str) -> list[dict]:
    """
    Parse "Séances prévues mercredi, samedi 11h15, 13h35..." en liste de séances.
    Formats: "jours à heures", "jours heures", "tous les jours à X", "Jour JJ mois à X"
    """
    from html import unescape
    texte = unescape(texte).replace("\xa0", " ")
    seances = []
    # Heures : 11h15, 13h35, 14h00, etc.
    heures = re.findall(r"\b(\d{1,2})[h:](\d{2})\b", texte)
    heures = list(dict.fromkeys(f"{int(h):02d}:{m}" for h, m in heures))

    # Cas "Mercredi 4 mars à 14h00" (date explicite)
    for m in re.finditer(
        r"(?:lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)\s+(\d{1,2})\s+(\w+)(?:\s+à)?\s*(\d{1,2})[h:](\d{2})",
        texte, re.I
    ):
        d = parse_date_fr(f"{m.group(0)[:50]}")
        if d and heures:
            h = f"{int(m.group(3)):02d}:{m.group(4)}"
            seances.append({"date": d.isoformat(), "heure": h, "version": version})
        continue

    if not week_dates or not heures:
        return seances

    # "tous les jours" → lundi à dimanche (1-7)
    if re.search(r"tous\s+les\s+jours", texte, re.I):
        for iso, d in week_dates.items():
            for h in heures:
                seances.append({"date": d.isoformat(), "heure": h, "version": version})
        return seances

    # "tous les jours sauf dimanche à X, et dimanche à Y"
    m_except = re.search(
        r"tous\s+les\s+jours\s+sauf\s+(\w+)\s+à\s+([^,]+)(?:,\s*et\s+\w+\s+à\s+([^.\s]+))?",
        texte, re.I
    )
    if m_except:
        jour_exclu = JOURS_FR.get(m_except.group(1).lower())
        h_norm = re.findall(r"\b(\d{1,2})[h:](\d{2})\b", m_except.group(2))
        h_dimanche = re.findall(r"\b(\d{1,2})[h:](\d{2})\b", m_except.group(3) or "")
        for iso, d in week_dates.items():
            if iso == jour_exclu:
                for h in h_dimanche or h_norm:
                    hh = f"{int(h[0]):02d}:{h[1]}"
                    seances.append({"date": d.isoformat(), "heure": hh, "version": version})
            else:
                for h in h_norm:
                    hh = f"{int(h[0]):02d}:{h[1]}"
                    seances.append({"date": d.isoformat(), "heure": hh, "version": version})
        return seances

    # "mercredi, samedi 11h15, 13h35" ou "mercredi, samedi et dimanche à 11h10"
    jours_match = re.findall(
        r"(?:lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)(?:\s*,\s*|\s+et\s+)?",
        texte, re.I
    )
    # Alternative: extraire les noms de jours
    jours_set = set()
    for j in ("lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"):
        if re.search(rf"\b{j}\b", texte, re.I):
            jours_set.add(JOURS_FR[j])

    if not jours_set:
        return seances

    for iso in jours_set:
        if iso in week_dates:
            for h in heures:
                seances.append({
                    "date": week_dates[iso].isoformat(),
                    "heure": h,
                    "version": version,
                })
    return seances


def parse_programme(html: str) -> list[dict]:
    """
    Parse le HTML de /programme-accessible/ (cinema-comoedia.com, structure Gatsby).
    Retourne une liste de films avec seances.
    """
    films = []
    week_dates = _parse_week_period(html)
    if week_dates:
        log.info(f"Semaine : {min(week_dates.values())} → {max(week_dates.values())}")
    else:
        log.warning("Période 'Du X au Y mois' non trouvée — séances sans date")

    # Structure : <h5>Titre</h5> suivi de <div> avec <p>...</p> contenant Réalisé par, Film produit, Séances prévues
    # On découpe par blocs h5 (titres) — chaque h5 = film ou événement
    h5_pattern = re.compile(r"<h5[^>]*>([^<]+)</h5>", re.I)
    # Contenu jusqu'au prochain widgetWrapper avec h5 ou h4
    blocks = list(h5_pattern.finditer(html))
    for i, m in enumerate(blocks):
        titre = re.sub(r"\s+", " ", m.group(1)).strip()
        if not titre:
            continue
        # Contenu : entre ce h5 et le prochain h5/h4
        start = m.end()
        end = blocks[i + 1].start() if i + 1 < len(blocks) else len(html)
        content = html[start:end]
        # Ignorer les sections (Films français, etc.) et événements sans "Réalisé par"
        if re.search(r"Films\s+(français|étrangers|jeune public)", titre, re.I):
            continue
        if "Réalisé par" not in content and "réalisé par" not in content.lower():
            continue  # Événement, pas un film

        film = _extract_film_comoedia(titre, content, week_dates)
        if film and film.get("titre"):
            films.append(film)

    log.info(f"{len(films)} films extraits")
    return films


def _extract_film_comoedia(titre: str, content: str, week_dates: dict | None) -> dict:
    """Extrait les infos d'un bloc film (titre + content HTML)."""
    from html import unescape
    titre = unescape(titre)
    content_clean = re.sub(r"<[^>]+>", " ", content)
    content_clean = unescape(content_clean).replace("\xa0", " ")
    content_clean = re.sub(r"\s+", " ", content_clean).strip()

    film = {
        "titre": titre,
        "titreOriginal": None,
        "annee": None,
        "realisateur": None,
        "duree": None,
        "genres": [],
        "synopsis": None,
        "imdbId": None,
        "seances": [],
    }

    # Réalisateur
    m_real = re.search(r"Réalisé par ([^.]+?)(?:\.|, avec|$)", content_clean, re.I)
    if m_real:
        film["realisateur"] = m_real.group(1).strip().rstrip(",")

    # Film produit en X en YYYY, d'une durée de 1h39 / 43 minutes
    m_duree = re.search(r"durée (?:de|d') ?(\d{1,2})h(\d{2})", content_clean, re.I)
    if m_duree:
        film["duree"] = int(m_duree.group(1)) * 60 + int(m_duree.group(2))
    else:
        m_duree = re.search(r"durée de (\d{1,2,3}) minutes?", content_clean, re.I)
        if m_duree:
            film["duree"] = int(m_duree.group(1))

    m_annee = re.search(r"en (19\d{2}|20\d{2})", content_clean)
    if m_annee:
        film["annee"] = int(m_annee.group(1))

    # Version
    version = "VF"
    if "sous-titr" in content_clean.lower() or "vostfr" in content_clean.lower() or "vost" in content_clean.lower():
        version = "VOSTFR"
    elif re.search(r"\bvo\b", content_clean.lower()) and "version française" not in content_clean.lower():
        version = "VO"
    elif "version française" in content_clean.lower() or "québécois" in content_clean.lower():
        version = "VF"

    # Synopsis : paragraphe qui n'est ni Réalisé par, ni Film produit, ni Séances prévues
    synop_match = re.search(
        r"Film (?:produit|d['\u2019]animation|documentaire)[^.]+\.[\s]*([A-ZÀ][^.]+?)(?=\s*Séances? prévues|\s*$)",
        content_clean, re.S
    )
    if synop_match:
        s = synop_match.group(1).strip()
        if len(s) > 30 and "Réalisé par" not in s:
            film["synopsis"] = s[:500] + ("…" if len(s) > 500 else "")

    # Séances prévues
    seances_match = re.search(
        r"Séances? prévues?\s+([^.]+)",
        content, re.I | re.S
    )
    if seances_match:
        txt = seances_match.group(0)
        film["seances"] = _parse_seances_texte(txt, week_dates or {}, version)

    # Dédupliquer et trier
    seen = set()
    dedup = []
    for s in film["seances"]:
        key = (s["date"], s["heure"], s["version"])
        if key not in seen:
            seen.add(key)
            dedup.append(s)
    film["seances"] = sorted(dedup, key=lambda x: (x["date"], x["heure"]))

    return film


def _extract_film(node: dict) -> dict:
    """Extrait les infos d'un nœud film."""
    film: dict = {
        "titre": None,
        "titreOriginal": None,
        "annee": None,
        "realisateur": None,
        "duree": None,
        "genres": [],
        "synopsis": None,
        "imdbId": None,
        "seances": [],
    }

    # ── Titre ──
    for tag in ("h2", "h3", "h1", "h4"):
        titres = find_nodes(node, tag=tag)
        if titres:
            film["titre"] = text_of(titres[0]).strip()
            break

    # Titre original (souvent en italique ou dans un span dédié)
    orig_nodes = (
        find_nodes(node, tag="span", cls="titre-original")
        or find_nodes(node, tag="em")
        or find_nodes(node, tag="i")
    )
    if orig_nodes:
        t = text_of(orig_nodes[0]).strip()
        if t and t != film["titre"]:
            film["titreOriginal"] = t

    # ── Infos (réalisateur, année, durée) ──
    info_nodes = (
        find_nodes(node, tag="p", cls="film-info")
        or find_nodes(node, tag="p", cls="infos")
        or find_nodes(node, tag="div", cls="film-info")
        or find_nodes(node, tag="p")
    )
    for info_node in info_nodes[:3]:
        txt = text_of(info_node)
        _extract_meta(film, txt)

    # ── Synopsis ──
    synop_nodes = (
        find_nodes(node, tag="div", cls="synopsis")
        or find_nodes(node, tag="p", cls="synopsis")
        or find_nodes(node, tag="div", cls="description")
    )
    if synop_nodes:
        film["synopsis"] = text_of(synop_nodes[0]).strip()

    # ── Version (VF/VOSTFR) ──
    # Cherche dans les spans/p dédiés ou dans le texte global
    version_defaut = "VF"
    ver_nodes = (
        find_nodes(node, tag="span", cls="version")
        or find_nodes(node, tag="p", cls="version")
        or find_nodes(node, tag="span", cls="vf")
        or find_nodes(node, tag="span", cls="vostfr")
    )
    if ver_nodes:
        version_defaut = detect_version(text_of(ver_nodes[0]))
    else:
        # Fallback : cherche dans tout le texte du nœud
        full_text = text_of(node).upper()
        if "VOSTFR" in full_text or "VOST" in full_text:
            version_defaut = "VOSTFR"
        elif "VO" in full_text and "VF" not in full_text:
            version_defaut = "VO"

    # ── Séances ──
    seance_nodes = (
        find_nodes(node, tag="li", cls="seance")
        or find_nodes(node, tag="div", cls="seance")
        or find_nodes(node, tag="li", cls="horaire")
        or find_nodes(node, tag="span", cls="seance")
        # Si les séances sont juste dans un <ul> générique
        or find_nodes(node, tag="li")
    )

    for s_node in seance_nodes:
        seance = _extract_seance(s_node, version_defaut)
        if seance:
            film["seances"].append(seance)

    # Déduplique les séances
    seen = set()
    dedup = []
    for s in film["seances"]:
        key = (s["date"], s["heure"], s["version"])
        if key not in seen:
            seen.add(key)
            dedup.append(s)
    film["seances"] = sorted(dedup, key=lambda s: (s["date"], s["heure"]))

    return film


def _extract_meta(film: dict, txt: str):
    """Extrait réalisateur, année, durée depuis une ligne de texte."""
    # Durée : "1h30", "1h 30", "90 min", "90min"
    m = re.search(r"(\d{1,2})\s*h\s*(\d{2})", txt, re.I)
    if m and not film["duree"]:
        film["duree"] = int(m.group(1)) * 60 + int(m.group(2))
    else:
        m = re.search(r"(\d{2,3})\s*min", txt, re.I)
        if m and not film["duree"]:
            film["duree"] = int(m.group(1))

    # Année : 4 chiffres entre 1900 et 2099
    m = re.search(r"\b(19\d{2}|20\d{2})\b", txt)
    if m and not film["annee"]:
        film["annee"] = int(m.group(1))

    # Réalisateur : souvent "De Prénom Nom" ou en premier champ avant " · "
    m = re.match(r"^(?:de\s+)?([A-ZÀ-Ü][a-zà-ü]+(?:\s+[A-ZÀ-Ü][a-zà-ü]+)+)", txt.strip())
    if m and not film["realisateur"] and len(m.group(1)) > 4:
        film["realisateur"] = m.group(1)


def _extract_seance(node: dict, version_defaut: str) -> dict | None:
    """Extrait date + heure + version d'un nœud de séance."""
    full = text_of(node)

    # Version spécifique à cette séance ?
    version = version_defaut
    if "VOSTFR" in full.upper() or "VOST" in full.upper():
        version = "VOSTFR"
    elif re.search(r"\bVO\b", full.upper()):
        version = "VO"
    elif re.search(r"\bVF\b", full.upper()):
        version = "VF"

    # Heure
    heure = None
    for h_node in (find_nodes(node, tag="span", cls="heure")
                   or find_nodes(node, tag="span", cls="time")
                   or [node]):
        h = parse_heure(text_of(h_node))
        if h:
            heure = h
            break
    if not heure:
        heure = parse_heure(full)

    # Date
    d = None
    for d_node in (find_nodes(node, tag="span", cls="jour")
                   or find_nodes(node, tag="span", cls="date")
                   or find_nodes(node, tag="time")
                   or [node]):
        # Attribut datetime (HTML5)
        if "datetime" in d_node["attrs"]:
            try:
                d = date.fromisoformat(d_node["attrs"]["datetime"][:10])
                break
            except ValueError:
                pass
        d = parse_date_fr(text_of(d_node))
        if d:
            break

    if not d:
        d = parse_date_fr(full)

    if heure and d:
        return {
            "date": d.isoformat(),
            "heure": heure,
            "version": version,
        }

    # Si on a l'heure mais pas la date, c'est peut-être sur la ligne du titre du film
    # On retourne quand même avec une date nulle, filtrée plus tard
    if heure:
        log.debug(f"Séance sans date : heure={heure}, texte='{full[:80]}'")

    return None


# ─────────────────────────────────────────────
# ENRICHISSEMENT OMDB
# ─────────────────────────────────────────────

def enrich_omdb(films: list[dict]) -> list[dict]:
    """
    Cherche chaque film sur OMDb et complète :
    imdbId, note, genres, synopsis (si manquant), affiche.
    """
    if OMDB_API_KEY == "VOTRE_CLE_OMDB":
        log.warning("Clé OMDb non configurée — enrichissement ignoré")
        return films

    for film in films:
        titre = film.get("titreOriginal") or film.get("titre", "")
        annee = film.get("annee")
        params = f"t={_urlencode(titre)}&apikey={OMDB_API_KEY}&type=movie"
        if annee:
            params += f"&y={annee}"
        url = f"{URL_OMDB_BASE}?{params}"

        try:
            data = json.loads(fetch(url, timeout=8))
            if data.get("Response") == "True":
                film["imdbId"]   = data.get("imdbID")
                film["poster"]   = data.get("Poster")   # URL directe CDN Amazon
                if not film.get("synopsis") and data.get("Plot") not in (None, "N/A"):
                    film["synopsis"] = data["Plot"]
                if not film.get("annee") and data.get("Year"):
                    try:
                        film["annee"] = int(data["Year"][:4])
                    except ValueError:
                        pass
                if not film.get("realisateur") and data.get("Director") not in (None, "N/A"):
                    film["realisateur"] = data["Director"]
                if not film.get("genres") and data.get("Genre") not in (None, "N/A"):
                    film["genres"] = [g.strip() for g in data["Genre"].split(",")]
                if data.get("imdbRating") not in (None, "N/A"):
                    film["imdbRating"] = float(data["imdbRating"])
                log.info(f"  ✓ OMDb : {film['titre']} → {film.get('imdbId')}")
            else:
                log.warning(f"  ✗ OMDb introuvable : {titre} ({annee})")
        except Exception as e:
            log.warning(f"  ✗ OMDb erreur pour {titre}: {e}")

    return films


def _urlencode(s: str) -> str:
    from urllib.parse import quote
    return quote(s, safe="")


# ─────────────────────────────────────────────
# FILTRAGE — ne garder que la semaine courante
# ─────────────────────────────────────────────

def filter_current_week(films: list[dict]) -> list[dict]:
    """
    Garde uniquement les séances entre aujourd'hui et J+6.
    Supprime les films sans séance restante.
    """
    today = date.today()
    limit = today + timedelta(days=7)

    filtered = []
    for film in films:
        seances_ok = [
            s for s in film["seances"]
            if today <= date.fromisoformat(s["date"]) < limit
        ]
        if seances_ok:
            f = dict(film)
            f["seances"] = seances_ok
            filtered.append(f)

    return filtered


# ─────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scraper programme Le Comedia")
    parser.add_argument("--debug",  action="store_true", help="Mode debug verbose")
    parser.add_argument("--dry-run", action="store_true", help="Ne pas écrire le fichier JSON")
    parser.add_argument("--output", default=str(OUTPUT_DEFAULT), help="Chemin du fichier JSON de sortie")
    parser.add_argument("--no-omdb", action="store_true", help="Désactiver l'enrichissement OMDb")
    parser.add_argument("--file", default=None, help="Fichier HTML local (pour test, évite le fetch)")
    parser.add_argument("--no-filter", action="store_true", help="Ne pas filtrer par semaine (pour test)")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("═" * 55)
    log.info(f"Comedia Scraper — {datetime.now().strftime('%A %d %B %Y %H:%M')}")
    log.info("═" * 55)

    # 1. Téléchargement ou lecture fichier
    if args.file:
        log.info(f"Lecture fichier → {args.file}")
        html = Path(args.file).read_text(encoding="utf-8", errors="replace")
    else:
        log.info(f"Fetch → {URL_PROGRAMME}")
        try:
            html = fetch(URL_PROGRAMME)
        except RuntimeError as e:
            log.error(f"Impossible de télécharger la page : {e}")
            sys.exit(1)
    log.info(f"HTML reçu : {len(html):,} caractères")

    # 2. Parsing
    log.info("Parsing HTML…")
    films = parse_programme(html)

    if not films:
        log.error(
            "Aucun film extrait. "
            "La structure HTML a probablement changé. "
            "Lancez avec --debug pour inspecter."
        )
        sys.exit(2)

    # 3. Enrichissement OMDb
    if not args.no_omdb:
        log.info(f"Enrichissement OMDb pour {len(films)} films…")
        films = enrich_omdb(films)

    # 4. Filtrage semaine
    if not args.no_filter:
        films = filter_current_week(films)
    log.info(f"{len(films)} films retenus pour la semaine")

    # 5. Écriture JSON
    output = {
        "generated_at": datetime.now().isoformat(),
        "source": URL_PROGRAMME,
        "films": films,
    }

    if args.dry_run:
        print(json.dumps(output, ensure_ascii=False, indent=2))
    else:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(output, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info(f"✓ Écrit → {out_path} ({out_path.stat().st_size:,} octets)")

    log.info("Terminé.")


if __name__ == "__main__":
    main()
