#!/usr/bin/env python3
from __future__ import annotations

"""
scraper.py — Multi-Cinémas Lyon (Comoedia + Lumière Terreaux/Bellecour/Fourmi)
Scrape les programmes et produit programme.json consommé par le site frontend.

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
URL_LUMIERE_BASE = "https://www.cinemas-lumiere.com/calendrier-general.html"
URL_OMDB_BASE    = "https://www.omdbapi.com/"
URL_TMDB_BASE    = "https://api.themoviedb.org/3/"
import os
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "822f09ad")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

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


def get_last_wednesday() -> date:
    """Retourne le dernier mercredi écoulé (ou aujourd'hui si c'est un mercredi)."""
    today = date.today()
    # isoweekday: lundi=1, mardi=2, mercredi=3, ..., dimanche=7
    days_since_wed = (today.isoweekday() - 3) % 7
    return today - timedelta(days=days_since_wed)


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
    """Détecte VF / VOSTFR / VO / VFST dans une chaîne."""
    s = s.upper()
    if "VOSTFR" in s or "VOST" in s:
        return "VOSTFR"
    if "VFST" in s:
        return "VFST"
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


# ─────────────────────────────────────────────
# SCRAPER COMOEDIA — wrappeur avec source/cinema
# ─────────────────────────────────────────────

def scrape_comoedia(html: str | None = None, file_path: str | None = None) -> list[dict]:
    """Scrape le programme du Comoedia. Retourne les films enrichis de source/cinema."""
    if file_path:
        log.info(f"Lecture fichier Comoedia → {file_path}")
        html_src = Path(file_path).read_text(encoding="utf-8", errors="replace")
    elif html is not None:
        html_src = html
    else:
        log.info(f"Fetch Comoedia → {URL_PROGRAMME}")
        try:
            html_src = fetch(URL_PROGRAMME)
        except RuntimeError as e:
            log.error(f"Impossible de télécharger Comoedia : {e}")
            return []

    log.info(f"Comoedia HTML reçu : {len(html_src):,} caractères")
    log.info("Parsing HTML Comoedia…")
    films = parse_programme(html_src)
    return [{**f, "source": "comoedia", "cinema": "Le Comoedia"} for f in films]


# ─────────────────────────────────────────────
# SCRAPER CINÉMAS LUMIÈRE — calendrier général
#
# Structure réelle de la page (inspectée mars 2026) :
#   <table class="schedule">
#     <tr class="days">               ← entête : <td> vide + <th class="day-title">×7
#       <th><time datetime="YYYY-MM-DD HH:MM:SS">…</time></th>
#     <tr class="cinema striped-background">  ← séparateur de cinéma
#       <th class="sticky"><div>Lumière <svg><use xlink:href="…#logo-terreaux"/></svg></div></th>
#       <td>×7  (vides)
#     <tr class="movie">              ← film
#       <th class="movie-title sticky"><a href="/film/slug.html">Titre</a></th>
#       <td class="schedule">×7
#         <time datetime="YYYY-MM-DD HH:MM:SS" class="session …">
#           HHhMM
#           <div class="dropdown-content"><div class="version">VF</div></div>
#         </time>
# ─────────────────────────────────────────────

def _direct_children(node: dict, tag: str) -> list[dict]:
    """Enfants directs d'un nœud filtrés par tag (non-récursif)."""
    return [c for c in node["children"] if c["tag"] == tag.lower()]


def _attrs_contain(node: dict, substring: str) -> bool:
    """Vérifie si une valeur d'attribut dans le nœud ou ses descendants contient substring."""
    for v in node["attrs"].values():
        if substring in v:
            return True
    for child in node["children"]:
        if _attrs_contain(child, substring):
            return True
    return False


def _lumiere_cinema_from_row(row: dict) -> str | None:
    """Extrait le nom du cinéma depuis une ligne <tr class='cinema'>."""
    # Les logos SVG ont des href de type "…#logo-terreaux" dans les attrs
    for name, key in (
        ("Lumière Terreaux",  "terreaux"),
        ("Lumière Bellecour", "bellecour"),
        ("Lumière Fourmi",    "fourmi"),
    ):
        if _attrs_contain(row, key):
            return name
    # Fallback texte brut
    txt = text_of(row).lower()
    for name, key in (
        ("Lumière Terreaux",  "terreaux"),
        ("Lumière Bellecour", "bellecour"),
        ("Lumière Fourmi",    "fourmi"),
    ):
        if key in txt:
            return name
    return None


def _lumiere_parse_days_row(row: dict) -> list[date | None]:
    """
    Extrait les dates depuis la ligne <tr class='days'>.
    Retourne [None, date_col1, ..., date_col7] (None = colonne titre).
    Les dates sont lues depuis l'attribut datetime des <time> dans les <th>.
    """
    col_dates: list[date | None] = [None]  # index 0 = colonne titre
    for child in row["children"]:
        if child["tag"] == "th":
            time_nodes = find_nodes(child, tag="time")
            if time_nodes:
                dt_str = time_nodes[0]["attrs"].get("datetime", "")
                try:
                    col_dates.append(date.fromisoformat(dt_str[:10]))
                    continue
                except ValueError:
                    pass
            col_dates.append(None)
    return col_dates


def _lumiere_parse_schedule_td(td: dict) -> list[dict]:
    """
    Extrait les séances depuis un <td class='schedule'>.
    Chaque <time datetime="YYYY-MM-DD HH:MM:SS" class="session"> → une séance.
    La version est dans le <div class="version"> imbriqué.
    """
    seances: list[dict] = []

    resa_url: str | None = None
    for link in find_nodes(td, tag="a"):
        href = link["attrs"].get("href", "")
        if href and ("cotecine" in href.lower() or "billet" in href.lower() or "reservation" in href.lower()):
            resa_url = href
            break

    for time_node in find_nodes(td, tag="time"):
        if "session" not in time_node["attrs"].get("class", ""):
            continue
        dt_str = time_node["attrs"].get("datetime", "")
        if not dt_str or len(dt_str) < 16:
            continue
        try:
            dt_date = date.fromisoformat(dt_str[:10])
        except ValueError:
            continue
        heure = dt_str[11:16]  # "HH:MM" from "YYYY-MM-DD HH:MM:SS"

        version_nodes = find_nodes(time_node, tag="div", cls="version")
        version = detect_version(text_of(version_nodes[0]).strip()) if version_nodes else "VF"

        seance: dict = {"date": dt_date.isoformat(), "heure": heure, "version": version}
        if resa_url:
            seance["resa_url"] = resa_url
        seances.append(seance)

    return seances


def _lumiere_extract_movie_row(row: dict, cinema: str) -> dict | None:
    """Extrait un film depuis une <tr class='movie'>."""
    # Titre depuis le <th> direct (non-récursif pour garder la bonne structure)
    th_children = _direct_children(row, "th")
    if not th_children:
        return None

    titre: str | None = None
    slug: str | None = None
    title_links = find_nodes(th_children[0], tag="a")
    if title_links:
        titre = text_of(title_links[0]).strip()
        href = title_links[0]["attrs"].get("href", "")
        m = re.search(r"/film/([^/?#]+?)(?:\.html)?(?:[?#]|$)", href)
        if m:
            slug = m.group(1)
    if not titre:
        titre = text_of(th_children[0]).strip()
    if not titre or len(titre.replace(" ", "")) < 2:
        return None

    # Séances depuis les <td> directs
    seances: list[dict] = []
    for td in _direct_children(row, "td"):
        td_cls = td["attrs"].get("class", "")
        if "schedule" in td_cls:
            seances.extend(_lumiere_parse_schedule_td(td))

    if not seances:
        return None

    return {
        "titre": titre,
        "slug": slug,
        "titreOriginal": None,
        "annee": None,
        "realisateur": None,
        "duree": None,
        "genres": [],
        "synopsis": None,
        "imdbId": None,
        "source": "lumiere",
        "cinema": cinema,
        "seances": sorted(seances, key=lambda x: (x["date"], x["heure"])),
    }


def _lumiere_fetch_film_detail(slug: str) -> dict:
    """
    Fetche la page de détail d'un film Lumière (/film/<slug>.html).
    Extrait : poster, realisateur, annee, duree, cast, synopsis.
    Retourne un dict partiel (seulement les champs trouvés).
    """
    url = f"https://www.cinemas-lumiere.com/film/{slug}.html"
    try:
        html = fetch(url, timeout=10)
    except RuntimeError as e:
        log.warning(f"  Lumière détail impossible pour {slug}: {e}")
        return {}

    root = parse_html(html)
    result: dict = {}

    # Poster : <figure class="poster"><img data-src="https://...">
    poster_figs = find_nodes(root, tag="figure", cls="poster")
    if poster_figs:
        img_nodes = find_nodes(poster_figs[0], tag="img")
        if img_nodes:
            ds = img_nodes[0]["attrs"].get("data-src", "")
            if ds and ds.startswith("http"):
                result["poster"] = ds

    # Réalisateur : <p class="filmmakers">de Prénom Nom</p>
    filmmakers = find_nodes(root, tag="p", cls="filmmakers")
    if filmmakers:
        txt = re.sub(r"^de\s+", "", text_of(filmmakers[0]).strip(), flags=re.I)
        if txt:
            result["realisateur"] = txt

    # Informations : <p class="informations">Pays | [version] | [année] | durée</p>
    # Exemples : "France | 1h39"  /  "États-Unis | VOSTF | 2026 | 2h29"
    infos = find_nodes(root, tag="p", cls="informations")
    if infos:
        info_txt = text_of(infos[0]).strip()
        m_year = re.search(r"\b(19\d{2}|20\d{2})\b", info_txt)
        if m_year:
            result["annee"] = int(m_year.group(1))
        m_dur = re.search(r"(\d{1,2})h(\d{2})", info_txt)
        if m_dur:
            result["duree"] = int(m_dur.group(1)) * 60 + int(m_dur.group(2))

    # Acteurs : <p class="actors">Avec A, B, C</p>
    actors_nodes = find_nodes(root, tag="p", cls="actors")
    if actors_nodes:
        txt = re.sub(r"^Avec\s+", "", text_of(actors_nodes[0]).strip(), flags=re.I)
        if txt:
            result["cast"] = txt

    # Synopsis : <div class="section synopsis"><p>...</p></div>
    synopsis_sections = find_nodes(root, tag="div", cls="synopsis")
    if synopsis_sections:
        p_nodes = find_nodes(synopsis_sections[0], tag="p")
        synop = text_of(p_nodes[0] if p_nodes else synopsis_sections[0]).strip()
        if synop:
            result["synopsis"] = synop[:500] + ("…" if len(synop) > 500 else "")

    return result


def scrape_lumiere(week_date: date | None = None) -> list[dict]:
    """Scrape le calendrier général des Cinémas Lumière pour la semaine donnée."""
    if week_date is None:
        week_date = get_last_wednesday()

    url = f"{URL_LUMIERE_BASE}?week={week_date.isoformat()}"
    log.info(f"Fetch Lumière → {url}")

    try:
        html = fetch(url)
    except RuntimeError as e:
        log.error(f"Impossible de télécharger Lumière : {e}")
        return []

    log.info(f"Lumière HTML reçu : {len(html):,} caractères")
    root = parse_html(html)

    tables = find_nodes(root, tag="table")
    if not tables:
        log.warning("Lumière: aucun tableau trouvé dans la page")
        return []

    # Le tableau principal est le plus grand
    table = max(tables, key=lambda t: len(find_nodes(t, tag="tr")))
    rows = find_nodes(table, tag="tr")
    if not rows:
        log.warning("Lumière: tableau vide")
        return []

    films: list[dict] = []
    current_cinema = "Lumière Terreaux"
    col_dates: list[date | None] = []

    for row in rows:
        row_class = row["attrs"].get("class", "")

        if "days" in row_class:
            col_dates = _lumiere_parse_days_row(row)
            log.debug(f"Lumière col_dates: {col_dates}")

        elif "cinema" in row_class:
            cinema = _lumiere_cinema_from_row(row)
            if cinema:
                current_cinema = cinema
                log.debug(f"Section cinéma : {current_cinema}")

        elif "movie" in row_class:
            film = _lumiere_extract_movie_row(row, current_cinema)
            if film:
                films.append(film)

    log.info(f"Lumière: {len(films)} films extraits ({week_date})")

    # Enrichir avec les pages de détail (poster, réalisateur, durée, cast, synopsis)
    # Une seule requête par slug unique, appliquée à tous les films du même slug
    slug_map: dict[str, list[dict]] = {}
    for film in films:
        slug = film.get("slug")
        if slug:
            slug_map.setdefault(slug, []).append(film)

    log.info(f"Lumière: enrichissement détails pour {len(slug_map)} films uniques…")
    enrich_fields = ["poster", "realisateur", "annee", "duree", "cast", "synopsis"]
    for slug, slug_films in slug_map.items():
        detail = _lumiere_fetch_film_detail(slug)
        if detail:
            for film in slug_films:
                for field in enrich_fields:
                    if detail.get(field) and not film.get(field):
                        film[field] = detail[field]

    return films


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
# ENRICHISSEMENT TMDB (source principale) + OMDb (fallback)
# ─────────────────────────────────────────────


def _normalize_title_key(titre: str) -> str:
    """Clé normalisée pour regrouper les variantes (ex. Le son / Le Son des souvenirs)."""
    return re.sub(r"\s+", " ", (titre or "").strip().lower())


def enrich_omdb(films: list[dict]) -> list[dict]:
    """
    TMDB en première source, OMDb en fallback.
    Complète : imdbId, poster, note, genres, synopsis, cast.
    """
    tmdb_ok = bool(TMDB_API_KEY)
    omdb_ok = OMDB_API_KEY and OMDB_API_KEY != "VOTRE_CLE_OMDB"
    if not tmdb_ok and not omdb_ok:
        log.warning("TMDB_API_KEY et OMDB_API_KEY non configurées — enrichissement ignoré")
        return films

    for film in films:
        titre = film.get("titreOriginal") or film.get("titre", "")
        if not titre:
            continue

        # 1. TMDB en premier (search par titre ou find par imdb_id)
        if tmdb_ok:
            _enrich_tmdb_first(film, titre)

        # 2. OMDb en fallback pour les champs manquants
        if omdb_ok:
            _enrich_omdb_fallback(film, titre)

    return films


def _tmdb_search(query: str, annee: int | None) -> dict | None:
    """Cherche un film sur TMDB par titre. Retry sans année si pas de résultat."""
    params = f"api_key={TMDB_API_KEY}&language=fr-FR&query={_urlencode(query)}"
    if annee:
        params += f"&year={annee}"
    url = f"{URL_TMDB_BASE}search/movie?{params}"
    try:
        data = json.loads(fetch(url, timeout=8))
        results = data.get("results") or []
        if results:
            return results[0]
        # Retry sans filtre d'année
        if annee:
            url_no_year = f"{URL_TMDB_BASE}search/movie?api_key={TMDB_API_KEY}&language=fr-FR&query={_urlencode(query)}"
            data2 = json.loads(fetch(url_no_year, timeout=8))
            results2 = data2.get("results") or []
            if results2:
                return results2[0]
    except Exception as e:
        log.warning(f"  ✗ TMDB search erreur pour «{query}»: {e}")
    return None


def _enrich_tmdb_first(film: dict, titre: str) -> None:
    """TMDB : find par imdb_id si dispo, sinon search par titre (avec fallback titre FR)."""
    annee = film.get("annee")
    tmdb_id = None

    # A. Find par imdb_id (le plus fiable)
    if film.get("imdbId"):
        url = f"{URL_TMDB_BASE}find/{film['imdbId']}?api_key={TMDB_API_KEY}&language=fr-FR&external_source=imdb_id"
        try:
            data = json.loads(fetch(url, timeout=8))
            results = data.get("movie_results") or []
            if results:
                m = results[0]
                tmdb_id = m.get("id")
                _apply_tmdb_movie(film, m)
        except Exception as e:
            log.warning(f"  ✗ TMDB find erreur pour {titre}: {e}")

    # B. Search par titre si pas encore trouvé
    if not tmdb_id:
        titre_fr = film.get("titre", "")
        # Essai 1 : titre transmis (peut être titreOriginal)
        m = _tmdb_search(titre, annee)
        # Essai 2 : titre français si différent
        if not m and titre_fr and titre_fr != titre:
            m = _tmdb_search(titre_fr, annee)
        if m:
            tmdb_id = m.get("id")
            _apply_tmdb_movie(film, m)
            # Récupérer imdbId depuis les détails TMDB
            if not film.get("imdbId") and tmdb_id:
                try:
                    det = json.loads(fetch(f"{URL_TMDB_BASE}movie/{tmdb_id}?api_key={TMDB_API_KEY}", timeout=8))
                    if det.get("imdb_id"):
                        film["imdbId"] = det["imdb_id"]
                except Exception:
                    pass
            log.info(f"  ✓ TMDB : {film['titre']} → tmdb:{tmdb_id} imdb:{film.get('imdbId', '—')}")
        else:
            log.warning(f"  ✗ TMDB introuvable : {titre} ({annee})")

    # C. Cast (credits)
    if tmdb_id and not film.get("cast"):
        try:
            data = json.loads(fetch(f"{URL_TMDB_BASE}movie/{tmdb_id}/credits?api_key={TMDB_API_KEY}", timeout=8))
            names = [c["name"] for c in (data.get("cast") or [])[:3] if c.get("name")]
            if names:
                film["cast"] = ", ".join(names)
        except Exception:
            pass

    # D. Synopsis FR depuis les détails si overview vide dans search
    if tmdb_id and not film.get("synopsis"):
        try:
            det = json.loads(fetch(f"{URL_TMDB_BASE}movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=fr-FR", timeout=8))
            ov = (det.get("overview") or "").strip()
            if ov:
                film["synopsis"] = ov[:500] + ("…" if len(ov) > 500 else "")
        except Exception:
            pass


def _apply_tmdb_movie(film: dict, m: dict) -> None:
    """Applique les champs d'un objet movie TMDB sur le film (sans écraser l'existant)."""
    if not film.get("poster") and m.get("poster_path"):
        film["poster"] = f"https://image.tmdb.org/t/p/w500{m['poster_path']}"
    if not film.get("synopsis"):
        ov = (m.get("overview") or "").strip()
        if ov:
            film["synopsis"] = ov[:500] + ("…" if len(ov) > 500 else "")
    if not film.get("imdbRating") and m.get("vote_average") is not None:
        film["imdbRating"] = float(m["vote_average"])


def _enrich_omdb_fallback(film: dict, titre: str) -> None:
    """OMDb : fallback pour champs manquants (par imdb_id ou par titre)."""
    annee = film.get("annee")
    url = None
    if film.get("imdbId"):
        url = f"{URL_OMDB_BASE}?i={film['imdbId']}&apikey={OMDB_API_KEY}&plot=full"
    else:
        params = f"t={_urlencode(titre)}&apikey={OMDB_API_KEY}&type=movie"
        if annee:
            params += f"&y={annee}"
        url = f"{URL_OMDB_BASE}?{params}"

    if not url:
        return
    try:
        data = json.loads(fetch(url, timeout=8))
        if data.get("Response") != "True":
            return
        if not film.get("imdbId") and data.get("imdbID"):
            film["imdbId"] = data["imdbID"]
        if (not film.get("poster") or film.get("poster") == "N/A") and data.get("Poster") not in (None, "N/A"):
            film["poster"] = data["Poster"]
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
        if not film.get("imdbRating") and data.get("imdbRating") not in (None, "N/A"):
            film["imdbRating"] = float(data["imdbRating"])
        if not film.get("cast") and data.get("Actors") not in (None, "N/A"):
            film["cast"] = data["Actors"]
    except Exception as e:
        log.warning(f"  ✗ OMDb erreur pour {titre}: {e}")


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
    parser = argparse.ArgumentParser(description="Scraper programme multi-cinémas Lyon")
    parser.add_argument("--debug",     action="store_true", help="Mode debug verbose")
    parser.add_argument("--dry-run",   action="store_true", help="Ne pas écrire le fichier JSON")
    parser.add_argument("--output",    default=str(OUTPUT_DEFAULT), help="Chemin du fichier JSON de sortie")
    parser.add_argument("--no-omdb",   action="store_true", help="Désactiver l'enrichissement OMDb")
    parser.add_argument("--file",      default=None, help="Fichier HTML local Comoedia (pour test)")
    parser.add_argument("--no-filter", action="store_true", help="Ne pas filtrer par semaine (pour test)")
    parser.add_argument("--no-lumiere", action="store_true", help="Désactiver le scraping des Cinémas Lumière")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("═" * 55)
    log.info(f"Multi-Cinémas Lyon Scraper — {datetime.now().strftime('%A %d %B %Y %H:%M')}")
    log.info("═" * 55)

    # 1. Scraping Comoedia
    comoedia_films = scrape_comoedia(file_path=args.file)
    if not comoedia_films:
        log.warning(
            "Aucun film Comoedia extrait — "
            "la structure HTML a peut-être changé. Lancez avec --debug."
        )

    # 2. Scraping Cinémas Lumière
    lumiere_films: list[dict] = []
    if not args.no_lumiere:
        lumiere_films = scrape_lumiere()

    # 3. Fusion des deux sources
    all_films = comoedia_films + lumiere_films
    if not all_films:
        log.error("Aucun film extrait (ni Comoedia ni Lumière).")
        sys.exit(2)
    log.info(
        f"{len(all_films)} films au total "
        f"(Comoedia:{len(comoedia_films)}, Lumière:{len(lumiere_films)})"
    )

    # 4. Enrichissement TMDB/OMDb avec cache inter-cinémas (un seul appel par titre)
    if not args.no_omdb:
        # Dédupliquer : n'enrichir chaque titre qu'une seule fois (clé normalisée)
        seen_keys: dict[str, dict] = {}
        for film in all_films:
            raw = (film.get("titreOriginal") or film.get("titre", "")).strip()
            key = _normalize_title_key(raw) if raw else ""
            if key and key not in seen_keys:
                seen_keys[key] = film

        unique_films = list(seen_keys.values())
        log.info(
            f"Enrichissement TMDB/OMDb pour {len(unique_films)} titres uniques "
            f"({len(all_films)} films au total)…"
        )
        enrich_omdb(unique_films)

        # Propagation bidirectionnelle : regrouper tous les films par titre,
        # collecter le meilleur champ disponible de n'importe quelle source,
        # puis l'appliquer à toutes les copies (ex: affiche Lumière → copie Comoedia).
        enrich_fields = [
            "imdbId", "poster", "imdbRating", "cast", "synopsis",
            "genres", "realisateur", "annee", "duree", "titreOriginal",
        ]
        title_groups: dict[str, list[dict]] = {}
        for film in all_films:
            raw = (film.get("titreOriginal") or film.get("titre", "")).strip()
            key = _normalize_title_key(raw) if raw else ""
            if key:
                title_groups.setdefault(key, []).append(film)

        for group in title_groups.values():
            # Collecter la meilleure valeur pour chaque champ dans tout le groupe
            best: dict = {}
            for film in group:
                for field in enrich_fields:
                    if film.get(field) and not best.get(field):
                        best[field] = film[field]
            # L'appliquer à tous les membres du groupe
            for film in group:
                for field in enrich_fields:
                    if best.get(field) and not film.get(field):
                        film[field] = best[field]

    # 5. Filtrage semaine
    if not args.no_filter:
        all_films = filter_current_week(all_films)
    log.info(f"{len(all_films)} films retenus pour la semaine")

    # 6. Écriture JSON
    output = {
        "generated_at": datetime.now().isoformat(),
        "sources": [URL_PROGRAMME, URL_LUMIERE_BASE],
        "films": all_films,
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
