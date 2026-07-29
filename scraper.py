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
import time
import hashlib
from datetime import datetime, date, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse
from html.parser import HTMLParser
from html import unescape as _unescape

# Charger .env pour SUPABASE_* et clés API
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
# cinema-comoedia.com (orthographe officielle) — fallback cinema-comedia.fr
URL_PROGRAMME    = "https://www.cinema-comoedia.com/programme-accessible/"
URL_LUMIERE_BASE = "https://www.cinemas-lumiere.com/calendrier-general.html"
URL_ZOLA_AFFICHE = "https://www.lezola.com/films-a-laffiche/"
URL_ZOLA_BASE    = "https://www.lezola.com"
URL_OMDB_BASE    = "https://www.omdbapi.com/"
URL_TMDB_BASE    = "https://api.themoviedb.org/3/"
import os
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "822f09ad")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

OUTPUT_DEFAULT   = Path(__file__).parent / "programme.json"
PDF_STATE_PATH   = Path(__file__).parent / "pdf_state.json"

# Page listant les PDFs hebdomadaires du Comoedia
# Depuis juin 2026, le PDF est hébergé sur un CDN (cms-assets.webediamovies.pro)
# avec un nom de fichier opaque (hash), lié depuis plusieurs pages.
# IMPORTANT : la GRILLE HEBDO (tableau des séances par jour, celle que le
# parser attend) est liée depuis /horaires-semaine-complete/ sous le libellé
# « télécharger le programme de la semaine ». La page d'accueil, elle, lie un
# PDF « programme » DIFFÉRENT (hash distinct) qui ne contient pas la grille —
# d'où 0 séance si on ne scanne QUE l'accueil. On scanne donc les deux.
URL_COMOEDIA_HORAIRES = "https://www.cinema-comoedia.com/horaires-semaine-complete/"
URL_COMOEDIA_HOME = "https://www.cinema-comoedia.com/"
URL_PDF_LISTING   = "https://www.cinema-comoedia.com/programme-semaine/"
URL_COMOEDIA_BASE = "https://www.cinema-comoedia.com"

# Mois → slug URL (sans accents, pour prédiction des noms de fichiers PDF)
MOIS_SLUG: dict[int, str] = {
    1: "janvier",   2: "fevrier",   3: "mars",      4: "avril",
    5: "mai",       6: "juin",      7: "juillet",   8: "aout",
    9: "septembre", 10: "octobre",  11: "novembre", 12: "decembre",
}
# Slug de mois → numéro (avec variantes accentuées en fallback)
MOIS_SLUG_TO_NUM: dict[str, int] = {v: k for k, v in MOIS_SLUG.items()}
MOIS_SLUG_TO_NUM.update({"février": 2, "août": 8})

# Abréviation de jour (PDF) → isoweekday (lundi=1…dimanche=7)
DAY_ABBREVS: dict[str, int] = {
    "mer": 3, "jeu": 4, "ven": 5, "sam": 6,
    "dim": 7, "lun": 1, "mar": 2,
}

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
    """Retourne le mercredi de la semaine courante du programme Lumière.

    Lumière publie le nouveau programme dès le mardi : si on est mardi,
    on retourne le mercredi *suivant* pour scraper la semaine à venir.
    """
    today = date.today()
    # isoweekday: lundi=1, mardi=2, mercredi=3, ..., dimanche=7
    days_since_wed = (today.isoweekday() - 3) % 7
    last_wed = today - timedelta(days=days_since_wed)
    # Le mardi, Lumière a déjà publié la semaine suivante
    if today.isoweekday() == 2:
        return last_wed + timedelta(days=7)
    return last_wed


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
# PDF SCRAPER COMOEDIA
# ─────────────────────────────────────────────

# ── État persistant des PDFs traités ──────────

def load_pdf_state() -> dict:
    """Charge l'état des PDFs déjà traités depuis pdf_state.json."""
    if PDF_STATE_PATH.exists():
        try:
            return json.loads(PDF_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"processed_urls": []}


def save_pdf_state(state: dict) -> None:
    """Sauvegarde l'état dans pdf_state.json."""
    PDF_STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ── Découverte des URLs de PDFs ────────────────

def _extract_pdf_links(html: str) -> list[str]:
    """Extrait tous les liens .pdf d'un HTML (href ou URL CDN brute), dédupliqués."""
    matches = re.findall(r'href=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.I)
    # Filet de sécurité : URLs CDN brutes hors attribut href
    matches += re.findall(r'https?://[^"\'\s]+\.pdf[^"\'\s]*', html, re.I)
    seen: set[str] = set()
    urls: list[str] = []
    for m in matches:
        url = m if m.startswith("http") else f"{URL_COMOEDIA_BASE}{m}"
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def fetch_pdf_urls() -> list[str]:
    """
    Découvre les URLs de PDFs du programme Comoedia.

    Depuis juin 2026 le PDF est hébergé sur un CDN (cms-assets.webediamovies.pro)
    avec un nom de fichier opaque (hash). DEUX PDF coexistent, liés depuis deux
    pages différentes, et selon les semaines l'un OU l'autre porte la grille de
    la semaine courante (l'autre peut être en retard d'une semaine) :
      - /horaires-semaine-complete/  → « programme de la semaine »
      - page d'accueil               → « Télécharger le programme »
    On ne peut donc pas se fier à une seule page. On scanne les DEUX et on AGRÈGE
    tous les PDF trouvés (au lieu de s'arrêter à la première page). Chaque PDF est
    ensuite dédupliqué par semaine via Supabase (check_week_in_supabase) : la
    semaine déjà en base est ignorée, la semaine nouvelle est insérée. Un PDF sans
    grille (entête de jours) donne 0 film et n'est pas retenu.

    Ordre de scan :
      1. /horaires-semaine-complete/,
      2. la page d'accueil,
      3. l'ancienne page de listing /programme-semaine/ (404 désormais),
      4. à défaut, la prédiction d'URL (ancien schéma self-hosted).
    """
    all_urls: list[str] = []
    seen: set[str] = set()
    for label, url in (("page horaires semaine", URL_COMOEDIA_HORAIRES),
                       ("page d'accueil", URL_COMOEDIA_HOME),
                       ("page de listing", URL_PDF_LISTING)):
        try:
            urls = _extract_pdf_links(fetch(url))
            new = [u for u in urls if u not in seen]
            if new:
                log.info(f"PDFs trouvés sur {label} : {len(new)}")
                for u in new:
                    seen.add(u)
                    all_urls.append(u)
            else:
                log.warning(f"{label} chargée mais aucun nouveau lien PDF détecté")
        except Exception as e:
            log.warning(f"{label} inaccessible ({e})")

    if all_urls:
        log.info(f"Total PDF candidats découverts : {len(all_urls)}")
        return all_urls

    log.warning("Aucun PDF découvert — fallback prédiction d'URL")
    return predict_pdf_urls()


def predict_pdf_url(week_start: date) -> str:
    """Construit l'URL prédite du PDF pour une semaine (week_start = mercredi)."""
    week_end = week_start + timedelta(days=6)
    d1, d2 = week_start.day, week_end.day
    m1, m2 = MOIS_SLUG[week_start.month], MOIS_SLUG[week_end.month]
    y = week_end.year
    if week_start.month == week_end.month:
        slug = f"du-{d1}-au-{d2}-{m1}-{y}"
    else:
        slug = f"du-{d1}-{m1}-au-{d2}-{m2}-{y}"
    return f"{URL_COMOEDIA_BASE}/pdf/cinema-lyon-comoedia-semaine-{slug}.pdf"


def predict_pdf_urls() -> list[str]:
    """Prédit les URLs des PDFs pour la semaine courante et les 2 précédentes."""
    today = date.today()
    # Si mardi, cibler la semaine prochaine (nouveau programme)
    if today.isoweekday() == 2:
        today += timedelta(days=1)
    days_since_wed = (today.isoweekday() - 3) % 7
    current_wed = today - timedelta(days=days_since_wed)
    return [predict_pdf_url(current_wed - timedelta(weeks=i)) for i in range(3)]


# ── Analyse du slug de l'URL ───────────────────

def parse_week_from_slug(url: str) -> tuple[date, date] | None:
    """
    Extrait les dates de début/fin de semaine depuis le slug du nom de fichier PDF.
    Supporte semaines intra-mois (du-8-au-14-octobre-2025) et
    inter-mois (du-26-novembre-au-2-decembre-2025), y compris inter-année.
    """
    m = re.search(r"semaine-(du-.+?)(?:\.pdf|\?|$)", url, re.I)
    if not m:
        return None
    slug = m.group(1).lower()
    # Normalise les accents pour la correspondance des noms de mois
    for src, dst in [("é", "e"), ("è", "e"), ("ê", "e"), ("û", "u"),
                     ("î", "i"), ("â", "a"), ("ô", "o")]:
        slug = slug.replace(src, dst)

    # Même mois : du-{d1}-au-{d2}-{mois}-{yyyy}
    pat_same = re.match(r"du-(\d+)-au-(\d+)-([a-z]+)-(\d{4})$", slug)
    if pat_same:
        d1, d2 = int(pat_same.group(1)), int(pat_same.group(2))
        mois = MOIS_SLUG_TO_NUM.get(pat_same.group(3))
        year = int(pat_same.group(4))
        if mois:
            try:
                return date(year, mois, d1), date(year, mois, d2)
            except ValueError:
                pass

    # Mois différents : du-{d1}-{mois1}-au-{d2}-{mois2}-{yyyy}
    pat_cross = re.match(r"du-(\d+)-([a-z]+)-au-(\d+)-([a-z]+)-(\d{4})$", slug)
    if pat_cross:
        d1 = int(pat_cross.group(1))
        m1_name = pat_cross.group(2)
        d2 = int(pat_cross.group(3))
        m2_name = pat_cross.group(4)
        year = int(pat_cross.group(5))
        m1_n = MOIS_SLUG_TO_NUM.get(m1_name)
        m2_n = MOIS_SLUG_TO_NUM.get(m2_name)
        if m1_n and m2_n:
            try:
                year1 = year - 1 if m1_n > m2_n else year
                return date(year1, m1_n, d1), date(year, m2_n, d2)
            except ValueError:
                pass
    return None


# ── Vérification Supabase ──────────────────────

def count_week_seances(
    week_start: date,
    week_end: date,
    *,
    slug: str | None = None,
    exclude_slugs: "list[str] | None" = None,
) -> int | None:
    """
    Compte les séances d'une semaine dans Supabase.

    - slug="comoedia"                       → uniquement ce cinéma
    - exclude_slugs=["comoedia", "le-zola"]  → tous SAUF ces cinémas
    - ni l'un ni l'autre                     → toutes salles confondues

    Retourne None si les credentials sont absents ou en cas d'erreur (état
    « inconnu » distinct de 0), pour que l'appelant ne conclue pas à tort.
    """
    sb_url = os.getenv("SUPABASE_URL")
    sb_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not sb_url or not sb_key:
        return None
    try:
        from supabase import create_client
        client = create_client(sb_url, sb_key)
        q = (
            client.table("seances")
            .select("id", count="exact")
            .gte("date", week_start.isoformat())
            .lte("date", week_end.isoformat())
        )
        if slug:
            r = client.table("cinemas").select("id").eq("slug", slug).execute()
            if not r.data:
                return 0  # cinéma introuvable : filtre sur ce cinéma → 0
            q = q.eq("cinema_id", r.data[0]["id"])
        elif exclude_slugs:
            r = client.table("cinemas").select("id").in_("slug", exclude_slugs).execute()
            # Un slug exclu absent de la table n'a par définition aucune séance :
            # on exclut simplement ceux qui existent.
            for row in r.data or []:
                q = q.neq("cinema_id", row["id"])
        return q.execute().count or 0
    except Exception as e:
        log.warning(f"Vérification Supabase échouée : {e} — on continue quand même")
        return None


def check_week_in_supabase(week_start: date, week_end: date) -> bool:
    """
    Vérifie si la semaine contient déjà des séances Comoedia dans Supabase.
    Retourne False si les credentials sont absents ou en cas d'erreur.
    """
    count = count_week_seances(week_start, week_end, slug="comoedia") or 0
    if count > 0:
        log.info(
            f"Semaine {week_start} déjà dans Supabase "
            f"({count} séances Comoedia) — ignoré"
        )
        return True
    return False


# ── Téléchargement du PDF ──────────────────────

def download_pdf(url: str) -> bytes | None:
    """Télécharge le PDF et retourne ses octets. Utilise requests pour le binaire."""
    try:
        import requests as req_lib
        resp = req_lib.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        log.info(f"PDF téléchargé ({len(resp.content):,} octets) : {url}")
        return resp.content
    except Exception as e:
        log.error(f"Impossible de télécharger le PDF {url} : {e}")
        return None


# ── Parsing du PDF ─────────────────────────────

def parse_comoedia_pdf(
    pdf_source: "bytes | str | Path",
) -> "tuple[list[list[str | None]], date | None]":
    """
    Ouvre le PDF (bytes, chemin string ou Path) et extrait :
      - le tableau de la 2e page (index 1)
      - la date de début de semaine lue en page 1 si possible
    Retourne (rows, week_start).
    """
    try:
        import pdfplumber
        import io as _io
    except ImportError:
        log.error("pdfplumber non installé — lancez : pip install pdfplumber")
        return [], None

    if isinstance(pdf_source, (str, Path)):
        ctx = pdfplumber.open(str(pdf_source))
    else:
        ctx = pdfplumber.open(_io.BytesIO(pdf_source))

    week_start: "date | None" = None
    table: "list[list[str | None]]" = []

    with ctx as pdf:
        if not pdf.pages:
            log.error("PDF vide — aucune page")
            return [], None

        # L'ordre des pages a changé en juin 2026 (CDN) : on ne se fie plus à
        # un index fixe. On cherche la date sur n'importe quelle page et le
        # tableau sur la page dont l'entête contient des noms de jours.
        # Tolère "du 17 au 23 Juin 2026" et "du Mercredi 17 au Mardi 23 Juin 2026".
        for page in pdf.pages:
            txt = page.extract_text() or ""
            m = re.search(
                r"[Dd]u\s+(?:\w+\s+)?(\d+)\s+au\s+(?:\w+\s+)?\d+\s+(\w+)\s+(\d{4})",
                txt,
            )
            if m:
                mois_n = MOIS_FR.get(m.group(2).lower())
                if mois_n:
                    try:
                        week_start = date(int(m.group(3)), mois_n, int(m.group(1)))
                        log.info(f"Début de semaine lu dans le PDF : {week_start}")
                        break
                    except ValueError:
                        pass

        # Choisir la page dont le tableau contient une ligne d'entête de jours
        for i, page in enumerate(pdf.pages):
            raw_table = page.extract_table()
            if raw_table and _is_day_header_row(raw_table[0]):
                table = raw_table
                log.info(f"Tableau extrait (page {i + 1}) : {len(table)} lignes")
                break
        else:
            # Repli : texte brut de la page la plus dense
            best = max(pdf.pages, key=lambda p: len(p.extract_text() or ""))
            log.warning(
                "Aucun tableau avec entête de jours — repli via extract_text()"
            )
            table = _pdf_text_to_rows(best.extract_text() or "")

    return table, week_start


def _is_day_header_row(row: "list[str | None]") -> bool:
    """Vrai si la ligne contient ≥4 noms de jours (entête du planning)."""
    joined = " ".join(c or "" for c in row).lower()
    return sum(1 for day in FR_DAY_NAMES if day in joined) >= 4


def _pdf_text_to_rows(text: str) -> "list[list[str | None]]":
    """
    Fallback : convertit le texte brut de la page PDF en pseudo-lignes.
    Structure attendue : chaque film sur une ou plusieurs lignes consécutives
    avec son titre, sa version, et ses horaires par jour.
    """
    rows: "list[list[str | None]]" = []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    i = 0
    while i < len(lines):
        line = lines[i]
        abbrevs_in_line = sum(1 for a in DAY_ABBREVS if a in line.lower())
        if abbrevs_in_line >= 4:
            # Ligne d'entête de jours — garder telle quelle
            rows.append([line])
            i += 1
            continue

        has_times = bool(re.search(r"\b\d{1,2}[h:]\d{2}\b", line))
        version_only = bool(re.match(r"^(VF|VO|VOSTFR?|VFST)$", line, re.I))

        if not has_times and not version_only and len(line) > 3:
            # Probable titre de film : agréger les lignes suivantes
            row: "list[str | None]" = [line]
            j = i + 1
            while j < len(lines) and j < i + 12:
                nl = lines[j]
                if (not re.search(r"\b\d{1,2}[h:]\d{2}\b", nl)
                        and not re.match(r"^(VF|VO|VOSTFR?|VFST)$", nl, re.I)
                        and len(nl) > 3):
                    break  # Nouveau titre
                row.append(nl)
                j += 1
            rows.append(row)
            i = j
            continue

        i += 1
    return rows


# ── Nettoyage du tableau ───────────────────────

# Noms de jours complets (PDF Comoedia) → isoweekday
FR_DAY_NAMES: dict[str, int] = {
    "mercredi": 3, "jeudi": 4, "vendredi": 5, "samedi": 6,
    "dimanche": 7, "lundi": 1, "mardi": 2,
}


def _infer_col_dates(
    header_row: "list[str | None]",
    week_start: "date | None",
) -> "dict[int, date]":
    """
    Construit col_index → date à partir de la ligne d'entête du PDF.

    Format réel observé : 'MERCREDI. 11', 'JEUDI. 12', … (jour + numéro du jour)
    Aussi supporté : abréviations courtes 'mer', 'jeu', … (fallback).

    Si week_start est fourni il est utilisé directement.
    Sinon les numéros de jours dans l'entête permettent d'inférer les dates
    en cherchant la semaine la plus proche de aujourd'hui.
    """
    col_dates: "dict[int, date]" = {}
    day_col_info: "dict[int, tuple[int, int | None]]" = {}  # col → (iso_day, day_num|None)

    for j, cell in enumerate(header_row):
        cell_l = (cell or "").strip().lower()
        if not cell_l:
            continue
        # Cherche un nom de jour complet ou une abréviation
        for day_name, iso_day in FR_DAY_NAMES.items():
            if day_name in cell_l:
                # Cherche le numéro du jour dans la cellule (ex : "mercredi. 11" → 11)
                m = re.search(r"\b(\d{1,2})\b", cell_l)
                day_num = int(m.group(1)) if m else None
                day_col_info[j] = (iso_day, day_num)
                break
        else:
            # Essai abréviations courtes
            for abbr, iso_day in DAY_ABBREVS.items():
                if re.search(rf"\b{abbr}\b", cell_l):
                    day_col_info[j] = (iso_day, None)
                    break

    if not day_col_info:
        return {}

    # Résoudre les dates
    if week_start:
        for col_j, (iso_day, _) in day_col_info.items():
            offset = (iso_day - 3) % 7
            col_dates[col_j] = week_start + timedelta(days=offset)
        return col_dates

    # Inférer depuis les numéros de jours : chercher la semaine contenant
    # un mercredi (ou jeudi si absent) dont le numéro correspond
    today = date.today()
    anchor_col = next(
        (j for j, (iso, _) in day_col_info.items() if iso == 3),
        next(iter(day_col_info)),
    )
    anchor_iso, anchor_day_num = day_col_info[anchor_col]

    if anchor_day_num is not None:
        # Search from closest to today outward to avoid false matches
        for delta in sorted(range(-28, 29), key=abs):
            candidate = today + timedelta(days=delta)
            if candidate.day == anchor_day_num and candidate.isoweekday() == anchor_iso:
                for col_j, (iso_day, _) in day_col_info.items():
                    offset = (iso_day - anchor_iso) % 7
                    col_dates[col_j] = candidate + timedelta(days=offset)
                log.info(f"Dates inférées depuis numéros de jours — ancre : {candidate}")
                return col_dates

    # Dernier recours : utiliser la semaine courante
    days_since_wed = (today.isoweekday() - 3) % 7
    wed = today - timedelta(days=days_since_wed)
    for col_j, (iso_day, _) in day_col_info.items():
        offset = (iso_day - 3) % 7
        col_dates[col_j] = wed + timedelta(days=offset)
    log.warning("Dates inférées depuis la semaine courante (fallback)")
    return col_dates


# Mots courts gardés en minuscules dans un titre (sauf en 1re position)
_TITLE_MINOR_WORDS = {
    "le", "la", "les", "un", "une", "des", "de", "du", "d", "à", "au", "aux",
    "et", "ou", "où", "en", "dans", "sur", "sous", "par", "pour", "avec", "sans",
    "the", "of", "and", "a", "an", "to", "in", "on", "at", "for", "or",
}

# Numéros romains courants (suites de films) : à préserver en capitales dans un
# titre ALL CAPS, sinon _titlecase_fr casse « II » en « Ii ». Volontairement
# limité aux formes ≥ 2 lettres pour ne pas toucher un « I »/« V »/« X » isolé
# qui serait une vraie lettre du titre (ex. « Malcolm X »).
_ROMAN_NUMERALS = {
    "II", "III", "IV", "VI", "VII", "VIII", "IX", "XI", "XII", "XIII", "XIV", "XV",
}


def _titlecase_fr(title: str) -> str:
    """Normalise un titre ALL CAPS vers une casse « titre » lisible (FR).

    N'agit QUE si le titre est essentiellement en capitales, afin de ne pas
    abîmer un titre déjà correctement casé. Gère apostrophes (L'AMOUR → L'Amour)
    et traits d'union (JEAN-PIERRE → Jean-Pierre), et garde les mots courts en
    minuscules sauf en tête.
    """
    letters = [c for c in title if c.isalpha()]
    if not letters:
        return title
    if sum(1 for c in letters if c.isupper()) / len(letters) < 0.8:
        return title  # déjà casé correctement — on ne touche pas

    def cap_token(tok: str, force: bool) -> str:
        if not tok:
            return tok
        if tok.upper() in _ROMAN_NUMERALS:   # II, III… → garder en capitales
            return tok.upper()
        low = tok.lower()
        if not force and low in _TITLE_MINOR_WORDS:
            return low
        return low[:1].upper() + low[1:]

    def cap_word(word: str, is_first: bool) -> str:
        parts = re.split(r"(['’])", word)  # isole les apostrophes
        out: list[str] = []
        first_done = False
        for p in parts:
            if p in ("'", "’"):
                out.append(p)
                continue
            segs = p.split("-")  # capitalise chaque segment d'un mot composé
            new_segs = []
            for seg in segs:
                new_segs.append(cap_token(seg, is_first and not first_done))
                if seg.strip():
                    first_done = True
            out.append("-".join(new_segs))
        return "".join(out)

    return " ".join(cap_word(w, i == 0) for i, w in enumerate(title.split()))


def clean_pdf_table(
    rows: "list[list[str | None]]",
    week_start: "date | None",
) -> "list[dict]":
    """
    Transforme les lignes brutes du tableau PDF en liste de films avec séances.

    Format réel du PDF Comoedia (observé mars 2026) :
      - Ligne 0 = entête : ['', 'MERCREDI. 11', 'JEUDI. 12', …]
      - Lignes suivantes = films :
          col 0 = 'TITRE\\nVERSION / DÉTAIL'
          col 1-7 = horaires du jour, ex '11h15 / 13h35\\n15h50' ou '-'
      - Chiffres de note de bas de page collés aux heures : '20h001' → '20h00'
    """
    if not rows:
        return []

    films: "list[dict]" = []

    # 1. Localiser la ligne d'entête (≥ 4 noms de jours)
    header_idx: "int | None" = None

    for i, row in enumerate(rows):
        cells_lower = [(c or "").strip().lower() for c in row]
        count = sum(
            1 for c in cells_lower
            if any(d in c for d in FR_DAY_NAMES)
               or any(re.search(rf"\b{a}\b", c) for a in DAY_ABBREVS)
        )
        if count >= 4:
            header_idx = i
            log.debug(f"Entête PDF trouvé à la ligne {i}")
            break

    if header_idx is None:
        log.warning("Aucun entête de jours trouvé dans le tableau PDF")
        return []

    col_dates = _infer_col_dates(rows[header_idx], week_start)
    if not col_dates:
        log.warning("Impossible de déterminer les dates des colonnes")
        return []

    # 2. Parcourir les lignes de données (à partir de la ligne après l'entête)
    for row in rows[header_idx + 1:]:
        if not row or all(not c for c in row):
            continue

        first_cell = (row[0] or "").strip()
        if not first_cell or first_cell == "-":
            continue

        # Le titre et la version sont dans la même cellule, séparés par \n
        # Ex : "ALTER EGO\nVF"  ou  "DEUX FEMMES ET QUELQUES\nHOMMES\nVFST"
        cell_lines = [ln.strip() for ln in first_cell.splitlines() if ln.strip()]

        # Identifier la ligne de version : contient VF / VO / VOST / etc.
        version_line_idx: "int | None" = None
        for li, ln in enumerate(cell_lines):
            if re.search(r"\b(VF|VO|VOST(?:FR)?|VFST)\b", ln, re.I):
                version_line_idx = li
                break

        if version_line_idx is not None:
            titre = " ".join(cell_lines[:version_line_idx]).strip()
            version_raw = cell_lines[version_line_idx]
        else:
            titre = " ".join(cell_lines).strip()
            version_raw = "VF"

        # Normaliser le titre :
        # - « JP » / « J P » = marqueur Jeune Public (jamais dans le titre), en
        #   préfixe, suffixe ou ligne isolée jointe → on le retire partout.
        # - « * » final = renvoi de note de bas de page.
        titre = re.sub(r"\bJ\s?P\b\.?", "", titre, flags=re.I)
        titre = re.sub(r"\s*\*+\s*$", "", titre)
        titre = re.sub(r"\s+", " ", titre).strip()
        titre = _titlecase_fr(titre)  # PDF en capitales → casse « titre » lisible
        if not titre or len(titre) < 2:
            continue

        # Extraire la version (premier segment avant ' / ' ou ' INT' ou ' ANS')
        version_token = re.split(r"\s*/\s*|\s+INT\b|\s+ANS\b|\s+AVERTISSEMENT\b",
                                 version_raw, maxsplit=1)[0].strip()
        version = detect_version(version_token)

        # 3. Séances : parcourir les colonnes de jours
        seances: "list[dict]" = []
        for col_j, col_date in col_dates.items():
            if col_j >= len(row):
                continue
            cell = (row[col_j] or "").strip()
            if not cell or cell == "-":
                continue
            # Retirer les chiffres de note de bas de page collés aux heures
            # Ex : '20h001' → '20h00', '11h004' → '11h00'
            cell_clean = re.sub(r"(\d{1,2}h\d{2})(\d)", r"\1", cell)
            for h, mn in re.findall(r"\b(\d{1,2})h(\d{2})\b", cell_clean):
                seances.append({
                    "date": col_date.isoformat(),
                    "heure": f"{int(h):02d}:{mn}",
                    "version": version,
                })

        if not seances:
            continue

        # Dédupliquer et trier
        seen_keys: "set[tuple]" = set()
        dedup: "list[dict]" = []
        for s in seances:
            k = (s["date"], s["heure"], s["version"])
            if k not in seen_keys:
                seen_keys.add(k)
                dedup.append(s)

        films.append({
            "titre": titre,
            "titreOriginal": None,
            "annee": None,
            "realisateur": None,
            "duree": None,
            "genres": [],
            "synopsis": None,
            "imdbId": None,
            "seances": sorted(dedup, key=lambda x: (x["date"], x["heure"])),
            "source": "comoedia",
            "cinema": "Le Comoedia",
        })

    log.info(f"{len(films)} films extraits du tableau PDF")
    return films


# ── Upsert Supabase ────────────────────────────

CINEMA_SLUGS = {
    "Le Comoedia": "comoedia",
    "Lumière Terreaux": "lumiere-terreaux",
    "Lumière Bellecour": "lumiere-bellecour",
    "Lumière Fourmi": "lumiere-fourmi",
    "Le Zola": "le-zola",
}


def _cinema_slug(name: str) -> str:
    return CINEMA_SLUGS.get(name) or name.lower().replace(" ", "-").replace("è", "e")


def _years_close(a, b, tol: int = 1) -> bool:
    """
    Garde-fou de la dédup par imdb_id : deux films ne se fusionnent sur un
    imdb_id partagé que si leurs années restent proches (≤ tol). Objectif :
    qu'un mauvais match TMDB (même imdb_id attribué par erreur à deux films
    distincts) ne fusionne pas deux vrais films différents.

    Une année manquante (None / non convertible) ne peut pas *contredire* la
    fusion → on l'autorise (l'imdb_id reste un signal fort ; le repli sur la
    clé brute gère de toute façon les cas ambigus).
    """
    try:
        return abs(int(a) - int(b)) <= tol
    except (TypeError, ValueError):
        return True


def _name_tokens(s: str) -> set[str]:
    """Tokens significatifs (≥ 3 lettres) d'un nom, insensibles à la casse."""
    return {w for w in re.split(r"\W+", (s or "").lower()) if len(w) >= 3}


def _reals_compatible(a: str, b: str) -> bool:
    """
    Garde-fou anti-homonyme du repli par titre : deux réalisateurs sont
    « compatibles » (même film sous une variante d'écriture) s'ils partagent un
    token significatif, ou si l'un est vide/absent. Ainsi « Wolfgang Becker » ⊂
    « Wolfgang Becker, Achim von Borries » (compatibles), mais « Jean Boyer » et
    « Stéphane Demoustier » ne le sont pas (La Chaleur 1938 vs 2026).
    """
    ta, tb = _name_tokens(a), _name_tokens(b)
    if not ta or not tb:
        return True
    return bool(ta & tb)


def upsert_all_to_supabase(films: list[dict]) -> None:
    """
    Upsert tous les films (Comoedia + Lumière) et leurs séances dans Supabase.
    Sans effet si SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY sont absents.
    """
    sb_url = os.getenv("SUPABASE_URL")
    sb_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not sb_url or not sb_key:
        log.info(
            "SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY absents "
            "— upsert Supabase ignoré"
        )
        return
    if not films:
        return

    try:
        from supabase import create_client
        client = create_client(sb_url, sb_key)
    except Exception as e:
        log.error(f"Connexion Supabase impossible : {e}")
        return

    cinema_ids: "dict[str, str]" = {}
    film_ids: "dict[tuple, str]" = {}          # (titre normalisé, annee, real) → film_id (repli)
    film_ids_by_imdb: "dict[str, str]" = {}    # imdb_id → film_id (clé primaire, option B)
    film_years_by_imdb: "dict[str, int]" = {}  # imdb_id → annee canonique (garde-fou)
    seances_count = 0

    # Index des films déjà en base, par titre normalisé → rattachement robuste
    # quand l'imdb_id manque (enrichissement intermittent) OU que la clé de repli
    # brute a dérivé (année 2025↔2026, réalisateur tronqué). Sans lui, un scrape
    # sans imdb crée une ligne en double et laisse la ligne canonique figée sur
    # ses vieux liens (cf. « Le Héros de Berlin » : resa_url périmés). Rechargé
    # une fois par run ; maintenu à jour au fil des upserts.
    existing_by_title: "dict[str, list[dict]]" = {}
    try:
        _all_films = client.table("films").select(
            "id,titre,annee,realisateur,imdb_id").execute().data or []
        for _f in _all_films:
            existing_by_title.setdefault(
                _normalize_title_key(_f.get("titre") or ""), []).append(_f)
    except Exception as e:
        log.warning(f"Préchargement de l'index films échoué : {e} — repli titre inactif")

    def _remember_film(fid, titre, annee, realisateur, imdb):
        """Maintient l'index titre à jour (nouveaux films / première occurrence)."""
        lst = existing_by_title.setdefault(_normalize_title_key(titre), [])
        if not any(o["id"] == fid for o in lst):
            lst.append({"id": fid, "titre": titre, "annee": annee,
                        "realisateur": realisateur, "imdb_id": imdb})

    for entry in films:
        cinema_name = entry.get("cinema") or "Le Comoedia"
        if cinema_name not in cinema_ids:
            slug = _cinema_slug(cinema_name)
            r = client.table("cinemas").upsert(
                {"name": cinema_name, "slug": slug},
                on_conflict="name",
            ).execute()
            if r.data:
                cinema_ids[cinema_name] = r.data[0]["id"]
            else:
                r2 = client.table("cinemas").select("id").eq("name", cinema_name).execute()
                if r2.data:
                    cinema_ids[cinema_name] = r2.data[0]["id"]
                else:
                    log.warning(f"Impossible de récupérer l'ID du cinéma : {cinema_name}")
                    continue

        titre = entry.get("titre") or ""
        annee = entry.get("annee")
        realisateur = entry.get("realisateur") or ""
        imdb_id = entry.get("imdbId") or None
        # Clé de repli brute, mais titre normalisé (casse/espaces) = « filet A » :
        # deux variantes de casse sans imdb_id, même année+réalisateur, ne
        # créent plus deux lignes. On garde annee+realisateur pour ne PAS
        # re-fusionner les vrais homonymes (ex. La Chaleur 1938 vs 2026).
        key = (_normalize_title_key(titre), annee, realisateur)

        film_id = None
        imdb_blocked = False  # garde-fou années : imdb_id présent mais match refusé

        # ── Dédup primaire par imdb_id (option B) ──────────────────────────
        # L'imdb_id résout le même film sous ses variantes de casse / format /
        # année → clé de dédup prioritaire. Garde-fou `_years_close` : ne
        # fusionner que si les années restent proches, pour qu'un mauvais match
        # TMDB (imdb_id partagé par erreur) ne fusionne pas deux films
        # distincts. Cf. exploration « Dédup inter-sources ».
        if imdb_id and imdb_id in film_ids_by_imdb:
            if _years_close(annee, film_years_by_imdb.get(imdb_id)):
                film_id = film_ids_by_imdb[imdb_id]
            else:
                imdb_blocked = True
        elif imdb_id:
            existing = (
                client.table("films").select("id,annee")
                .eq("imdb_id", imdb_id).limit(1).execute()
            )
            if existing.data:
                if _years_close(annee, existing.data[0].get("annee")):
                    film_id = existing.data[0]["id"]
                    film_ids_by_imdb[imdb_id] = film_id
                    film_years_by_imdb[imdb_id] = existing.data[0].get("annee")
                else:
                    imdb_blocked = True

        # Garde-fou déclenché : on ne fait pas confiance à cet imdb_id pour ce
        # film (années trop éloignées → probable mauvais match). On le stocke
        # SANS imdb_id — sinon on créerait une 2e ligne au même imdb_id, ce que
        # l'index unique partiel (migration 003) rejette. Le film reste dédup
        # par la clé de repli.
        store_imdb = None if imdb_blocked else imdb_id
        if imdb_blocked:
            log.warning(
                f"Dédup imdb_id ignorée (années éloignées) pour « {titre} » "
                f"({annee}, imdb_id={imdb_id}) — stocké sans imdb_id"
            )

        # ── Repli : clé brute normalisée (titre, annee, realisateur) ───────
        if film_id is None:
            film_id = film_ids.get(key)

        # ── Repli robuste par titre normalisé (index base) ─────────────────
        # Rattache à une ligne existante du même film même quand l'imdb_id est
        # absent et que (année, réalisateur) ont dérivé — le cas qui, sinon,
        # crée un doublon et fige la ligne canonique sur de vieux resa_url.
        # Garde-fous anti-homonyme : années proches (≤ tol) ET réalisateurs
        # compatibles (La Chaleur 1938/Boyer vs 2026/Demoustier → écartés).
        if film_id is None:
            for cand in existing_by_title.get(_normalize_title_key(titre), []):
                if not _years_close(annee, cand.get("annee")):
                    continue
                if not _reals_compatible(realisateur, cand.get("realisateur") or ""):
                    continue
                film_id = cand["id"]
                # Backfill imdb_id : si le scrape en fournit un et que la ligne
                # n'en a pas (et qu'aucune autre ligne ne le porte déjà), on le
                # renseigne pour fiabiliser le matching des prochains runs.
                if store_imdb and not cand.get("imdb_id") and not any(
                    o.get("imdb_id") == store_imdb
                    for lst in existing_by_title.values() for o in lst
                ):
                    try:
                        client.table("films").update(
                            {"imdb_id": store_imdb}).eq("id", film_id).execute()
                        cand["imdb_id"] = store_imdb
                    except Exception as e:
                        log.warning(f"Backfill imdb_id échoué (« {titre} ») : {e}")
                log.info(f"Rattaché par titre : « {titre} » ({annee}) → ligne "
                         f"existante {film_id} (imdb absent/clé dérivée)")
                break

        # ── Création si toujours introuvable ───────────────────────────────
        if film_id is None:
            row = {
                "titre": titre,
                "titre_original": entry.get("titreOriginal"),
                "annee": annee,
                "realisateur": realisateur,
                "duree": entry.get("duree"),
                "genres": entry.get("genres") or [],
                "synopsis": entry.get("synopsis"),
                "imdb_id": store_imdb,
                "poster": entry.get("poster"),
                "backdrop": entry.get("backdrop"),
                "trailer": entry.get("trailer"),
                "imdb_rating": entry.get("imdbRating"),
                "cast": entry.get("cast"),
                "source": entry.get("source"),
            }
            r = client.table("films").upsert(
                row, on_conflict="titre,annee,realisateur"
            ).execute()
            if r.data:
                film_id = r.data[0]["id"]
            else:
                r2 = (
                    client.table("films").select("id")
                    .eq("titre", titre)
                    .eq("annee", annee)
                    .eq("realisateur", realisateur)
                    .execute()
                )
                if r2.data:
                    film_id = r2.data[0]["id"]

        # Enregistrer le film_id (canonique) dans les caches + l'index titre.
        if film_id:
            film_ids[key] = film_id
            if store_imdb:
                film_ids_by_imdb.setdefault(store_imdb, film_id)
                film_years_by_imdb.setdefault(store_imdb, annee)
            _remember_film(film_id, titre, annee, realisateur, store_imdb)

            # Backfill des métadonnées TMDB sur les lignes DÉJÀ créées : les
            # colonnes ajoutées après coup (backdrop, trailer) restent NULL sur
            # l'historique tant qu'on ne remplit qu'à la création. On les met à
            # jour UNIQUEMENT quand elles sont encore NULL en base (filtre
            # .is_(col, "null")) → jamais d'écrasement d'une valeur existante,
            # et no-op une fois remplies. Idem applicable aux nouveaux champs.
            for col, val in (("backdrop", entry.get("backdrop")),
                             ("trailer", entry.get("trailer"))):
                if val:
                    try:
                        client.table("films").update({col: val}) \
                            .eq("id", film_id).is_(col, "null").execute()
                    except Exception as e:
                        log.warning(f"Backfill {col} échoué (« {titre} ») : {e}")

        cinema_id = cinema_ids.get(cinema_name)
        if not film_id or not cinema_id:
            continue

        for s in entry.get("seances", []):
            d_val = s.get("date")
            h_val = s.get("heure")
            if not d_val or not h_val:
                continue
            heure = h_val + ":00" if len(h_val) == 5 and ":" in h_val else h_val
            try:
                client.table("seances").upsert(
                    {
                        "film_id": film_id,
                        "cinema_id": cinema_id,
                        "date": d_val,
                        "heure": heure,
                        "version": s.get("version"),
                        "resa_url": s.get("resa_url"),
                    },
                    on_conflict="film_id,cinema_id,date,heure",
                ).execute()
                seances_count += 1
            except Exception as e:
                log.warning(
                    f"Séance non insérée ({titre} {d_val} {h_val}) : {e}"
                )

    log.info(
        f"Supabase : {len(set(film_ids.values()))} films, {seances_count} séances "
        f"upsertés ({len(cinema_ids)} cinémas)"
    )


# ── Orchestrateur PDF principal ────────────────

def scrape_comoedia_pdf(
    pdf_file: "str | None" = None,
    pdf_url_override: "str | None" = None,
    dry_run: bool = False,
) -> list[dict]:
    """
    Orchestrateur du scraper PDF Comoedia.
    Retourne la liste de films au même format que scrape_comoedia().
    Gère découverte, déduplication, téléchargement et parsing.
    """
    # Mode fichier local (test / debug)
    if pdf_file:
        log.info(f"Mode fichier PDF local : {pdf_file}")
        table, week_start = parse_comoedia_pdf(Path(pdf_file))
        return clean_pdf_table(table, week_start)

    state = load_pdf_state()
    processed: list[str] = state.setdefault("processed_urls", [])

    # Une URL passée explicitement en --pdf-url est un ordre manuel : on FORCE
    # son (re)traitement, sans la court-circuiter via la garde « déjà traité ».
    forced = pdf_url_override is not None
    urls_to_check = [pdf_url_override] if forced else fetch_pdf_urls()
    all_films: list[dict] = []

    for url in urls_to_check:
        # ── Garde 1 : déjà traité ? (ignorée si URL forcée) ──
        if url in processed and not forced:
            log.info(f"PDF déjà traité — ignoré : {url}")
            continue

        # ── Garde 2a : semaine depuis l'URL (rapide, sans download) ──
        week_range = parse_week_from_slug(url)
        if week_range:
            week_start, week_end = week_range
            log.info(f"Semaine PDF (depuis URL) : {week_start} → {week_end}")
            if check_week_in_supabase(week_start, week_end):
                if not dry_run:
                    processed.append(url)
                    save_pdf_state(state)
                continue
        else:
            week_start = None
            log.info(
                f"URL sans slug de semaine lisible (CDN ?) — "
                "la date sera lue depuis le contenu du PDF"
            )

        # ── Téléchargement ─────────────────────
        pdf_bytes = download_pdf(url)
        if not pdf_bytes:
            continue

        # ── Parsing ────────────────────────────
        table, pdf_week_start = parse_comoedia_pdf(pdf_bytes)
        # resolved_start may be None for CDN URLs — clean_pdf_table infers from header
        resolved_start = pdf_week_start or week_start

        # ── Garde 2b : vérification Supabase post-parse (cas CDN) ──
        if week_start is None and pdf_week_start:
            week_end_calc = pdf_week_start + timedelta(days=6)
            if check_week_in_supabase(pdf_week_start, week_end_calc):
                if not dry_run:
                    processed.append(url)
                    save_pdf_state(state)
                continue

        films = clean_pdf_table(table, resolved_start)  # None → date inference from header
        if not films:
            log.warning(f"Aucun film extrait du PDF : {url}")
            continue

        log.info(f"{len(films)} films extraits de {url}")
        all_films.extend(films)

        if not dry_run:
            processed.append(url)
            save_pdf_state(state)

    return all_films


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


def is_valid_resa_url(href: str | None) -> bool:
    """
    Allowlist des liens de réservation (condition Sécu ① du doc « Accès
    billetterie ») : la page n'a pas de CSP posable, l'allowlist EST la seule
    défense. N'accepte qu'un https vers une billetterie connue — rejette
    javascript:, data:, http:, et tout hôte tiers (`javascript:…/*cotecine*/`
    passait l'ancien test de sous-chaîne). Le front re-valide de son côté (②).
    """
    if not href or not isinstance(href, str):
        return False
    try:
        p = urlparse(href)
    except ValueError:
        return False
    if p.scheme != "https":
        return False
    host = (p.hostname or "").lower()
    return host.endswith(".cotecine.fr") or host == "www.ticketingcine.com"


def _lumiere_parse_schedule_td(td: dict) -> list[dict]:
    """
    Extrait les séances depuis un <td class='schedule'>.
    Chaque <time datetime="YYYY-MM-DD HH:MM:SS" class="session"> → une séance.
    La version est dans le <div class="version"> imbriqué, et le lien de
    réservation cotecine est le <a> imbriqué DANS ce même <time>.

    ⚠️ Chercher le <a> au périmètre du <td> renverrait le PREMIER lien du jour
    pour toutes les séances → toutes pointent sur la 1re séance (mauvaise heure,
    et « séance passée » dès que la 1re est jouée). Le lien vit dans le <time>
    (cf. doc de recherche « Accès billetterie », spike SP1 : 275/275 imbriqués).
    """
    seances: list[dict] = []

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

        # Lien propre à CETTE séance : le <a> imbriqué dans son <time>.
        resa_url: str | None = None
        for link in find_nodes(time_node, tag="a"):
            href = link["attrs"].get("href", "")
            if is_valid_resa_url(href):
                resa_url = href
                break

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


# ─────────────────────────────────────────────
# SCRAPER LE ZOLA (Villeurbanne)
# ─────────────────────────────────────────────
# Structure réelle (inspectée juillet 2026) — WordPress thème maison « zola »,
# HTML rendu serveur :
#   Index /films-a-laffiche/ : cartes <a class="well poster" href=".../movies/{slug}/">
#     avec <img class="well-image"> (affiche) et <h3 class="well__title">.
#   Fiche /movies/{slug}/ :
#     <h1 class="movie-sheet__title">Titre</h1>
#     <p class="movie-sheet__synopsis">…</p>
#     bloc infos (dans .hide-phone, DUPLIQUÉ dans .show-phone — n'en lire qu'un) :
#       « Sortie le 17 Juin 2026 », « Genre : A, B », « Durée : 1h42 », « Origine : US »
#     séances : <div class="movie-sessions-week"> > <div class="movie-sessions-day">
#       (un bloc par jour, vide si pas de séance ; chaque bloc non vide porte SA date
#        dans <p class="show-phone"><b>10</b><br> Juil</p> — mois ABRÉGÉ, sans année)
#       puis paires <div class="mb-3"><b>14h30</b>…<small>VF VI</small></div>
#                 + <p class="mb-4"><a href="ticketingcine.com/…">Réserver</a></p>
#       Séance passée : bouton « disabled » avec href vide → resa_url None.
#   Versions observées : « VF VI », « VO ST », « VF ST,OCAP,VI » (VI=audiodescription,
#   ST/OCAP=sourds-malentendants) → compacter les espaces puis detect_version().
#   Billetterie TicketingCiné : URL stable par séance (pas de token horaire,
#   contrairement au cotecine Lumière).
#
# IMPORTANT (dédup inter-cinémas) : on laisse volontairement annee/realisateur à
# None bien que la fiche les affiche. L'enrichissement et la propagation ne
# remplissent QUE les champs vides : une copie Zola vide hérite de la valeur du
# groupe (Lumière/Comoedia/OMDb) et la clé d'upsert (titre, annee, realisateur)
# converge. La « Sortie le … 2026 » est en outre l'année de sortie FRANÇAISE,
# pas l'année de production → la renseigner ferait diverger la clé à coup sûr.
# ─────────────────────────────────────────────

def _zola_parse_mois(label: str) -> int | None:
    """Mois FR éventuellement abrégé (« Juil », « Sept », « Févr ») → numéro."""
    s = label.strip().lower().rstrip(".")
    if not s:
        return None
    matches = [n for nom, n in MOIS_FR.items() if nom.startswith(s)]
    # « jui » seul est ambigu (juin/juillet) → exiger un match unique
    return matches[0] if len(matches) == 1 else None


def _zola_parse_date(jour: int, mois_label: str) -> date | None:
    """
    Date d'une séance Zola (« 10 » + « Juil ») — sans année dans le HTML.
    Année inférée : celle qui place la date au plus près d'aujourd'hui
    (gère le passage décembre → janvier).
    """
    mois = _zola_parse_mois(mois_label)
    if not mois:
        return None
    today = date.today()
    candidats = []
    for annee in (today.year - 1, today.year, today.year + 1):
        try:
            candidats.append(date(annee, mois, jour))
        except ValueError:
            continue
    if not candidats:
        return None
    return min(candidats, key=lambda d: abs((d - today).days))


def _zola_extract_seances(day_node: dict) -> list[dict]:
    """
    Extrait les séances d'un <div class="movie-sessions-day">.
    Parcourt les enfants dans l'ordre : la date du jour (p.show-phone), puis
    des paires horaire (div.mb-3) / lien résa (p.mb-4).
    """
    seances: list[dict] = []
    jour_date: date | None = None
    pending: dict | None = None

    def _walk(children):
        nonlocal jour_date, pending
        for child in children:
            cls = child["attrs"].get("class", "")
            if child["tag"] == "p" and "show-phone" in cls:
                # Jour dans <b>, mois en texte du <p> — l'ordre de restitution
                # varie (le texte après <br> remonte au parent) → extraction
                # insensible à l'ordre.
                txt = text_of(child)
                m_jour = re.search(r"\b(\d{1,2})\b", txt)
                m_mois = re.search(r"\b([A-Za-zÀ-ÿ]{3,})\b", txt)
                if m_jour and m_mois:
                    jour_date = _zola_parse_date(int(m_jour.group(1)), m_mois.group(1))
            elif child["tag"] == "div" and "mb-3" in cls:
                b_nodes = find_nodes(child, tag="b")
                heure = parse_heure(text_of(b_nodes[0])) if b_nodes else None
                small_nodes = find_nodes(child, tag="small")
                version_txt = text_of(small_nodes[0]) if small_nodes else ""
                # « VO ST » → « VOST », « VF ST,OCAP,VI » → « VFST… »
                version = detect_version(version_txt.replace(" ", ""))
                if heure:
                    pending = {"heure": heure, "version": version, "resa_url": None}
            elif child["tag"] == "p" and "mb-4" in cls:
                links = find_nodes(child, tag="a")
                if pending is not None:
                    if links:
                        href = links[0]["attrs"].get("href", "").strip()
                        a_cls = links[0]["attrs"].get("class", "")
                        if "disabled" not in a_cls and is_valid_resa_url(href):
                            pending["resa_url"] = href
                    if jour_date:
                        pending["date"] = jour_date.isoformat()
                        seances.append(pending)
                    pending = None
            else:
                _walk(child["children"])

    _walk(day_node["children"])
    return seances


def _zola_extract_film(html: str, slug: str) -> dict | None:
    """Parse une fiche film /movies/{slug}/ → film dict (ou None si vide)."""
    root = parse_html(html)

    titres = find_nodes(root, tag="h1", cls="movie-sheet__title")
    titre = text_of(titres[0]).strip() if titres else None
    if not titre:
        log.warning(f"  Zola: titre introuvable pour {slug} — fiche ignorée")
        return None

    film: dict = {
        "titre": titre,
        "slug": slug,
        "titreOriginal": None,
        "annee": None,          # volontairement vide — voir bloc de commentaire
        "realisateur": None,    # idem (convergence de la clé de dédup)
        "duree": None,
        "genres": [],
        "synopsis": None,
        "imdbId": None,
        "source": "zola",
        "cinema": "Le Zola",
        "seances": [],
    }

    synops = find_nodes(root, tag="p", cls="movie-sheet__synopsis")
    if synops:
        synop = text_of(synops[0]).strip()
        if synop:
            film["synopsis"] = synop[:500] + ("…" if len(synop) > 500 else "")

    # Bloc infos « <b>Durée</b> : 1h42<br> » : le texte et les labels <b> sont
    # entrelacés — l'arbre de SimpleHTMLParser perd cet ordre (le texte libre
    # s'agrège sur le parent) → regex sur le HTML BRUT.
    # (Le bloc est dupliqué desktop/mobile : on prend la 1re occurrence.)
    # NB : le « Genre » affiché par la fiche n'est PAS ingéré — même logique que
    # annee/realisateur : les genres viennent de l'enrichissement OMDb pour
    # toutes les sources (vocabulaire uniforme), pas du site.
    m = re.search(r"<b>\s*Durée\s*</b>\s*:\s*(\d{1,2})h(\d{2})?", html)
    if m:
        film["duree"] = int(m.group(1)) * 60 + int(m.group(2) or 0)

    for day_node in find_nodes(root, tag="div", cls="movie-sessions-day"):
        film["seances"].extend(_zola_extract_seances(day_node))

    film["seances"].sort(key=lambda s: (s["date"], s["heure"]))
    return film


def scrape_zola() -> list[dict]:
    """
    Scrape Le Zola : index /films-a-laffiche/ → une fiche par film.
    Pas de paramètre de semaine côté site (liste roulante ~15 jours) :
    c'est filter_current_week() qui borne ensuite la fenêtre affichée.
    """
    # Le site renvoie parfois (rarement) une page partielle sans les cartes
    # (variante CDN/hoquet serveur, observé en juillet 2026) → un retry simple.
    cards: dict[str, dict] = {}
    for attempt in (1, 2):
        log.info(f"Fetch Zola → {URL_ZOLA_AFFICHE}" + (" (retry)" if attempt == 2 else ""))
        try:
            html = fetch(URL_ZOLA_AFFICHE)
        except RuntimeError as e:
            log.error(f"Impossible de télécharger Le Zola : {e}")
            return []
        log.info(f"Zola HTML reçu : {len(html):,} caractères")

        root = parse_html(html)
        # Cartes films : <a class="well poster" href=".../movies/{slug}/">
        for a in find_nodes(root, tag="a", cls="poster"):
            href = a["attrs"].get("href", "")
            m = re.search(r"/movies/([^/?#]+)/?", href)
            if m:
                cards.setdefault(m.group(1), a)
        if cards:
            break
        time.sleep(3)
    if not cards:
        log.warning("Zola: aucune carte film trouvée sur l'index (après retry)")
        return []
    log.info(f"Zola: {len(cards)} films sur l'index")

    films: list[dict] = []
    for slug, card in cards.items():
        try:
            detail_html = fetch(f"{URL_ZOLA_BASE}/movies/{slug}/", timeout=10)
        except RuntimeError as e:
            log.warning(f"  Zola détail impossible pour {slug}: {e}")
            continue
        film = _zola_extract_film(detail_html, slug)
        if not film:
            continue
        # Affiche depuis la carte de l'index (img.well-image)
        imgs = find_nodes(card, tag="img")
        if imgs:
            src = imgs[0]["attrs"].get("src", "")
            if src.startswith("http"):
                film["poster"] = src
        if film["seances"]:
            films.append(film)
        else:
            log.debug(f"  Zola: {film['titre']} sans séance à venir — ignoré")

    log.info(f"Zola: {len(films)} films avec séances")
    return films


# ═════════════════════════════════════════════════════════════════════════
# ÉVÉNEMENTS — avant-premières, rencontres, séances spéciales, festivals
# ═════════════════════════════════════════════════════════════════════════
# Sources sondées le 2026-07-27 (cf. « Brief - Onglet Événements », vault) :
#   • Comoedia  /tous-les-evenements  → le TYPE est étiqueté à la source, et
#     chaque événement expose un JSON Gatsby (/page-data/…/page-data.json)
#     avec dates, affiche, description HTML et liens films `?date=YYYY-MM-DD`.
#   • Lumière   evenement.html + rendez-vous.html → pages WYSIWYG : sections
#     <h2> + lignes de <table>. Pas de champ type : préfixe en gras × section.
#     ⚠️ avant-premieres.html est PÉRIMÉE (non purgée) — jamais ingérée.
#
# Le « dict événement » (contrat C5, miroir du C1 des films) :
#   {type, forme, titre, description, date_debut, date_fin, precision,
#    affiche_url, source, source_url,
#    films:    [{titre, dates:[iso]}],
#    creneaux: [{cinema, date, heure, titre_film, invite, description, resa_url}]}
# ═════════════════════════════════════════════════════════════════════════

URL_COMOEDIA_EVENTS = "https://www.cinema-comoedia.com/tous-les-evenements"
URL_LUMIERE_SITE    = "https://www.cinemas-lumiere.com"
URL_LUMIERE_EVENTS  = f"{URL_LUMIERE_SITE}/evenement.html"
URL_LUMIERE_RDV     = f"{URL_LUMIERE_SITE}/rendez-vous.html"

# Priorité de source pour le TITRE CANONIQUE (§9.2). Surtout PAS « le premier
# récupéré » : main() scrape Comoedia d'abord, donc Comoedia gagnerait toujours
# et le titre basculerait silencieusement le jour où un scraper échoue.
EVENT_SOURCE_PRIORITY = {"lumiere": 0, "comoedia": 1, "zola": 2}

# Fenêtre de dédup inter-sources : sans elle, deux avant-premières du même film
# à trois mois d'écart fusionneraient à tort (cas réel : « Notre salut »).
EVENT_DEDUP_WINDOW_DAYS = 14

# Types de la source Comoedia → taxonomie interne.
_COMOEDIA_TYPE_MAP = {
    "avant-première": ("avant_premiere", None),
    "avant-premiere": ("avant_premiere", None),
    "rencontre":      ("rencontre", None),
    "festival":       ("festival", "festival"),
    "rétrospective":  ("festival", "retrospective"),
    "retrospective":  ("festival", "retrospective"),
    "cycle":          ("festival", "cycle"),
    "jeune public":   ("festival", "jeune_public"),
}

# Préfixes en gras des pages Lumière (§3.2) → type primaire.
_LUMIERE_PREFIX_MAP = [
    (r"l['’]avant-première du lundi", ("avant_premiere", None)),
    (r"avant-première",               ("avant_premiere", None)),
    (r"séance spéciale",              ("seance_speciale", None)),
    (r"ressortie nationale",          ("seance_speciale", None)),
    (r"rencontre",                    ("rencontre", None)),
    (r"cycle\b",                      ("festival", "cycle")),
    (r"rétrospective",                ("festival", "retrospective")),
    (r"festival",                     ("festival", "festival")),
]

# §4.1 — la discriminante est « par + personne nommée » (rencontre) contre
# « en partenariat avec / dans le cadre de + organisation » (séance spéciale).
_RENCONTRE_RE = re.compile(
    r"en\s+présence\s+d|"
    r"présenté[e]?\s+par\s+\S|"
    r"suivi[e]?\s+d['’]un\s+échange|"
    r"animée?\s+par|"
    r"rencontre\s+avec",
    re.I,
)
_SPECIALE_RE = re.compile(r"en\s+partenariat\s+avec|dans\s+le\s+cadre\s+d", re.I)

_SAISONS_RE = re.compile(r"\bcet\s+été\b|\bcet\s+hiver\b|\bce\s+printemps\b|\bcet\s+automne\b", re.I)
_EN_COURS_RE = re.compile(r"\bactuellement\b|\ben\s+cours\b", re.I)

_MOIS_ALT = "|".join(MOIS_FR.keys())
_JOUR_ALT = "|".join(JOURS_FR.keys())


def _html_to_text(fragment: str) -> str:
    """
    Texte lisible d'un fragment HTML, DANS L'ORDRE du document.

    ⚠️ Ne pas utiliser `text_of()` ici : il concatène le texte d'un nœud AVANT
    celui de ses enfants, ce qui mélange l'ordre sur les pages Lumière (le
    titre du film y remonte après le nom du réalisateur) — or toute la
    classification repose sur « le préfixe en gras précède la date ».
    """
    if not fragment:
        return ""
    txt = re.sub(r"<(script|style)\b.*?</\1>", " ", fragment, flags=re.S | re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = _unescape(txt)
    return re.sub(r"[\s ]+", " ", txt).strip()


def _deslugify(slug: str) -> str:
    """`les-vacances-de-monsieur-hulot` → `Les Vacances De Monsieur Hulot` (repli)."""
    return re.sub(r"^\d+-", "", slug or "").replace("-", " ").strip().title()


def _event_year(mois: int, jour: int, ref: date) -> int:
    """
    Année d'une date française sans millésime. On reste sur l'année de
    référence, sauf si ça place la date dans un passé lointain (> 60 j) : la
    source parle alors de l'année suivante (« janvier » lu en décembre).
    """
    for annee in (ref.year, ref.year + 1):
        try:
            d = date(annee, mois, jour)
        except ValueError:
            continue
        if (ref - d).days <= 60:
            return annee
    return ref.year


def _parse_jour_mois(txt: str, ref: date) -> "date | None":
    """« mardi 15 septembre 2026 » / « 1er octobre » → date."""
    m = re.search(
        rf"(?:(?:{_JOUR_ALT})\s+)?(\d{{1,2}})(?:er)?\s+({_MOIS_ALT})(?:\s+(\d{{4}}))?",
        txt, re.I,
    )
    if not m:
        return None
    jour = int(m.group(1))
    mois = MOIS_FR.get(m.group(2).lower())
    if not mois:
        return None
    annee = int(m.group(3)) if m.group(3) else _event_year(mois, jour, ref)
    try:
        return date(annee, mois, jour)
    except ValueError:
        return None


def parse_event_period(texte: str, ref: "date | None" = None) -> tuple:
    """
    (date_debut, date_fin, precision) depuis un texte de source, en ISO.

    Modélise l'imprécision au lieu de la masquer : `precision` ∈ exact | jour |
    mois | saison | en_cours. Les cas sans date exploitable (« Actuellement »,
    « Cet été ») sont résolus plus tard par jointure avec les séances (§4.3) —
    ici on se contente de les qualifier.
    """
    ref = ref or date.today()
    t = re.sub(r"[\s ]+", " ", texte or "")

    # « du 4 juillet au 22 août 2026 » — l'année finale vaut pour les deux bornes.
    m = re.search(
        rf"du\s+(\d{{1,2}})(?:er)?\s+({_MOIS_ALT})(?:\s+(\d{{4}}))?\s+au\s+"
        rf"(\d{{1,2}})(?:er)?\s+({_MOIS_ALT})(?:\s+(\d{{4}}))?",
        t, re.I,
    )
    if m:
        mois_f = MOIS_FR.get(m.group(5).lower())
        jour_f = int(m.group(4))
        an_f = int(m.group(6)) if m.group(6) else _event_year(mois_f, jour_f, ref)
        mois_d = MOIS_FR.get(m.group(2).lower())
        jour_d = int(m.group(1))
        an_d = int(m.group(3)) if m.group(3) else (an_f - 1 if mois_d > mois_f else an_f)
        try:
            return (date(an_d, mois_d, jour_d).isoformat(),
                    date(an_f, mois_f, jour_f).isoformat(), "exact")
        except (ValueError, TypeError):
            pass

    # « jusqu'au 31 août 2026 » → fin connue, début ouvert (l'événement a commencé).
    m = re.search(rf"jusqu['’]au\s+(.{{0,32}}?({_MOIS_ALT})(?:\s+\d{{4}})?)", t, re.I)
    if m:
        d = _parse_jour_mois(m.group(1), ref)
        if d:
            return (None, d.isoformat(), "exact")

    # « à partir du 12 août », « dès le 29 juillet » → début connu, fin OUVERTE.
    # `en_cours` et non `exact` : la source n'annonce pas de fin. Une fin déduite
    # des séances scrapées glisserait de semaine en semaine (une ressortie
    # afficherait « 29 juillet → 4 août », puis « 5 → 11 août »…) — c'est le
    # marqueur que le front lit pour n'afficher que ce qui est réellement su.
    m = re.search(rf"(?:à partir du|dès le|dès)\s+(.{{0,32}}?({_MOIS_ALT})(?:\s+\d{{4}})?)", t, re.I)
    if m:
        d = _parse_jour_mois(m.group(1), ref)
        if d:
            return (d.isoformat(), None, "en_cours")

    if _EN_COURS_RE.search(t):
        return (None, None, "en_cours")
    if _SAISONS_RE.search(t):
        return (None, None, "saison")

    # Date unitaire — « Mardi 15 septembre à 20h30 ».
    d = _parse_jour_mois(t, ref)
    if d:
        return (d.isoformat(), d.isoformat(), "exact")

    # « en septembre 2026 » — bucket mensuel, suffisant pour trier et grouper.
    m = re.search(rf"\b({_MOIS_ALT})\s+(\d{{4}})\b", t, re.I)
    if m:
        mois = MOIS_FR.get(m.group(1).lower())
        annee = int(m.group(2))
        if mois:
            fin = date(annee + (mois == 12), (mois % 12) + 1, 1) - timedelta(days=1)
            return (date(annee, mois, 1).isoformat(), fin.isoformat(), "mois")

    return (None, None, "en_cours")


def parse_event_time(texte: str) -> "str | None":
    """« à 20h30 » / « à 20h » → « 20:30 » / « 20:00 »."""
    m = re.search(r"\b(?:à|a)\s*(\d{1,2})\s*h\s*(\d{2})?\b", texte or "", re.I)
    if not m:
        return None
    return f"{int(m.group(1)):02d}:{m.group(2) or '00'}"


def classify_event_type(texte: str, type_source: "str | None" = None,
                        primaire: "str | None" = None) -> str:
    """
    Type d'un événement (§4.1).

    Un type EXPLICITE de la source (avant-première, rencontre, festival) fait
    foi : « Séance présentée » sur une avant-première Cannes ne doit pas la
    transformer en rencontre. Le raffinement ne s'applique qu'au type vague
    « séance spéciale » (ou à l'absence de type).

    `primaire` permet à l'appelant d'imposer le type primaire qu'il a déduit
    autrement (préfixe en gras × section, côté Lumière).
    """
    if primaire is None:
        primaire, _ = _event_type_forme_from_source(texte, type_source)
    if primaire in ("avant_premiere", "rencontre", "festival"):
        return primaire
    # Rencontre l'emporte sur séance spéciale quand les deux marqueurs
    # cohabitent : « présentée PAR l'auteur… EN PARTENARIAT AVEC Quais du
    # Polar » est bien une rencontre (une personne est là).
    if _RENCONTRE_RE.search(texte or ""):
        return "rencontre"
    if _SPECIALE_RE.search(texte or ""):
        return "seance_speciale"
    return primaire or "seance_speciale"


def classify_event_forme(texte: str, type_: str, forme_source: "str | None" = None) -> "str | None":
    """
    Sous-classe d'un festival (§4.2). NULL dès que le type n'est pas festival —
    c'est la contrainte portée par la base (evenements_forme_chk).
    """
    if type_ != "festival":
        return None
    if forme_source:
        return forme_source
    t = texte or ""
    if re.search(r"\bcycle\b", t, re.I):
        return "cycle"
    if re.search(r"rétrospective|retrospective", t, re.I):
        return "retrospective"
    if re.search(r"jeune public|little films|dès \d+ ans", t, re.I):
        return "jeune_public"
    return "festival"


def _event_type_forme_from_source(texte: str, type_source: "str | None") -> tuple:
    """(type, forme) issus de l'étiquette de source, sinon du préfixe en gras."""
    if type_source:
        key = type_source.strip().lower()
        if key in _COMOEDIA_TYPE_MAP:
            return _COMOEDIA_TYPE_MAP[key]
    for pattern, (t, f) in _LUMIERE_PREFIX_MAP:
        if re.search(pattern, texte or "", re.I):
            return (t, f)
    return (None, None)


def _cinema_from_text(texte: str, defaut: "str | None" = None) -> "str | None":
    """
    Salle citée dans le texte (« au Lumière Bellecour »). Tolère les coquilles
    de saisie de la source (« Belleocur » vu le 2026-07-27) en ne cherchant
    qu'un radical.
    """
    t = (texte or "").lower()
    for radical, nom in (
        ("terreaux", "Lumière Terreaux"),
        ("bellec", "Lumière Bellecour"),
        ("belleoc", "Lumière Bellecour"),
        ("fourmi", "Lumière Fourmi"),
        ("comœdia", "Le Comoedia"),
        ("comoedia", "Le Comoedia"),
        ("comédia", "Le Comoedia"),
        ("zola", "Le Zola"),
    ):
        if radical in t:
            return nom
    return defaut


def _new_event(**kw) -> dict:
    """Dict événement (contrat C5) avec tous ses champs, même vides."""
    ev = {
        "type": "seance_speciale", "forme": None, "titre": "", "description": None,
        "date_debut": None, "date_fin": None, "precision": "exact",
        "affiche_url": None, "source": None, "source_url": None,
        "films": [], "creneaux": [],
    }
    ev.update(kw)
    return ev


# ── Comoedia ──────────────────────────────────────────────────────────────

def _comoedia_event_films(desc_html: str) -> tuple:
    """
    Films et créneaux d'une description Comoedia.

    Les liens `/films/<id>-<slug>/?date=YYYY-MM-DD` donnent le film ET la date
    en clair — c'est ce qui alimente `evenement_films`/`evenement_seances` sans
    aucun matching flou. L'heure, elle, vit dans le paragraphe qui précède le
    lien (« • Dimanche 2 août à 11h00 »), d'où le découpage par <p>.
    """
    films: dict = {}
    slots: list = []
    for bloc in re.split(r"(?i)<p\b", desc_html or ""):
        heure = parse_event_time(_html_to_text(bloc))
        for m in re.finditer(r"<a[^>]+href=\"([^\"]+)\"[^>]*>(.*?)</a>", bloc, re.S | re.I):
            href, inner = m.group(1), m.group(2)
            # Le chemin /films/ doit être CELUI DU COMOEDIA : les descriptions
            # citent aussi des distributeurs (carlottafilms.com/films/…), qui
            # deviendraient sinon des films de l'événement.
            if "/films/" not in href:
                continue
            if href.startswith("http") and "cinema-comoedia.com" not in href:
                continue
            slug_m = re.search(r"/films/([^/?#\"]+)", href)
            slug = slug_m.group(1) if slug_m else ""
            titre = _html_to_text(inner) or _deslugify(slug)
            if not titre:
                continue
            key = _normalize_title_key(titre)
            d_m = re.search(r"[?&]date=(\d{4}-\d{2}-\d{2})", href)
            d_val = d_m.group(1) if d_m else None
            entry = films.setdefault(key, {"titre": titre, "dates": []})
            if d_val and d_val not in entry["dates"]:
                entry["dates"].append(d_val)
            if d_val:
                slots.append({"titre_film": titre, "date": d_val, "heure": heure})
    return list(films.values()), slots


def comoedia_event_from_json(data: dict, source_url: str, ref: "date | None" = None) -> "dict | None":
    """Un événement Comoedia (JSON Gatsby `page-data`) → dict événement (C5)."""
    ref = ref or date.today()
    titre = (data.get("title") or "").strip()
    if not titre:
        return None

    desc_html = data.get("description") or ""
    desc_txt = _html_to_text(desc_html)
    court = (data.get("shortDescription") or "").strip()
    texte = f"{court} {desc_txt}"

    type_ = classify_event_type(texte, data.get("type"))
    _, forme_src = _event_type_forme_from_source(texte, data.get("type"))
    forme = classify_event_forme(f"{titre} {texte}", type_, forme_src)

    films, slots = _comoedia_event_films(desc_html)

    # Enveloppe des dates : les créneaux font foi quand ils existent, sinon le
    # JSON (startAt/endAt), sinon le texte.
    debut = (data.get("startAt") or "")[:10] or None
    fin = (data.get("endAt") or "")[:10] or None
    heure = (data.get("startAt") or "")[11:16] or None
    precision = "exact"
    if slots:
        # Enveloppe = UNION du calendrier annoncé et des créneaux datés. Prendre
        # les seuls créneaux rétrécirait un festival à ses séances documentées
        # (Little Films Festival : 2 liens datés pour 7 semaines de programmation).
        dates = sorted(s["date"] for s in slots)
        debut = min(debut, dates[0]) if debut else dates[0]
        fin = max(fin, dates[-1]) if fin else dates[-1]
        precision = "exact"
    elif debut and not fin:
        if type_ == "festival":
            # Une programmation ouverte sans date de fin : la jointure avec les
            # séances tranchera (§4.3) plutôt que d'inventer une fin.
            precision = "en_cours"
        else:
            fin = debut
    elif not debut:
        debut, fin, precision = parse_event_period(texte, ref)

    creneaux = []
    if slots:
        for s in slots:
            creneaux.append({
                "cinema": "Le Comoedia", "date": s["date"], "heure": s["heure"],
                "titre_film": s["titre_film"], "invite": None,
                "description": court or None, "resa_url": None,
            })
    elif debut and precision == "exact":
        creneaux.append({
            "cinema": "Le Comoedia", "date": debut, "heure": heure,
            "titre_film": titre if not films else None, "invite": None,
            "description": court or None, "resa_url": None,
        })

    if not films and type_ in ("avant_premiere", "rencontre"):
        # Événement-film sans lien : le titre de l'événement EST le film.
        films = [{"titre": titre, "dates": [debut] if debut else []}]

    return _new_event(
        type=type_, forme=forme, titre=titre,
        description=court or (desc_txt[:600] or None),
        date_debut=debut, date_fin=fin, precision=precision,
        affiche_url=data.get("poster") or None,
        source="comoedia", source_url=source_url,
        films=films, creneaux=creneaux,
    )


def scrape_comoedia_events(ref: "date | None" = None) -> list:
    """Événements du Comoedia : page liste (pour les slugs) + JSON de détail."""
    try:
        html = fetch(URL_COMOEDIA_EVENTS, timeout=20)
    except RuntimeError as e:
        log.warning(f"Comoedia événements : liste inaccessible ({e})")
        return []

    slugs = sorted(set(re.findall(r"href=\"/events/(\d+-[^\"/]+)/?\"", html)))
    log.info(f"Comoedia événements : {len(slugs)} fiches à lire")

    events = []
    for slug in slugs:
        url = f"{URL_COMOEDIA_BASE}/page-data/events/{slug}/page-data.json"
        try:
            data = json.loads(fetch(url, timeout=15))["result"]["data"]["event"]
        except Exception as e:
            log.warning(f"  Comoedia événement {slug} illisible : {e}")
            continue
        if not data:
            continue
        ev = comoedia_event_from_json(
            data, f"{URL_COMOEDIA_BASE}/events/{slug}/", ref)
        if ev:
            events.append(ev)
        time.sleep(0.2)

    log.info(f"Comoedia : {len(events)} événements extraits")
    return events


# ── Cinémas Lumière ───────────────────────────────────────────────────────

def _lumiere_abs(url: str) -> str:
    if not url:
        return ""
    if url.startswith("http"):
        return url
    return f"{URL_LUMIERE_SITE}/{url.lstrip('/')}"


def _lumiere_row_films(fragment: str) -> list:
    """
    Films liés d'une ligne : liens `film/<slug>.html`, puis — à défaut — les
    items de liste en italique (« <li><em>Le château de l'araignée</em> (1957)
    </li> », rétrospective Kurosawa : les titres y sont annoncés SANS lien).
    """
    films: dict = {}
    for m in re.finditer(r"<a[^>]+href=\"([^\"]*film/[^\"]+\.html)\"[^>]*>(.*?)</a>",
                         fragment, re.S | re.I):
        inner = _html_to_text(m.group(2))
        if not inner or "lire la suite" in inner.lower():
            continue
        slug_m = re.search(r"film/([^/\"]+)\.html", m.group(1))
        titre = inner if len(inner) > 1 else _deslugify(slug_m.group(1) if slug_m else "")
        if not titre:
            continue
        films.setdefault(_normalize_title_key(titre), {"titre": titre, "dates": []})

    for m in re.finditer(r"<li\b[^>]*>(.*?)</li>", fragment, re.S | re.I):
        if "href" in m.group(1):
            continue                      # déjà pris par la passe « liens »
        em = re.search(r"<em\b[^>]*>(.*?)</em>", m.group(1), re.S | re.I)
        if not em:
            continue
        titre = re.sub(r"\s*\(\d{4}\)\s*$", "", _html_to_text(em.group(1))).strip()
        if 1 < len(titre) < 90:
            films.setdefault(_normalize_title_key(titre), {"titre": titre, "dates": []})
    return list(films.values())


def lumiere_event_from_row(fragment: str, section: str, page_url: str,
                           ref: "date | None" = None) -> "dict | None":
    """
    Une ligne de tableau Lumière → dict événement (C5).

    Les pages Lumière n'ont pas de champ type : on classe par PRÉFIXE EN GRAS
    (`AVANT-PREMIÈRE`, `SÉANCE SPÉCIALE`, `CYCLE …`) croisé avec la section
    <h2>, et le lieu se lit dans le texte (« au Lumière Bellecour »).
    """
    ref = ref or date.today()
    texte = _html_to_text(fragment)
    if len(texte) < 40:
        return None

    h1_m = re.search(r"<h1\b[^>]*>(.*?)</h1>", fragment, re.S | re.I)
    titre_bloc = _html_to_text(h1_m.group(1)) if h1_m else None

    # Un cycle est éclaté en une ligne PAR FILM : c'est le nom du cycle (en
    # capitales) qui les recolle en un seul événement — la fusion se fait plus
    # loin, sur le titre. On s'arrête au premier mot non capitalisé.
    cycle_m = re.search(r"CYCLE\s+([A-ZÀ-ÖØ-Þ0-9'’\s-]{3,60})", texte)
    titre_cycle = None
    if cycle_m:
        nom = re.sub(r"\s+", " ", cycle_m.group(1)).strip()
        # La capture déborde d'une initiale sur le mot suivant (« … 50 - A|ctuellement »).
        nom = re.sub(r"\s+[A-ZÀ-ÖØ-Þ]$", "", nom).strip(" -–|")
        titre_cycle = "Cycle " + (nom[:1] + nom[1:].lower() if nom else "")

    # Titre porté par le lien vers la page événement (rétrospective Tati : ni
    # <h1> ni cycle, le titre est le libellé du lien `evenement/…`). Le premier
    # de ces liens est l'affiche (contenu = <img>) : on prend le premier qui
    # porte vraiment du texte.
    titre_lien = None
    for inner in re.findall(
            r"<a[^>]+href=\"[^\"]*evenement/[^\"]+\.html\"[^>]*>(.*?)</a>", fragment, re.S | re.I):
        cand = _html_to_text(inner)
        if cand and "lire la suite" not in cand.lower() and 2 < len(cand) < 120:
            titre_lien = cand
            break

    films = _lumiere_row_films(fragment)
    titre = titre_bloc or titre_cycle or titre_lien or (films[0]["titre"] if films else None)
    if not titre:
        return None
    # Le titre de l'événement n'est pas un de ses films (Tati, Kurosawa…).
    if titre_bloc or titre_cycle or titre_lien:
        films = [f for f in films if _normalize_title_key(f["titre"]) != _normalize_title_key(titre)]

    section_type = {
        "SÉANCES SPÉCIALES": ("seance_speciale", None),
        "FESTIVALS": ("festival", "festival"),
        "FILMS CLASSIQUES": ("seance_speciale", None),
    }.get((section or "").strip().upper(), (None, None))

    # Le préfixe en gras ouvre la ligne : le chercher dans TOUT le texte ferait
    # passer « Little Films Festival … 2 avant-premières exclusives » pour une
    # avant-première. Hors préfixe, la section <h2> fait foi.
    prefixe_type, prefixe_forme = _event_type_forme_from_source(texte[:200], None)
    type_ = classify_event_type(texte, None, primaire=prefixe_type or section_type[0])
    if re.search(r"rétrospective|festival|\bcycle\b", titre, re.I) and type_ != "rencontre":
        type_ = "festival"
    forme = classify_event_forme(f"{titre} {texte}", type_,
                                 prefixe_forme or (section_type[1] if not prefixe_type else None))

    debut, fin, precision = parse_event_period(texte, ref)
    heure = parse_event_time(texte)
    cinema = _cinema_from_text(texte)

    img_m = re.search(r"<img[^>]+src=\"([^\"]+)\"", fragment, re.I)
    affiche = _lumiere_abs(img_m.group(1)) if img_m else None

    # L'invité vit dans l'italique de la ligne — mais l'italique sert AUSSI aux
    # titres de films sur ces pages : on ne retient que ce qui décrit vraiment
    # une présentation de séance.
    invite = None
    for em in re.findall(r"<em\b[^>]*>(.*?)</em>", fragment, re.S | re.I):
        cand = _html_to_text(em)
        if 15 < len(cand) < 300 and re.search(
                r"présent|présence|animée?\s+par|échange|partenariat|dans le cadre|suivi",
                cand, re.I):
            invite = cand
            break

    resa_url = None
    for href in re.findall(r"href=\"([^\"]+)\"", fragment):
        if is_valid_resa_url(href):
            resa_url = href
            break

    ev_link_m = re.search(r"href=\"([^\"]*evenement/[^\"]+\.html)\"", fragment, re.I)
    source_url = _lumiere_abs(ev_link_m.group(1)) if ev_link_m else page_url

    # Ligne de film simple (pas de titre d'événement propre) : l'événement EST
    # le film. Un festival/cycle/rétrospective, lui, garde une liste vide tant
    # que sa page détail n'a pas répondu — surtout pas lui-même comme film.
    if not films and not (titre_bloc or titre_cycle or titre_lien):
        films = [{"titre": titre, "dates": []}]

    creneaux = []
    if cinema:
        creneaux.append({
            "cinema": cinema,
            "date": debut if precision == "exact" and debut == fin else None,
            "heure": heure,
            "titre_film": films[0]["titre"] if len(films) == 1 else None,
            "invite": invite, "description": None, "resa_url": resa_url,
        })

    return _new_event(
        type=type_, forme=forme, titre=titre,
        description=_lumiere_row_description(fragment),
        date_debut=debut, date_fin=fin, precision=precision,
        affiche_url=affiche, source="lumiere", source_url=source_url,
        films=films, creneaux=creneaux,
    )


def _lumiere_row_description(fragment: str) -> "str | None":
    """
    Chapô d'une ligne : on saute la ligne de date et la fiche technique
    (« Espagne | 2025 | 1h40 | VOSTF ») pour garder la vraie description.
    """
    for bloc in re.split(r"(?i)</p>", fragment):
        txt = _html_to_text(bloc)
        if len(txt) < 60:
            continue
        if txt.count("|") >= 2 or re.search(r"lire la suite", txt, re.I):
            continue
        # Ligne de date (elle peut être précédée du titre) : ce n'est pas le chapô.
        if re.search(r"\b(du|jusqu['’]au|dès|à partir du|actuellement)\b", txt[:90], re.I):
            continue
        if re.match(rf"^\W*(?:{_JOUR_ALT})\s+\d{{1,2}}", txt, re.I):
            continue
        return txt[:600]
    return None


def _lumiere_event_page_films(url: str) -> list:
    """Films listés sur une page événement Lumière (`evenement/<slug>.html`)."""
    try:
        html = fetch(url, timeout=15)
    except RuntimeError as e:
        log.warning(f"  Lumière page événement inaccessible ({url}) : {e}")
        return []
    body = html[html.find("<body"):] or html
    return _lumiere_row_films(body)


def scrape_lumiere_events(ref: "date | None" = None, suivre_details: bool = True) -> list:
    """
    Événements des Cinémas Lumière : `evenement.html` + `rendez-vous.html`.

    ⚠️ `avant-premieres.html` n'est JAMAIS ingérée : page non purgée, contenu
    périmé de plusieurs semaines (vérifié 2026-07-27).
    """
    events = []
    for page_url in (URL_LUMIERE_EVENTS, URL_LUMIERE_RDV):
        try:
            html = fetch(page_url, timeout=20)
        except RuntimeError as e:
            log.warning(f"Lumière événements : {page_url} inaccessible ({e})")
            continue
        body = html[html.find("<body"):] or html

        # Sections <h2> et lignes <tr> dans l'ordre du document : chaque ligne
        # hérite de la dernière section rencontrée.
        marqueurs = [(m.start(), "h2", _html_to_text(m.group(1)))
                     for m in re.finditer(r"<h2\b[^>]*>(.*?)</h2>", body, re.S | re.I)]
        marqueurs += [(m.start(), "tr", m.group(0))
                      for m in re.finditer(r"<tr\b[^>]*>.*?</tr>", body, re.S | re.I)]
        marqueurs.sort(key=lambda x: x[0])

        section = ""
        for _, kind, payload in marqueurs:
            if kind == "h2":
                section = payload
                continue
            ev = lumiere_event_from_row(payload, section, page_url, ref)
            if ev:
                events.append(ev)

    # Pages événement dédiées : elles portent la liste complète des films
    # (rétrospectives, festivals) que la ligne de tableau ne donne pas toujours.
    if suivre_details:
        vues = set()
        for ev in events:
            url = ev.get("source_url") or ""
            if "evenement/" not in url or url in vues:
                continue
            vues.add(url)
            extra = _lumiere_event_page_films(url)
            connus = {_normalize_title_key(f["titre"]) for f in ev["films"]}
            connus.add(_normalize_title_key(ev["titre"]))   # l'événement n'est pas son propre film
            for f in extra:
                key = _normalize_title_key(f["titre"])
                if key not in connus:
                    connus.add(key)
                    ev["films"].append(f)
            time.sleep(0.2)

    log.info(f"Lumière : {len(events)} événements extraits")
    return events


# ── Fusion inter-sources ──────────────────────────────────────────────────

def _event_film_keys(ev: dict) -> set:
    return {_normalize_title_key(f["titre"]) for f in ev.get("films", []) if f.get("titre")}


def _event_dates(ev: dict) -> list:
    """Toutes les dates connues d'un événement (enveloppe, créneaux, films)."""
    dates = {d for d in (ev.get("date_debut"), ev.get("date_fin")) if d}
    dates |= {c["date"] for c in ev.get("creneaux", []) if c.get("date")}
    for f in ev.get("films", []):
        dates |= set(f.get("dates") or [])
    return sorted(dates)


def _events_proches(a: dict, b: dict) -> bool:
    """
    Fenêtre temporelle de la clé de dédup (§2). Sans elle, deux avant-premières
    du même film à trois mois d'écart fusionneraient (cas réel « Notre salut » :
    Comoedia le 28 août, Lumière le 21 septembre — deux événements distincts).
    Deux événements sans aucune date (programmations « en cours ») sont
    considérés comme proches : c'est leur titre qui tranchera.
    """
    da, db = _event_dates(a), _event_dates(b)
    if not da or not db:
        return True
    for x in da:
        for y in db:
            if abs((date.fromisoformat(x) - date.fromisoformat(y)).days) <= EVENT_DEDUP_WINDOW_DAYS:
                return True
    return False


def _meme_evenement(a: dict, b: dict) -> bool:
    if a.get("type") != b.get("type"):
        return False
    if not _events_proches(a, b):
        return False
    if _normalize_title_key(a["titre"]) == _normalize_title_key(b["titre"]):
        return True
    ka, kb = _event_film_keys(a), _event_film_keys(b)
    if len(ka) == 1 and ka == kb:
        return True          # même film unique, même type, même fenêtre
    # Programmation partagée (une rétrospective annoncée par deux salles) — mais
    # la MÊME forme est exigée : un festival de classiques englobe volontiers
    # les films d'une rétrospective sans être cette rétrospective (cas réel :
    # « Plein Soleil sur les Classiques » ⊃ les 6 Tati).
    return len(ka & kb) >= 2 and a.get("forme") == b.get("forme")


_PRECISION_RANG = {"exact": 0, "jour": 1, "mois": 2, "saison": 3, "en_cours": 4}


def _fusionne_paire(a: dict, b: dict) -> dict:
    """
    Fusionne b dans a. Le titre canonique suit la PRIORITÉ DE SOURCE (§9.2)
    — surtout pas « le premier récupéré » : main() scrape Comoedia en premier,
    Comoedia gagnerait donc systématiquement et le titre basculerait le jour où
    un scraper échoue.
    """
    pa = EVENT_SOURCE_PRIORITY.get(a.get("source"), 9)
    pb = EVENT_SOURCE_PRIORITY.get(b.get("source"), 9)
    maitre, autre = (a, b) if pa <= pb else (b, a)

    fusion = dict(maitre)
    fusion["description"] = maitre.get("description") or autre.get("description")
    fusion["affiche_url"] = maitre.get("affiche_url") or autre.get("affiche_url")
    fusion["forme"] = maitre.get("forme") or autre.get("forme")
    # Une rencontre l'emporte : une personne est annoncée quelque part, c'est
    # l'information la plus forte (l'invité, lui, reste porté par son créneau).
    if "rencontre" in (a.get("type"), b.get("type")):
        fusion["type"] = "rencontre"
        fusion["forme"] = None

    debuts = [x for x in (a.get("date_debut"), b.get("date_debut")) if x]
    fins = [x for x in (a.get("date_fin"), b.get("date_fin")) if x]
    fusion["date_debut"] = min(debuts) if debuts else None
    fusion["date_fin"] = max(fins) if fins else None
    fusion["precision"] = min((a.get("precision", "en_cours"), b.get("precision", "en_cours")),
                              key=lambda p: _PRECISION_RANG.get(p, 9))

    films = list(maitre.get("films") or [])
    connus = {_normalize_title_key(f["titre"]) for f in films}
    for f in autre.get("films") or []:
        k = _normalize_title_key(f["titre"])
        if k in connus:
            cible = next(x for x in films if _normalize_title_key(x["titre"]) == k)
            cible["dates"] = sorted(set(cible.get("dates") or []) | set(f.get("dates") or []))
        else:
            connus.add(k)
            films.append(dict(f))
    fusion["films"] = films
    fusion["creneaux"] = list(a.get("creneaux") or []) + list(b.get("creneaux") or [])
    fusion["sources"] = sorted(set(
        (a.get("sources") or [a.get("source")]) + (b.get("sources") or [b.get("source")])))
    return fusion


def merge_events(events: list) -> list:
    """Dédup inter-sources : `film + type + fenêtre ±14 j` (§2)."""
    fusionnes: list = []
    for ev in events:
        for i, deja in enumerate(fusionnes):
            if _meme_evenement(deja, ev):
                fusionnes[i] = _fusionne_paire(deja, ev)
                break
        else:
            copie = dict(ev)
            copie["sources"] = [ev.get("source")]
            fusionnes.append(copie)
    log.info(f"Événements : {len(events)} bruts → {len(fusionnes)} après dédup")
    return fusionnes


# ── Jointure avec les séances scrapées (§4.3) ─────────────────────────────

def _seances_index(films: list) -> dict:
    """titre normalisé → séances scrapées (cinéma, date, heure, lien resa)."""
    idx: dict = {}
    for f in films:
        key = _normalize_title_key(f.get("titre") or "")
        if not key:
            continue
        for s in f.get("seances") or []:
            if not s.get("date"):
                continue
            idx.setdefault(key, []).append({
                "cinema": f.get("cinema"), "date": s["date"],
                "heure": s.get("heure"), "resa_url": s.get("resa_url"),
                "titre_film": f.get("titre"),
            })
    return idx


def resolve_dates_from_seances(events: list, films: list,
                               today: "date | None" = None) -> list:
    """
    Résout les événements sans date par JOINTURE avec les séances déjà scrapées
    (§4.3) — jamais par inférence. Un événement qu'aucune séance ne peut dater
    n'est PAS affiché : pas de fantôme.
    """
    today = today or date.today()
    idx = _seances_index(films)
    gardes = []

    for ev in events:
        cinemas = {c["cinema"] for c in ev.get("creneaux", []) if c.get("cinema")}
        borne_haute = ev.get("date_fin")
        borne_basse = ev.get("date_debut")
        # Une date unique et exacte est un ordre du programmateur, pas une
        # enveloppe : ne pas y agréger toutes les séances du film.
        etendable = not (ev.get("precision") == "exact"
                         and borne_basse and borne_basse == borne_haute)

        for f in ev.get("films", []):
            for s in idx.get(_normalize_title_key(f["titre"]), []):
                if cinemas and s["cinema"] not in cinemas:
                    continue
                if s["date"] < today.isoformat():
                    continue
                if not etendable:
                    continue
                if borne_basse and s["date"] < borne_basse:
                    continue
                if borne_haute and s["date"] > borne_haute:
                    continue
                if s["date"] not in (f.get("dates") or []):
                    f.setdefault("dates", []).append(s["date"])
                # L'identité d'un créneau inclut le FILM : trois films d'un
                # cycle peuvent partager salle, date et horaire d'affichage.
                deja = any(c.get("cinema") == s["cinema"] and c.get("date") == s["date"]
                           and c.get("heure") == s["heure"]
                           and c.get("titre_film") == s["titre_film"] for c in ev["creneaux"])
                if not deja:
                    ev["creneaux"].append({
                        "cinema": s["cinema"], "date": s["date"], "heure": s["heure"],
                        "titre_film": s["titre_film"], "invite": None,
                        "description": None, "resa_url": s["resa_url"],
                    })

        # Un créneau sans date n'existait que pour porter la salle (et parfois
        # l'invité) tant qu'aucune séance n'était connue : dès qu'une séance
        # datée arrive pour cette salle, il fait doublon. On ne le garde que
        # s'il porte du contenu éditorial (invité / descriptif de séance).
        salles_datees = {c["cinema"] for c in ev["creneaux"] if c.get("date")}
        ev["creneaux"] = [c for c in ev["creneaux"]
                          if c.get("date") or c.get("invite") or c.get("description")
                          or c.get("cinema") not in salles_datees]

        # La jointure COMPLÈTE les dates manquantes (tri, mois couverts,
        # filtrage du passé) mais ne touche JAMAIS à `precision` : celle-ci dit
        # ce que la SOURCE a annoncé. Promouvoir en « exact » une enveloppe
        # déduite d'une semaine de séances ferait afficher une période fausse et
        # mouvante là où « En cours » est la seule information vraie.
        dates = _event_dates(ev)
        if dates:
            ev["date_debut"] = ev.get("date_debut") or dates[0]
            ev["date_fin"] = ev.get("date_fin") or max(dates)

        if not ev.get("date_debut") and not ev.get("date_fin"):
            log.debug(f"  Événement sans date résoluble — ignoré : {ev['titre']}")
            continue
        gardes.append(ev)

    log.info(f"Événements : {len(gardes)}/{len(events)} datés après jointure")
    return gardes


def filter_events_current(events: list, today: "date | None" = None) -> list:
    """
    Filtre le passé À L'INGESTION (§3.3) : aucune source ne purge (Comoedia
    gardait 4 événements écoulés le 2026-07-27, `avant-premieres.html` de
    Lumière est intégralement périmée).
    """
    today = today or date.today()
    iso = today.isoformat()
    gardes = []
    for ev in events:
        dates = _event_dates(ev)
        fin = ev.get("date_fin") or (max(dates) if dates else None) or ev.get("date_debut")
        if fin and fin < iso:
            continue
        gardes.append(ev)
    if len(gardes) != len(events):
        log.info(f"Événements : {len(events) - len(gardes)} écoulés écartés")
    return gardes


def event_dedup_key(ev: dict) -> str:
    """
    Clé stable d'un événement en base (`evenements.cle`) : rend l'upsert
    idempotent d'un run à l'autre. Bucket MENSUEL plutôt que date exacte —
    une source qui corrige sa date d'un jour ne doit pas créer une 2e ligne.
    """
    films = sorted(_event_film_keys(ev))
    identite = films[0] if len(films) == 1 else _normalize_title_key(ev["titre"])
    ancre = ev.get("date_debut") or ev.get("date_fin")
    return f"{ev['type']}|{identite}|{ancre[:7] if ancre else 'ouvert'}"


# ── Résumé du mois (API Claude) ───────────────────────────────────────────

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL   = "claude-haiku-4-5-20251001"
ANTHROPIC_URL     = "https://api.anthropic.com/v1/messages"

# Liste FERMÉE d'icônes : le front ne rend qu'un pictogramme de cette liste
# (SVG inline). Le modèle ne peut donc pas inventer une icône introuvable.
EVENT_ICONS = (
    "movie", "festival", "jeune_public", "musique", "monde",
    "art", "documentaire", "patrimoine", "rencontre", "frisson",
)

_RESUME_PROMPT = """Tu rédiges, pour un agrégateur de cinémas indépendants lyonnais, \
la phrase d'accroche du mois de {mois}.

Programmation du mois :
{liste}

Réponds UNIQUEMENT par un tableau JSON de segments, sans texte autour. Chaque segment est :
  {{"t": "<texte>", "s": "strong"|"mute"}}   ou   {{"icon": "<nom>"}}

Règles :
- `strong` : le mois, les TYPES d'événement, et les récurrences de type (« deux festivals », « 3 avant-premières »).
- `mute` : tout le reste — liant, noms propres, thématiques.
- `icon` devant une sous-catégorie (jeune public, cinéma ibérique…) ; si aucune icône ne convient, \
place-la devant la catégorie. Icônes disponibles : {icones}.
- Une seule phrase fluide, commençant par « En {mois}, ». Reste synthétique quand le mois est chargé.
- ⚠️ JAMAIS de première personne ni d'appropriation des salles : pas de « nos cinémas », \
« nos salles », « chez nous », « nous vous proposons ». Le site AGRÈGE une programmation, il ne \
l'organise pas et n'appartient à aucune salle ni à aucun réseau. Écrire « les cinémas indépendants \
lyonnais », « à l'affiche », ou tourner la phrase sans sujet possessif.
- Français irréprochable : accords en genre et en nombre, énumérations cohérentes. La phrase est \
publiée telle quelle, sans relecture humaine.
- Les espaces font partie des segments (le rendu concatène sans séparateur).

Exemple de forme :
[{{"t":"En ","s":"mute"}},{{"t":"Juillet","s":"strong"}},{{"t":", retrouvez ","s":"mute"}},\
{{"icon":"movie"}},{{"t":"une rétrospective","s":"strong"}},{{"t":" sur Jacques Tati","s":"mute"}}]"""


def _valider_segments(data) -> "list | None":
    """
    Valide la réponse du modèle. La programmation vient de pages tierces : on ne
    fait confiance ni au contenu ni à la forme — seules les clés attendues,
    typées et bornées, passent. Toute anomalie ⇒ None ⇒ pas de bloc résumé
    (le fallback prévu au brief : on n'invente pas de phrase de secours).
    """
    if not isinstance(data, list) or not (0 < len(data) <= 40):
        return None
    out = []
    total = 0
    for seg in data:
        if not isinstance(seg, dict):
            return None
        if "icon" in seg:
            if seg["icon"] not in EVENT_ICONS:
                continue          # icône inconnue : on la laisse tomber, pas le résumé
            out.append({"icon": seg["icon"]})
            continue
        texte, style = seg.get("t"), seg.get("s")
        if not isinstance(texte, str) or style not in ("strong", "mute"):
            return None
        total += len(texte)
        out.append({"t": texte[:120], "s": style})
    if not out or total > 400:
        return None
    return out


def generate_month_summary(events: list, mois: str) -> "list | None":
    """
    Résumé du mois en segments typés (§5), via l'API Claude (Haiku).

    Le modèle ne renvoie pas une phrase mais un tableau de segments — c'est ce
    qui rend le rendu multicolore déterministe côté front. Sans clé API, ou si
    la réponse ne valide pas, on renvoie None : le bloc est alors ABSENT.
    """
    if not ANTHROPIC_API_KEY:
        log.info(f"ANTHROPIC_API_KEY absente — pas de résumé pour {mois}")
        return None
    if not events:
        return None

    libelle_mois = f"{[k for k, v in MOIS_FR.items() if v == int(mois[5:7])][0]} {mois[:4]}"
    lignes = []
    for ev in events[:25]:
        etiquette = ev.get("forme") or ev.get("type")
        lignes.append(f"- [{etiquette}] {ev['titre'][:80]}")

    payload = json.dumps({
        "model": ANTHROPIC_MODEL,
        "max_tokens": 800,
        "messages": [{"role": "user", "content": _RESUME_PROMPT.format(
            mois=libelle_mois, liste="\n".join(lignes), icones=", ".join(EVENT_ICONS))}],
    }).encode("utf-8")

    req = Request(ANTHROPIC_URL, data=payload, headers={
        "content-type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    })
    try:
        with urlopen(req, timeout=30) as r:
            rep = json.loads(r.read().decode("utf-8"))
        texte = "".join(b.get("text", "") for b in rep.get("content", []))
    except (HTTPError, URLError, ValueError) as e:
        log.warning(f"Résumé {mois} : appel API échoué ({e}) — bloc omis")
        return None

    m = re.search(r"\[.*\]", texte, re.S)
    if not m:
        log.warning(f"Résumé {mois} : réponse sans tableau JSON — bloc omis")
        return None
    try:
        segments = _valider_segments(json.loads(m.group(0)))
    except ValueError:
        segments = None
    if not segments:
        log.warning(f"Résumé {mois} : JSON invalide — bloc omis")
        return None
    log.info(f"Résumé {mois} : {len(segments)} segments")
    return segments


# ── Affiches : rapatriement dans Supabase Storage ─────────────────────────

def _affiche_path(url: str) -> str:
    ext = ".jpg"
    for cand in (".png", ".webp", ".jpeg", ".jpg"):
        if cand in url.lower():
            ext = ".jpg" if cand == ".jpeg" else cand
            break
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16] + ext


def _rapatrie_affiche(client, url: str, existants: set) -> "str | None":
    """
    Copie une affiche de source dans le bucket `affiches` et renvoie son URL
    publique. Les CDN des salles renvoient des 403 en hotlink et leurs URLs
    tournent — on ne peut pas s'y fier pour un affichage durable.
    """
    if not url or not url.startswith("http"):
        return None
    chemin = _affiche_path(url)
    base = os.getenv("SUPABASE_URL", "").rstrip("/")
    public = f"{base}/storage/v1/object/public/affiches/{chemin}"
    if chemin in existants:
        return public
    try:
        with urlopen(Request(url, headers=HEADERS), timeout=20) as r:
            data = r.read()
        if not data or len(data) > 5_000_000:
            return None
        mime = "image/png" if chemin.endswith(".png") else (
            "image/webp" if chemin.endswith(".webp") else "image/jpeg")
        client.storage.from_("affiches").upload(
            chemin, data, {"content-type": mime, "upsert": "true"})
        existants.add(chemin)
        return public
    except Exception as e:
        log.warning(f"Affiche non rapatriée ({url[:70]}) : {e}")
        return None


# ── Upsert Supabase ───────────────────────────────────────────────────────

def _mois_couverts(ev: dict) -> list:
    """Mois 'YYYY-MM' traversés par un événement (borné à 12 pour les ouverts)."""
    debut = ev.get("date_debut") or ev.get("date_fin")
    fin = ev.get("date_fin") or ev.get("date_debut")
    if not debut:
        return []
    d = date.fromisoformat(debut).replace(day=1)
    f = date.fromisoformat(fin).replace(day=1)
    mois = []
    while d <= f and len(mois) < 12:
        mois.append(d.isoformat()[:7])
        d = (d + timedelta(days=32)).replace(day=1)
    return mois


def upsert_events_to_supabase(events: list, force_resume: bool = False) -> None:
    """
    Upsert des événements, de leurs films et de leurs créneaux.

    Les tables filles sont RÉÉCRITES (delete + insert) : un film retiré de la
    programmation doit disparaître, ce qu'un simple upsert ne fait pas.
    `seance_id` / `film_id` sont résolus opportunistement — la plupart resteront
    NULL au-delà de la semaine scrapée, c'est le régime normal (cf. migration).
    """
    sb_url = os.getenv("SUPABASE_URL")
    sb_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not sb_url or not sb_key:
        log.info("SUPABASE_* absents — upsert des événements ignoré")
        return
    if not events:
        return
    try:
        from supabase import create_client
        client = create_client(sb_url, sb_key)
    except Exception as e:
        log.error(f"Connexion Supabase impossible : {e}")
        return

    today = date.today().isoformat()
    cinema_ids: dict = {}
    try:
        for c in client.table("cinemas").select("id,name").execute().data or []:
            cinema_ids[c["name"]] = c["id"]
    except Exception as e:
        log.error(f"Lecture des cinémas impossible : {e}")
        return

    films_par_titre: dict = {}
    try:
        for f in client.table("films").select("id,titre,poster").execute().data or []:
            films_par_titre.setdefault(_normalize_title_key(f["titre"]), f)
    except Exception as e:
        log.warning(f"Index films indisponible ({e}) — liens film non résolus")

    seances_idx: dict = {}
    try:
        rows = client.table("seances").select(
            "id,film_id,cinema_id,date,heure").gte("date", today).execute().data or []
        for s in rows:
            seances_idx[(s["film_id"], s["cinema_id"], s["date"], (s["heure"] or "")[:5])] = s["id"]
    except Exception as e:
        log.warning(f"Index séances indisponible ({e}) — seance_id non résolus")

    affiches_existantes = set()
    try:
        for obj in client.storage.from_("affiches").list(
                options={"limit": 1000}) or []:
            affiches_existantes.add(obj.get("name"))
    except Exception as e:
        log.debug(f"Bucket affiches non listé : {e}")

    n_films = n_creneaux = 0
    for ev in events:
        affiche = _rapatrie_affiche(client, ev.get("affiche_url"), affiches_existantes) \
            or ev.get("affiche_url")
        row = {
            "cle": ev["cle"], "type": ev["type"], "forme": ev.get("forme"),
            "titre": ev["titre"], "description": ev.get("description"),
            "date_debut": ev.get("date_debut"), "date_fin": ev.get("date_fin"),
            "precision": ev.get("precision") or "exact",
            "affiche_url": affiche, "source": ev.get("source"),
            "source_url": ev.get("source_url"),
            "updated_at": datetime.now().isoformat(),
        }
        try:
            r = client.table("evenements").upsert(row, on_conflict="cle").execute()
            ev_id = r.data[0]["id"] if r.data else None
            if not ev_id:
                ev_id = client.table("evenements").select("id").eq(
                    "cle", ev["cle"]).execute().data[0]["id"]
        except Exception as e:
            log.warning(f"Événement non upserté (« {ev['titre']} ») : {e}")
            continue
        ev["id"] = ev_id

        try:
            client.table("evenement_films").delete().eq("evenement_id", ev_id).execute()
            client.table("evenement_seances").delete().eq("evenement_id", ev_id).execute()
        except Exception as e:
            log.warning(f"Purge des liens échouée (« {ev['titre']} ») : {e}")

        lignes_films = []
        for i, f in enumerate(ev.get("films") or []):
            key = _normalize_title_key(f["titre"])
            connu = films_par_titre.get(key)
            lignes_films.append({
                "evenement_id": ev_id, "film_id": connu["id"] if connu else None,
                "titre": f["titre"], "titre_key": key,
                "affiche_url": (connu or {}).get("poster"), "ordre": i,
            })
        if lignes_films:
            try:
                client.table("evenement_films").insert(lignes_films).execute()
                n_films += len(lignes_films)
            except Exception as e:
                log.warning(f"Films non liés (« {ev['titre']} ») : {e}")

        lignes_creneaux = []
        vus = set()
        for c in ev.get("creneaux") or []:
            cid = cinema_ids.get(c.get("cinema"))
            if not cid:
                continue
            titre_film = c.get("titre_film")
            fkey = _normalize_title_key(titre_film) if titre_film else None
            film = films_par_titre.get(fkey) if fkey else None
            heure = (c.get("heure") or "")[:5] or None
            signature = (cid, c.get("date"), heure, fkey)
            if signature in vus:
                continue
            vus.add(signature)
            lignes_creneaux.append({
                "evenement_id": ev_id, "cinema_id": cid,
                "date": c.get("date"), "heure": (heure + ":00") if heure else None,
                "film_id": film["id"] if film else None,
                "seance_id": seances_idx.get(
                    (film["id"] if film else None, cid, c.get("date"), heure)),
                "titre_film": titre_film, "invite": c.get("invite"),
                "description": c.get("description"),
                "resa_url": c.get("resa_url") if is_valid_resa_url(c.get("resa_url")) else None,
            })
        if lignes_creneaux:
            try:
                client.table("evenement_seances").insert(lignes_creneaux).execute()
                n_creneaux += len(lignes_creneaux)
            except Exception as e:
                log.warning(f"Créneaux non insérés (« {ev['titre']} ») : {e}")

    # Données mensuelles : graine de tirage (renouvelée à chaque run, stable
    # entre deux runs pour tous les visiteurs) + résumé.
    par_mois: dict = {}
    for ev in events:
        for m in _mois_couverts(ev):
            par_mois.setdefault(m, []).append(ev)

    existants: dict = {}
    try:
        for r in client.table("evenement_mois").select(
                "mois,resume_segments,resume_generated_at").execute().data or []:
            existants[r["mois"]] = r
    except Exception as e:
        log.warning(f"Lecture evenement_mois impossible : {e}")

    for mois, evs_mois in sorted(par_mois.items()):
        if mois < today[:7]:
            continue
        precedent = existants.get(mois) or {}
        segments = precedent.get("resume_segments")
        genere = precedent.get("resume_generated_at")
        frais = False
        if genere:
            try:
                frais = (datetime.now() - datetime.fromisoformat(
                    genere.replace("Z", "+00:00")).replace(tzinfo=None)).days < 7
            except ValueError:
                frais = False
        if force_resume or not (segments and frais):
            nouveau = generate_month_summary(evs_mois, mois)
            if nouveau:
                segments = nouveau
                genere = datetime.now().isoformat()
        try:
            client.table("evenement_mois").upsert({
                "mois": mois,
                "selection_seed": os.urandom(4).hex(),
                "resume_segments": segments,
                "resume_generated_at": genere,
                "updated_at": datetime.now().isoformat(),
            }, on_conflict="mois").execute()
        except Exception as e:
            log.warning(f"Mois {mois} non upserté : {e}")

    log.info(f"Supabase : {len(events)} événements, {n_films} films liés, "
             f"{n_creneaux} créneaux, {len(par_mois)} mois")


# ── Orchestrateur ─────────────────────────────────────────────────────────

def scrape_events(films: list, ref: "date | None" = None,
                  sans_comoedia: bool = False, sans_lumiere: bool = False) -> list:
    """
    Pipeline complet des événements : extraction → fusion → jointure → filtrage.
    `films` = les films scrapés du run, qui servent à dater les événements sans
    date et à rattacher les séances (§4.3).
    """
    ref = ref or date.today()
    bruts: list = []
    if not sans_comoedia:
        bruts += scrape_comoedia_events(ref)
    if not sans_lumiere:
        bruts += scrape_lumiere_events(ref)
    if not bruts:
        log.warning("Aucun événement extrait")
        return []

    events = merge_events(bruts)
    events = resolve_dates_from_seances(events, films, ref)
    events = filter_events_current(events, ref)

    vues: dict = {}
    for ev in events:
        cle = event_dedup_key(ev)
        if cle in vues:
            vues[cle] += 1
            cle = f"{cle}#{vues[cle]}"
        else:
            vues[cle] = 1
        ev["cle"] = cle

    events.sort(key=lambda e: (e.get("date_debut") or "9999", e["titre"]))
    return events


def events_public(events: list) -> list:
    """Forme servie au front (Supabase et repli `programme.json` alignés)."""
    return [{
        "cle": ev["cle"], "type": ev["type"], "forme": ev.get("forme"),
        "titre": ev["titre"], "description": ev.get("description"),
        "date_debut": ev.get("date_debut"), "date_fin": ev.get("date_fin"),
        "precision": ev.get("precision"), "affiche_url": ev.get("affiche_url"),
        "films": [{"titre": f["titre"], "dates": sorted(f.get("dates") or [])}
                  for f in ev.get("films") or []],
        "creneaux": [{k: c.get(k) for k in
                      ("cinema", "date", "heure", "titre_film", "invite", "description")}
                     for c in ev.get("creneaux") or []],
    } for ev in events]


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


def _tmdb_trailer(tmdb_id: int) -> str | None:
    """URL YouTube de la bande-annonce TMDB : trailer officiel FR en priorité,
    puis trailer FR, puis EN, puis teaser. None si aucune vidéo YouTube."""
    for lang in ("fr-FR", "en-US"):
        url = f"{URL_TMDB_BASE}movie/{tmdb_id}/videos?api_key={TMDB_API_KEY}&language={lang}"
        try:
            vids = json.loads(fetch(url, timeout=8)).get("results") or []
        except Exception:
            continue
        yt = [v for v in vids if v.get("site") == "YouTube" and v.get("key")]
        pick = (
            next((v for v in yt if v.get("type") == "Trailer" and v.get("official")), None)
            or next((v for v in yt if v.get("type") == "Trailer"), None)
            or next((v for v in yt if v.get("type") == "Teaser"), None)
        )
        if pick:
            return f"https://www.youtube.com/watch?v={pick['key']}"
    return None


def _strip_trailing_footnote(title: str) -> "str | None":
    """Retire un renvoi de note de bas de page final : « * » ou un chiffre ISOLÉ
    (un seul chiffre précédé d'une espace, ex. « Memento 1 » → « Memento »). Ne
    matche pas un vrai numéro collé (« 2049 ») ni multi-chiffres. Retourne None si
    rien n'est retiré. N'est utilisé qu'en dernier recours par _enrich_tmdb_first,
    donc les vrais numéros de suite (« Toy Story 5 ») ne sont jamais nettoyés tant
    que leur recherche directe aboutit."""
    src = (title or "").strip()
    cleaned = re.sub(r"\s+[\d*]\s*$", "", src).strip()
    return cleaned if cleaned and cleaned != src and len(cleaned) >= 2 else None


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
        # Essai 3 (aller-retour note de bas de page) : l'entrée PDF porte peut-être
        # un renvoi de note final (chiffre isolé / « * »), ex. « Memento 1 ». On
        # réessaie sans ; si ça matche, on adopte le titre propre de TMDB. Sûr pour
        # les vrais numéros (« Toy Story 5 ») dont la recherche directe a réussi.
        adopt_clean = False
        if not m:
            cleaned = _strip_trailing_footnote(titre) or (
                _strip_trailing_footnote(titre_fr) if titre_fr else None)
            if cleaned:
                m = _tmdb_search(cleaned, annee)
                adopt_clean = bool(m)
        if m:
            tmdb_id = m.get("id")
            _apply_tmdb_movie(film, m)
            if adopt_clean and m.get("title"):
                film["titre"] = m["title"]            # titre propre TMDB, sans la note
                if m.get("original_title"):
                    film["titreOriginal"] = m["original_title"]
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

    # E. Bande-annonce (vidéos TMDB : trailer YouTube officiel FR en priorité, puis EN)
    if tmdb_id and not film.get("trailer"):
        trailer = _tmdb_trailer(tmdb_id)
        if trailer:
            film["trailer"] = trailer


def _apply_tmdb_movie(film: dict, m: dict) -> None:
    """Applique les champs d'un objet movie TMDB sur le film (sans écraser l'existant)."""
    if not film.get("poster") and m.get("poster_path"):
        film["poster"] = f"https://image.tmdb.org/t/p/w500{m['poster_path']}"
    if not film.get("backdrop") and m.get("backdrop_path"):
        film["backdrop"] = f"https://image.tmdb.org/t/p/w1280{m['backdrop_path']}"
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

# Champs régénérés par la source à chaque fetch (donc différents à chaque run
# sans que le programme ait changé) : à ignorer pour décider s'il faut réécrire
# le fichier fallback programme.json. `resa_url` (lien de réservation cotecine
# Lumière) embarque un token horaire qui tourne à chaque scrape.
VOLATILE_SEANCE_FIELDS = {"resa_url"}


def _films_sans_volatiles(films: list[dict]) -> list[dict]:
    """Copie des films dont les séances sont dépouillées des champs volatils,
    pour comparer deux versions du programme sur le seul contenu signifiant."""
    stable: list[dict] = []
    for f in films:
        g = dict(f)
        seances = g.get("seances")
        if isinstance(seances, list):
            g["seances"] = [
                {k: v for k, v in s.items() if k not in VOLATILE_SEANCE_FIELDS}
                for s in seances
            ]
        stable.append(g)
    return stable


def main():
    parser = argparse.ArgumentParser(description="Scraper programme multi-cinémas Lyon")
    parser.add_argument("--debug",      action="store_true", help="Mode debug verbose")
    parser.add_argument("--dry-run",    action="store_true", help="Ne pas écrire le fichier JSON")
    parser.add_argument("--output",     default=str(OUTPUT_DEFAULT), help="Chemin du fichier JSON de sortie")
    parser.add_argument("--no-omdb",    action="store_true", help="Désactiver l'enrichissement OMDb/TMDB")
    parser.add_argument("--no-filter",  action="store_true", help="Ne pas filtrer par semaine (pour test)")
    parser.add_argument("--no-lumiere", action="store_true", help="Désactiver le scraping des Cinémas Lumière")
    parser.add_argument("--no-zola",    action="store_true", help="Désactiver le scraping du Zola")
    parser.add_argument("--no-events",  action="store_true", help="Désactiver le scraping des événements")
    parser.add_argument("--events-only", action="store_true",
                        help="Ne scraper que les événements (films lus depuis programme.json)")
    parser.add_argument("--force-resume", action="store_true",
                        help="Régénérer le résumé du mois même s'il est récent")
    parser.add_argument("--no-comoedia-pdf", action="store_true",
                        help="Désactiver le scraper PDF Comoedia")
    parser.add_argument("--pdf-file",   default=None,
                        help="Fichier PDF local Comoedia (pour test, remplace le téléchargement)")
    parser.add_argument("--pdf-url",    default=None,
                        help="URL directe du PDF Comoedia (pour test)")
    parser.add_argument("--lumiere-week", default=None, metavar="YYYY-MM-DD",
                        help="Date du mercredi de la semaine à scraper pour Lumière (ex: 2026-03-25)")
    # Rétrocompatibilité : --file était l'ancien chemin HTML
    parser.add_argument("--file",       default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # --events-only : on ne re-scrape aucun film ; ceux du dernier programme.json
    # suffisent à la jointure qui date les événements (§4.3).
    if args.events_only:
        args.no_comoedia_pdf = args.no_lumiere = args.no_zola = True
        args.no_omdb = args.no_filter = True

    log.info("═" * 55)
    log.info(f"Multi-Cinémas Lyon Scraper — {datetime.now().strftime('%A %d %B %Y %H:%M')}")
    log.info("═" * 55)

    # 1. Scraping Comoedia (PDF uniquement — programme-accessible obsolète)
    comoedia_films: list[dict] = []
    if not args.no_comoedia_pdf:
        comoedia_films = scrape_comoedia_pdf(
            pdf_file=args.pdf_file,
            pdf_url_override=args.pdf_url,
            dry_run=args.dry_run,
        )
    # 0 film Comoedia ce run n'est PAS forcément une panne : le PDF hebdo n'est
    # publié qu'une fois par semaine alors que le scraper tourne un jour sur deux,
    # donc les runs suivants dédupliquent légitimement (« PDF déjà traité »). La
    # santé réelle du pipeline (et le garde-fou exit 4) est donc évaluée en FIN
    # de run — une fois Lumière scrapé et les données upsertées — voir plus bas.
    if not comoedia_films and not args.no_comoedia_pdf:
        log.info(
            "0 film Comoedia scrapé ce run (dédup d'un PDF déjà traité, ou PDF non "
            "encore publié) — vérification de santé différée en fin de run."
        )

    # 2. Scraping Cinémas Lumière
    lumiere_films: list[dict] = []
    if not args.no_lumiere:
        lumiere_week_override: date | None = None
        if args.lumiere_week:
            try:
                lumiere_week_override = date.fromisoformat(args.lumiere_week)
            except ValueError:
                log.error(f"--lumiere-week invalide : '{args.lumiere_week}' (attendu YYYY-MM-DD)")
                sys.exit(1)
        lumiere_films = scrape_lumiere(week_date=lumiere_week_override)

    # 2bis. Scraping Le Zola
    zola_films: list[dict] = []
    if not args.no_zola:
        zola_films = scrape_zola()

    # 3. Fusion des sources
    all_films = comoedia_films + lumiere_films + zola_films
    if args.events_only:
        try:
            all_films = json.loads(Path(args.output).read_text(encoding="utf-8")).get("films") or []
            log.info(f"--events-only : {len(all_films)} films relus depuis {args.output}")
        except Exception as e:
            log.warning(f"--events-only : programme.json illisible ({e}) — jointure sans séances")
            all_films = []
    elif not all_films:
        log.error("Aucun film extrait (ni Comoedia, ni Lumière, ni Zola).")
        sys.exit(2)
    log.info(
        f"{len(all_films)} films au total "
        f"(Comoedia:{len(comoedia_films)}, Lumière:{len(lumiere_films)}, "
        f"Zola:{len(zola_films)})"
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
            "imdbId", "poster", "backdrop", "trailer", "imdbRating", "cast", "synopsis",
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

    # 5. Upsert tous les films (Comoedia + Lumière) dans Supabase (avant filtrage)
    if not args.dry_run and not args.events_only:
        upsert_all_to_supabase(all_films)

    # 5bis. Événements — APRÈS l'upsert des films (la jointure des créneaux a
    # besoin des films/séances du run) et AVANT le filtrage semaine (qui
    # amputerait les séances servant à dater les événements longs).
    evenements: list = []
    if not args.no_events:
        try:
            evenements = scrape_events(all_films)
            if not args.dry_run:
                upsert_events_to_supabase(evenements, force_resume=args.force_resume)
        except Exception as e:
            # Les événements ne doivent jamais faire tomber le pipeline séances.
            log.error(f"Pipeline événements en échec ({e}) — programme séances inchangé")
            evenements = []

    # 6. Filtrage semaine
    if not args.no_filter:
        all_films = filter_current_week(all_films)
    log.info(f"{len(all_films)} films retenus pour la semaine")

    # 7. Écriture JSON — fallback consommé par le front UNIQUEMENT si Supabase
    #    est indisponible (source principale = Supabase, lu en direct).
    #    Écriture CONDITIONNELLE : on ne réécrit que si la liste des films change
    #    réellement. Sinon le champ `generated_at` bouge à chaque run et force un
    #    commit + un redéploiement Pages à chaque run pour rien.
    output = {
        "generated_at": datetime.now().isoformat(),
        "sources": [URL_PDF_LISTING, URL_LUMIERE_BASE, URL_ZOLA_AFFICHE],
        "films": all_films,
        # Repli des événements (contrat C4 étendu) : sans lui, l'onglet
        # Événements n'aurait AUCUNE source de secours si Supabase tombe (I3).
        # Les liens de réservation en sont exclus (I5 : token horaire volatil).
        "evenements": events_public(evenements),
    }

    if args.dry_run:
        print(json.dumps(output, ensure_ascii=False, indent=2))
    else:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        previous = None
        if out_path.exists():
            try:
                previous = json.loads(out_path.read_text(encoding="utf-8"))
            except Exception:
                previous = None
        # Comparaison sur le contenu SIGNIFIANT uniquement : on ignore les champs
        # volatils (resa_url = token horaire régénéré à chaque fetch), sinon la
        # détection conclurait « changé » à chaque run et le fichier serait
        # réécrit + committé inutilement. Le resa_url frais est tout de même écrit
        # quand une réécriture a lieu (vrai changement de programme).
        # --no-events (ou pipeline événements en échec) ne doit pas VIDER le
        # repli : on reconduit les événements du fichier précédent.
        if args.no_events and previous:
            output["evenements"] = previous.get("evenements") or []
        unchanged = (
            previous is not None
            and _films_sans_volatiles(previous.get("films") or []) == _films_sans_volatiles(all_films)
            and previous.get("sources") == output["sources"]
            and (previous.get("evenements") or []) == output["evenements"]
        )
        if unchanged:
            log.info(
                f"programme.json inchangé (hors horodatage) — pas de réécriture "
                f"({len(all_films)} films)"
            )
        else:
            out_path.write_text(
                json.dumps(output, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            log.info(f"✓ Écrit → {out_path} ({out_path.stat().st_size:,} octets)")

    # ── Garde-fou de santé Comoedia (ASYMÉTRIQUE) ──
    # But : détecter une VRAIE panne Comoedia (site changé, PDF introuvable/
    # illisible) sans jamais crier au loup quand tout va bien.
    #
    # Semaine de référence = mercredi de la semaine EN COURS (celle contenant
    # aujourd'hui), SANS le saut au mercredi suivant que fait get_last_wednesday()
    # le mardi : on n'exige jamais Comoedia pour une semaine encore à venir.
    #
    # Critère d'échec ASYMÉTRIQUE : on n'échoue QUE si Lumière a publié cette
    # semaine (preuve qu'elle est bien « en ligne ») ALORS QUE Comoedia y est
    # absent. Si Lumière non plus n'a rien, c'est que personne n'a encore publié
    # (non-événement) → pas d'échec. C'est ce qui rend la cadence rapprochée
    # sûre : aucun faux rouge avant publication. La présence est lue
    # dans Supabase APRÈS l'upsert (étape 5), donc les données de CE run comptent.
    #
    # Le Zola est EXCLU de la preuve « semaine publiée » : il publie ~15 jours
    # en avance (liste roulante), là où Comoedia et Lumière sont hebdomadaires.
    # Le compter ferait accuser Comoedia à tort dès que Zola seul a des séances.
    if not args.no_comoedia_pdf and not args.dry_run:
        _today = date.today()
        wk_start = _today - timedelta(days=(_today.isoweekday() - 3) % 7)
        wk_end = wk_start + timedelta(days=6)
        # NB : on lit via count_week_seances (silencieux) et non check_week_in_supabase,
        # dont le log « … — ignoré » n'a de sens que dans le chemin de déduplication.
        comoedia_live = bool(comoedia_films) or (count_week_seances(wk_start, wk_end, slug="comoedia") or 0) > 0
        lumiere_live = (count_week_seances(wk_start, wk_end, exclude_slugs=["comoedia", "le-zola"]) or 0) > 0

        if comoedia_live:
            log.info(f"Pipeline Comoedia sain — semaine {wk_start} présente.")
        elif not lumiere_live:
            log.warning(
                f"Ni Comoedia ni Lumière pour la semaine {wk_start} → {wk_end} "
                "— programme probablement pas encore publié. Pas d'échec."
            )
        else:
            log.error(
                f"Échec volontaire : Lumière a publié la semaine {wk_start} → "
                f"{wk_end}, mais Comoedia y est absent — pipeline Comoedia cassé, "
                "à corriger. Vérifier /horaires-semaine-complete/ "
                "(lien « programme de la semaine ») ou passer --pdf-url."
            )
            sys.exit(4)

    log.info("Terminé.")


if __name__ == "__main__":
    main()
