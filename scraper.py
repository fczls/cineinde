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
URL_OMDB_BASE    = "https://www.omdbapi.com/"
URL_TMDB_BASE    = "https://api.themoviedb.org/3/"
import os
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "822f09ad")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

OUTPUT_DEFAULT   = Path(__file__).parent / "programme.json"
PDF_STATE_PATH   = Path(__file__).parent / "pdf_state.json"

# Page stable listant les horaires (et lien PDF si présent) — programme-semaine renvoie 404
URL_PDF_LISTING   = "https://www.cinema-comoedia.com/horaires-semaine-complete/"
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

def _fetch_pdf_urls_via_playwright(skip: bool = False) -> list[str]:
    """
    Rend la page horaires-semaine-complete avec Playwright et extrait les liens PDF.
    - Intercepte les requêtes/réponses réseau pour capturer les URLs CDN (*.pdf).
    - Parse aussi le HTML rendu pour les liens href.
    Retourne [] si skip=True, Playwright indisponible ou si aucun PDF trouvé.
    """
    if skip:
        return []
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.debug("Playwright non installé — extraction PDF via navigateur ignorée")
        return []

    pdf_urls: list[str] = []

    def _collect_pdf(request_or_url: str) -> None:
        url = request_or_url if isinstance(request_or_url, str) else getattr(request_or_url, "url", "")
        if url and ".pdf" in url.lower() and url not in pdf_urls:
            pdf_urls.append(url)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()

                # Intercepter les requêtes pour capturer les URLs PDF (CDN webediamovies, etc.)
                page.on("request", lambda req: _collect_pdf(req.url))
                page.on("response", lambda resp: _collect_pdf(resp.url))

                page.goto(
                    URL_PDF_LISTING,
                    wait_until="domcontentloaded",
                    timeout=15000,
                )
                page.wait_for_timeout(5000)  # Laisser le JS charger (liens, API, etc.)
                html = page.content()

                # Liens href contenant .pdf
                for m in re.findall(r'href=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.I):
                    url = m if m.startswith("http") else f"{URL_COMOEDIA_BASE}{m}"
                    if url not in pdf_urls:
                        pdf_urls.append(url)

                # Fallback : toute URL .pdf dans le HTML
                if not pdf_urls:
                    for m in re.findall(r'https?://[^\s"\'<>]+\.pdf[^\s"\'<>]*', html):
                        if m not in pdf_urls:
                            pdf_urls.append(m)

                if pdf_urls:
                    log.info(f"PDFs extraits via Playwright : {len(pdf_urls)}")
            finally:
                browser.close()
    except Exception as e:
        log.warning(f"Playwright : {e}")
    return pdf_urls


def fetch_pdf_urls(use_playwright: bool = True) -> list[str]:
    """
    Récupère les URLs de PDFs automatiquement.
    1. Variable d'environnement PDF_URL (prioritaire, ex. URL CDN webediamovies).
    2. Fetch statique (rapide, souvent vide car page en JS).
    3. Playwright si dispo (rend la page, intercepte requêtes, extrait liens PDF).
    4. Fallback : prédiction d'URLs (semaine courante + 2 précédentes).
    """
    # 1. Env PDF_URL (priorité, pour URL CDN manuelle)
    env_url = os.getenv("PDF_URL", "").strip()
    if env_url and ".pdf" in env_url.lower():
        log.info(f"PDF depuis PDF_URL : {env_url[:60]}...")
        return [env_url]

    # 2. Fetch statique (HTML brut)
    try:
        html = fetch(URL_PDF_LISTING)
        matches = re.findall(r'href=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.I)
        seen: set[str] = set()
        urls: list[str] = []
        for m in matches:
            url = m if m.startswith("http") else f"{URL_COMOEDIA_BASE}{m}"
            if url not in seen:
                seen.add(url)
                urls.append(url)
        if urls:
            log.info(f"PDFs trouvés (fetch statique) : {len(urls)}")
            return urls
    except Exception as e:
        log.debug(f"Fetch statique : {e}")

    # 3. Playwright (page rendue en JS + interception requêtes)
    urls = _fetch_pdf_urls_via_playwright(skip=not use_playwright)
    if urls:
        return urls

    # 4. Fallback prédiction (URLs cinema-comoedia.com/pdf/... souvent 404)
    log.warning("Aucun lien PDF détecté — fallback prédiction d'URLs")
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

def check_week_in_supabase(week_start: date, week_end: date) -> bool:
    """
    Vérifie si la semaine contient déjà des séances Comoedia dans Supabase.
    Retourne False si les credentials sont absents ou en cas d'erreur.
    """
    sb_url = os.getenv("SUPABASE_URL")
    sb_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not sb_url or not sb_key:
        return False
    try:
        from supabase import create_client
        client = create_client(sb_url, sb_key)
        r = client.table("cinemas").select("id").eq("slug", "comoedia").execute()
        if not r.data:
            return False
        cinema_id = r.data[0]["id"]
        r2 = (
            client.table("seances")
            .select("id", count="exact")
            .eq("cinema_id", cinema_id)
            .gte("date", week_start.isoformat())
            .lte("date", week_end.isoformat())
            .execute()
        )
        count = r2.count or 0
        if count > 0:
            log.info(
                f"Semaine {week_start} déjà dans Supabase "
                f"({count} séances Comoedia) — ignoré"
            )
            return True
    except Exception as e:
        log.warning(f"Vérification Supabase échouée : {e} — on continue quand même")
    return False


# ── Téléchargement du PDF ──────────────────────

def download_pdf(url: str) -> bytes | None:
    """Télécharge le PDF et retourne ses octets (urllib, pas de dépendance requests)."""
    try:
        req = Request(url, headers=HEADERS)
        with urlopen(req, timeout=30) as r:
            data = r.read()
        log.info(f"PDF téléchargé ({len(data):,} octets) : {url}")
        return data
    except HTTPError as e:
        log.error(f"Impossible de télécharger le PDF {url} : HTTP {e.code}")
        return None
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
        if len(pdf.pages) < 2:
            log.error("Le PDF n'a que %d page(s) — 2 attendues", len(pdf.pages))
            return [], None

        # Chercher la date "Du X au Y mois YYYY" dans la page 1
        p0_text = pdf.pages[0].extract_text() or ""
        m = re.search(r"[Dd]u\s+(\d+)\s+au\s+\d+\s+(\w+)\s+(\d{4})", p0_text)
        if m:
            mois_n = MOIS_FR.get(m.group(2).lower())
            if mois_n:
                try:
                    week_start = date(int(m.group(3)), mois_n, int(m.group(1)))
                    log.info(f"Début de semaine lu en page 1 : {week_start}")
                except ValueError:
                    pass

        # Extraire le tableau de la page 2
        p1 = pdf.pages[1]
        raw_table = p1.extract_table()
        if raw_table:
            table = raw_table
            log.info(f"Tableau extrait (page 2) : {len(table)} lignes")
        else:
            log.warning(
                "extract_table() vide en page 2 — tentative via extract_text()"
            )
            table = _pdf_text_to_rows(p1.extract_text() or "")

    return table, week_start


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

        # Normaliser le titre : retirer les suffixes de catégorie
        titre = re.sub(r"\s+JP\s*$", "", titre, flags=re.I).strip()
        titre = re.sub(r"\s+", " ", titre).strip()
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
}


def _cinema_slug(name: str) -> str:
    return CINEMA_SLUGS.get(name) or name.lower().replace(" ", "-").replace("è", "e")


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
    film_ids: "dict[tuple, str]" = {}
    seances_count = 0

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
        key = (titre, annee, realisateur)

        if key not in film_ids:
            row = {
                "titre": titre,
                "titre_original": entry.get("titreOriginal"),
                "annee": annee,
                "realisateur": realisateur,
                "duree": entry.get("duree"),
                "genres": entry.get("genres") or [],
                "synopsis": entry.get("synopsis"),
                "imdb_id": entry.get("imdbId"),
                "poster": entry.get("poster"),
                "imdb_rating": entry.get("imdbRating"),
                "cast": entry.get("cast"),
                "source": entry.get("source"),
            }
            r = client.table("films").upsert(
                row, on_conflict="titre,annee,realisateur"
            ).execute()
            if r.data:
                film_ids[key] = r.data[0]["id"]
            else:
                r2 = (
                    client.table("films").select("id")
                    .eq("titre", titre)
                    .eq("annee", annee)
                    .eq("realisateur", realisateur)
                    .execute()
                )
                if r2.data:
                    film_ids[key] = r2.data[0]["id"]

        film_id = film_ids.get(key)
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
        f"Supabase : {len(film_ids)} films, {seances_count} séances upsertés "
        f"({len(cinema_ids)} cinémas)"
    )


# ── Orchestrateur PDF principal ────────────────

def scrape_comoedia_pdf(
    pdf_file: "str | None" = None,
    pdf_url_override: "str | None" = None,
    dry_run: bool = False,
    use_playwright: bool = True,
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

    urls_to_check = (
        [pdf_url_override]
        if pdf_url_override
        else fetch_pdf_urls(use_playwright=use_playwright)
    )
    all_films: list[dict] = []

    for url in urls_to_check:
        # ── Garde 1 : déjà traité ? ────────────
        if url in processed:
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
    parser.add_argument("--debug",      action="store_true", help="Mode debug verbose")
    parser.add_argument("--dry-run",    action="store_true", help="Ne pas écrire le fichier JSON")
    parser.add_argument("--output",     default=str(OUTPUT_DEFAULT), help="Chemin du fichier JSON de sortie")
    parser.add_argument("--no-omdb",    action="store_true", help="Désactiver l'enrichissement OMDb/TMDB")
    parser.add_argument("--no-filter",  action="store_true", help="Ne pas filtrer par semaine (pour test)")
    parser.add_argument("--no-lumiere", action="store_true", help="Désactiver le scraping des Cinémas Lumière")
    parser.add_argument("--no-comoedia-pdf", action="store_true",
                        help="Désactiver le scraper PDF Comoedia")
    parser.add_argument("--pdf-file",   default=None,
                        help="Fichier PDF local Comoedia (pour test, remplace le téléchargement)")
    parser.add_argument("--pdf-url",    default=None,
                        help="URL directe du PDF Comoedia (pour test)")
    parser.add_argument("--no-playwright", action="store_true",
                        help="Ne pas utiliser Playwright pour extraire le lien PDF")
    # Rétrocompatibilité : --file était l'ancien chemin HTML
    parser.add_argument("--file",       default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

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
            use_playwright=not args.no_playwright,
        )
    if not comoedia_films:
        log.warning(
            "Aucun film Comoedia extrait — le PDF est la seule source valide. "
            "Utilisez --pdf-url ou --pdf-file avec l'URL CDN du programme."
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

    # 5. Upsert tous les films (Comoedia + Lumière) dans Supabase (avant filtrage)
    if not args.dry_run:
        upsert_all_to_supabase(all_films)

    # 6. Filtrage semaine
    if not args.no_filter:
        all_films = filter_current_week(all_films)
    log.info(f"{len(all_films)} films retenus pour la semaine")

    # 7. Écriture JSON
    output = {
        "generated_at": datetime.now().isoformat(),
        "sources": [URL_PDF_LISTING, URL_LUMIERE_BASE],
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
