#!/usr/bin/env python3
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
URL_PROGRAMME    = "https://www.cinema-comedia.fr/programme-accessible/"
URL_OMDB_BASE    = "https://www.omdbapi.com/"
OMDB_API_KEY     = "VOTRE_CLE_OMDB"   # https://www.omdbapi.com/apikey.aspx

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
# HTML PARSER — structure de cinema-comedia.fr
# ─────────────────────────────────────────────
# La page /programme-accessible/ de Comedia utilise une structure
# WordPress avec le plugin "Ciné-Média" ou similaire.
# Structure typique observée :
#
#   <div class="film-semaine"> ou <article class="film">
#     <h2 class="film-titre"> ou <h3>Titre du Film</h3>
#     <p class="film-infos">Réalisateur · Pays · Année · durée</p>
#     <p class="film-version">VF / VOSTFR</p>
#     <div class="seances"> ou <ul class="horaires">
#       <li class="seance">
#         <span class="jour">Mercredi 12 mars</span>
#         <span class="heure">14h30</span>
#       </li>
#       ...
#     </div>
#   </div>
#
# Le parser est adaptatif : il tente plusieurs sélecteurs CSS courants
# et loggue ce qu'il trouve pour faciliter l'ajustement.
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


def parse_programme(html: str) -> list[dict]:
    """
    Parse le HTML de /programme-accessible/ et retourne une liste de films.
    Structure de retour :
      {
        "titre": str,
        "titreOriginal": str | None,
        "annee": int | None,
        "realisateur": str | None,
        "duree": int | None,          # en minutes
        "genres": [str],
        "synopsis": str | None,
        "imdbId": str | None,         # rempli plus tard par OMDb
        "seances": [
          {"date": "YYYY-MM-DD", "heure": "HH:MM", "version": "VF|VOSTFR|VO"}
        ]
      }
    """
    root = parse_html(html)
    films = []

    # ── Stratégie 1 : articles ou divs avec class contenant "film" ──
    film_nodes = (
        find_nodes(root, tag="article", cls="film")
        or find_nodes(root, tag="div", cls="film-semaine")
        or find_nodes(root, tag="div", cls="film")
        or find_nodes(root, tag="div", cls="movie")
        or find_nodes(root, tag="section", cls="film")
    )

    log.info(f"Stratégie 1 : {len(film_nodes)} nœuds 'film' trouvés")

    # ── Stratégie 2 (fallback) : cherche les titres h2/h3 et remonte ──
    if not film_nodes:
        log.warning("Stratégie 1 échouée → stratégie h2/h3")
        for h in find_nodes(root, tag="h2") + find_nodes(root, tag="h3"):
            txt = text_of(h).strip()
            if txt and len(txt) > 2:
                # Crée un pseudo-nœud avec le parent immédiat
                film_nodes.append(h)

    for node in film_nodes:
        film = _extract_film(node)
        if film and film.get("titre"):
            films.append(film)

    log.info(f"{len(films)} films extraits")
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
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("═" * 55)
    log.info(f"Comedia Scraper — {datetime.now().strftime('%A %d %B %Y %H:%M')}")
    log.info("═" * 55)

    # 1. Téléchargement
    log.info(f"Fetch → {URL_PROGRAMME}")
    try:
        html = fetch(URL_PROGRAMME)
        log.info(f"HTML reçu : {len(html):,} caractères")
    except RuntimeError as e:
        log.error(f"Impossible de télécharger la page : {e}")
        sys.exit(1)

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
