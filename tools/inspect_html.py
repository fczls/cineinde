#!/usr/bin/env python3
"""
inspect_html.py — Outil d'inspection de la structure HTML de Comedia.
À utiliser UNE FOIS pour comprendre la structure réelle de la page,
puis mettre à jour scraper.py en conséquence.

Usage : python inspect_html.py [--url URL] [--file fichier.html]
"""
import re
import sys
import argparse
from pathlib import Path
from urllib.request import urlopen, Request

URL_DEFAUT = "https://www.cinema-comedia.fr/programme-accessible/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ComediaBot/1.0)",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

# ─ Classes / IDs intéressants à détecter ─
KEYWORDS = [
    "film", "movie", "seance", "horaire", "programme", "titre",
    "synopsis", "version", "vf", "vo", "vostfr", "realis",
    "genre", "duree", "semaine", "schedule",
]


def fetch(url):
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=15) as r:
        cs = r.headers.get_content_charset() or "utf-8"
        return r.read().decode(cs, errors="replace")


def inspect(html: str):
    print(f"\n{'═'*60}")
    print(f"  Taille HTML : {len(html):,} caractères")
    print(f"{'═'*60}\n")

    # 1. Toutes les classes contenant un mot-clé
    classes_found = {}
    for m in re.finditer(r'class=["\']([^"\']+)["\']', html, re.I):
        for cls in m.group(1).split():
            cls_l = cls.lower()
            for kw in KEYWORDS:
                if kw in cls_l:
                    classes_found[cls] = classes_found.get(cls, 0) + 1

    print("── Classes HTML contenant des mots-clés cinéma ──")
    for cls, count in sorted(classes_found.items(), key=lambda x: -x[1]):
        print(f"  .{cls:40s} × {count}")

    # 2. IDs intéressants
    ids_found = {}
    for m in re.finditer(r'id=["\']([^"\']+)["\']', html, re.I):
        for kw in KEYWORDS:
            if kw in m.group(1).lower():
                ids_found[m.group(1)] = ids_found.get(m.group(1), 0) + 1

    if ids_found:
        print("\n── IDs HTML contenant des mots-clés cinéma ──")
        for id_, count in sorted(ids_found.items(), key=lambda x: -x[1]):
            print(f"  #{id_:40s} × {count}")

    # 3. Extrait les 3 premiers blocs contenant "film" ou "seance"
    print("\n── Extraits HTML (premiers blocs 'film' / 'seance') ──")
    for pattern in [
        r'<(?:article|div|section)[^>]*class=["\'][^"\']*film[^"\']*["\'][^>]*>',
        r'<(?:article|div|section)[^>]*class=["\'][^"\']*seance[^"\']*["\'][^>]*>',
        r'<(?:li)[^>]*class=["\'][^"\']*(?:seance|horaire)[^"\']*["\'][^>]*>',
    ]:
        matches = list(re.finditer(pattern, html, re.I))
        if matches:
            print(f"\n  Pattern : {pattern[:60]}…")
            print(f"  → {len(matches)} occurrence(s)")
            # Affiche le premier bloc (jusqu'à 600 chars)
            start = matches[0].start()
            print(f"  Extrait :\n{html[start:start+600]}\n  …")

    # 4. Recherche des horaires (patterns typiques)
    print("\n── Patterns d'horaires trouvés ──")
    horaire_re = re.compile(r"\b\d{1,2}[hH:]\d{2}\b")
    context_size = 80
    printed = 0
    for m in horaire_re.finditer(html):
        if printed >= 5:
            break
        s, e = m.start(), m.end()
        ctx = html[max(0, s-context_size):e+context_size]
        # Retire les balises pour lisibilité
        ctx_clean = re.sub(r"<[^>]+>", " ", ctx).strip()
        ctx_clean = re.sub(r"\s+", " ", ctx_clean)
        print(f"  ↳ «{ctx_clean[:180]}»")
        printed += 1

    # 5. Résumé de la structure
    print("\n── Structure globale (tags de niveau 1-2 en body) ──")
    depth = 0
    counts: dict[str, int] = {}
    for m in re.finditer(r"<(/?)(\w+)[^>]*>", html):
        closing, tag = m.group(1), m.group(2).lower()
        if tag in ("html", "head", "body", "script", "style", "link", "meta"):
            continue
        if not closing:
            key = f"  {'  '*min(depth,4)}<{tag}>"
            counts[key] = counts.get(key, 0) + 1
            depth = min(depth + 1, 8)
        else:
            depth = max(depth - 1, 0)

    for k, v in list(counts.items())[:30]:
        print(f"{k:50s} × {v}")

    print(f"\n{'═'*60}")
    print("  → Modifiez scraper.py selon cette structure.")
    print(f"{'═'*60}\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url",  default=URL_DEFAUT)
    ap.add_argument("--file", default=None, help="Fichier HTML local (évite le fetch)")
    args = ap.parse_args()

    if args.file:
        html = Path(args.file).read_text(encoding="utf-8", errors="replace")
    else:
        print(f"Téléchargement de {args.url} …")
        try:
            html = fetch(args.url)
        except Exception as e:
            print(f"Erreur : {e}")
            sys.exit(1)

    inspect(html)


if __name__ == "__main__":
    main()
