#!/usr/bin/env python3
"""
fix_titles_case.py — Corrige la casse des titres déjà stockés dans Supabase.

Les titres extraits du PDF Comoedia étaient enregistrés en CAPITALES. Ce script
réapplique la même normalisation que le scraper (_titlecase_fr) aux titres déjà
en base, pour corriger l'historique. Les titres déjà bien casés (ex. Lumière /
TMDB) ne sont pas modifiés (_titlecase_fr n'agit que sur les titres majoritaire-
ment en capitales).

À lancer une seule fois après le déploiement du correctif de casse.

Usage : python scripts/fix_titles_case.py [--dry-run]
Requiert : SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY dans l'environnement (.env).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from supabase import create_client, Client

# Réutilise EXACTEMENT la logique de casse du scraper (source unique de vérité).
from scraper import _titlecase_fr

PAGE = 1000  # plafond REST Supabase — on pagine au-delà


def paginate(make_query) -> list[dict]:
    out: list[dict] = []
    start = 0
    while True:
        rows = (make_query().range(start, start + PAGE - 1).execute().data) or []
        out.extend(rows)
        if len(rows) < PAGE:
            return out
        start += PAGE


def main() -> int:
    parser = argparse.ArgumentParser(description="Corrige la casse des titres en base")
    parser.add_argument("--dry-run", action="store_true",
                        help="Afficher les changements sans rien écrire")
    args = parser.parse_args()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("Error: SUPABASE_URL et SUPABASE_SERVICE_ROLE_KEY requis.", file=sys.stderr)
        return 1

    client: Client = create_client(url, key)
    tag = "[dry-run] " if args.dry_run else ""

    films = paginate(lambda: client.table("films").select("id,titre,source"))
    print(f"{len(films)} films en base.")

    changed = 0
    errors = 0
    for f in films:
        old = f.get("titre") or ""
        new = _titlecase_fr(old)
        if new == old:
            continue
        changed += 1
        print(f"{tag}[{f.get('source') or '?':8}] {old!r} → {new!r}")
        if not args.dry_run:
            try:
                client.table("films").update({"titre": new}).eq("id", f["id"]).execute()
            except Exception as e:
                errors += 1
                print(f"  ⚠ échec mise à jour (id={f['id']}) : {e}", file=sys.stderr)

    print(f"{tag}{changed} titre(s) à corriger"
          + (f", {errors} erreur(s)." if errors else "."))
    if args.dry_run and changed:
        print("→ Relance sans --dry-run pour appliquer.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
