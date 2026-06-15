#!/usr/bin/env python3
"""
cleanup_old_seances.py — Nettoyage hebdomadaire de Supabase.

Supprime :
  1. les séances dont la date est antérieure à J−N (défaut 10 jours) ;
  2. les films devenus orphelins (aucune séance à J−N ou après).

Garder la table légère évite aussi le plafond de 1000 lignes de l'API REST
(les anciennes séances tronquaient l'affichage côté frontend).

Usage : python scripts/cleanup_old_seances.py [--days N] [--dry-run]
Requiert : SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY dans l'environnement.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from supabase import create_client, Client

DEFAULT_DAYS = 10
PAGE = 1000  # plafond REST Supabase — on pagine au-delà


def paginate(make_query) -> list[dict]:
    """Récupère toutes les lignes d'une requête en paginant (contourne le plafond 1000)."""
    out: list[dict] = []
    start = 0
    while True:
        rows = (make_query().range(start, start + PAGE - 1).execute().data) or []
        out.extend(rows)
        if len(rows) < PAGE:
            return out
        start += PAGE


def main() -> int:
    parser = argparse.ArgumentParser(description="Nettoyage des séances anciennes")
    parser.add_argument("--days", type=int,
                        default=int(os.getenv("CLEANUP_DAYS", DEFAULT_DAYS)),
                        help=f"Jours de conservation (défaut {DEFAULT_DAYS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Afficher ce qui serait supprimé sans rien supprimer")
    args = parser.parse_args()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("Error: SUPABASE_URL et SUPABASE_SERVICE_ROLE_KEY requis.", file=sys.stderr)
        return 1

    cutoff = (date.today() - timedelta(days=args.days)).isoformat()
    client: Client = create_client(url, key)
    tag = "[dry-run] " if args.dry_run else ""

    # ── 1. Séances antérieures au seuil ────────────────────────────────
    r = client.table("seances").select("id", count="exact").lt("date", cutoff).limit(1).execute()
    n_seances = r.count or 0
    print(f"{tag}Séances antérieures à {cutoff} (J−{args.days}) : {n_seances}")
    if n_seances and not args.dry_run:
        client.table("seances").delete().lt("date", cutoff).execute()
        print(f"✓ {n_seances} séances supprimées.")

    # ── 2. Films orphelins (aucune séance à J−N ou après) ──────────────
    # On définit l'orphelin par l'état post-nettoyage : ainsi le compte est
    # correct en dry-run comme en réel.
    referenced = {row["film_id"] for row in paginate(
        lambda: client.table("seances").select("film_id").gte("date", cutoff)
    )}
    all_film_ids = [row["id"] for row in paginate(
        lambda: client.table("films").select("id")
    )]
    orphans = [fid for fid in all_film_ids if fid not in referenced]
    print(f"{tag}Films orphelins (sans séance ≥ {cutoff}) : {len(orphans)}")
    if orphans and not args.dry_run:
        for i in range(0, len(orphans), 100):
            client.table("films").delete().in_("id", orphans[i:i + 100]).execute()
        print(f"✓ {len(orphans)} films orphelins supprimés.")

    print(f"{tag}Nettoyage terminé.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
