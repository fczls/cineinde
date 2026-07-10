#!/usr/bin/env python3
"""
merge_duplicate_films.py — Fusion one-shot des films dédoublés par imdb_id.

Contexte : avant l'introduction de la dédup par imdb_id (option B, cf.
exploration Obsidian « Dédup inter-sources »), la clé brute
(titre, annee, realisateur) créait des lignes `films` en double quand deux
sources (Comoedia + Lumière) décrivaient le même film avec une casse /
orthographe / année différente. Ce script fusionne les doublons DÉJÀ en base :

  1. regroupe les films par imdb_id (les lignes sans imdb_id sont ignorées —
     leur dédup reste régie par la clé brute) ;
  2. choisit une ligne canonique par groupe (la plus complète, puis la plus
     ancienne) ;
  3. réassigne les `seances` des doublons vers la ligne canonique — en
     supprimant les séances qui feraient doublon (même cinema/date/heure) ;
  4. supprime les lignes `films` surnuméraires.

Garde-fou : deux films partageant un imdb_id mais dont les années divergent de
plus de `--year-tol` (défaut 1) NE sont PAS fusionnés (mauvais match TMDB
possible) — ils sont signalés et laissés séparés.

À exécuter UNE FOIS, AVANT la migration 003 (l'index unique partiel sur
imdb_id échoue tant que des doublons subsistent).

Usage :
  python scripts/merge_duplicate_films.py            # dry-run (défaut)
  python scripts/merge_duplicate_films.py --apply    # exécute réellement
Requiert : SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY dans l'environnement.
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

PAGE = 1000  # plafond REST Supabase — on pagine au-delà

# Champs dont la présence traduit une ligne « riche » (pour élire la canonique).
COMPLETENESS_FIELDS = (
    "poster", "synopsis", "realisateur", "annee",
    "duree", "imdb_rating", "cast", "titre_original",
)


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


def years_close(a, b, tol: int) -> bool:
    """Garde-fou : n'autorise la fusion que si les années restent proches (une année manquante n'infirme rien)."""
    try:
        return abs(int(a) - int(b)) <= tol
    except (TypeError, ValueError):
        return True


def completeness(film: dict) -> int:
    """Nombre de champs significatifs renseignés (départage la ligne canonique)."""
    score = sum(1 for f in COMPLETENESS_FIELDS if film.get(f) not in (None, "", []))
    if film.get("genres"):
        score += 1
    return score


def main() -> int:
    parser = argparse.ArgumentParser(description="Fusion des films dédoublés par imdb_id")
    parser.add_argument("--apply", action="store_true",
                        help="Exécuter réellement (par défaut : dry-run)")
    parser.add_argument("--year-tol", type=int, default=1,
                        help="Écart d'année max pour fusionner (défaut 1)")
    args = parser.parse_args()
    dry = not args.apply
    tag = "[dry-run] " if dry else ""

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("Error: SUPABASE_URL et SUPABASE_SERVICE_ROLE_KEY requis.", file=sys.stderr)
        return 1
    client: Client = create_client(url, key)

    # ── 1. Charger tous les films et regrouper par imdb_id ─────────────────
    films = paginate(lambda: client.table("films").select("*"))
    by_imdb: dict[str, list[dict]] = {}
    for f in films:
        imdb = (f.get("imdb_id") or "").strip()
        if imdb:
            by_imdb.setdefault(imdb, []).append(f)

    groups = {imdb: rows for imdb, rows in by_imdb.items() if len(rows) > 1}
    print(f"{tag}{len(films)} films en base · {len(groups)} imdb_id en doublon.\n")
    if not groups:
        print("Aucun doublon à fusionner.")
        return 0

    total_merged = 0     # lignes films supprimées
    total_moved = 0      # séances réassignées
    total_dropped = 0    # séances supprimées (collision)

    for imdb, rows in sorted(groups.items()):
        # Canonique = la plus complète, puis la plus ancienne (created_at).
        rows_sorted = sorted(
            rows,
            key=lambda f: (-completeness(f), f.get("created_at") or ""),
        )
        canonical = rows_sorted[0]
        canon_id = canonical["id"]
        canon_year = canonical.get("annee")
        dups = rows_sorted[1:]

        print(f"• {imdb} — canonique : « {canonical.get('titre')} » "
              f"({canon_year}, {canonical.get('realisateur') or '—'})")

        # Séances déjà présentes sur la canonique (clés de collision).
        canon_keys = {
            (s["cinema_id"], s["date"], str(s["heure"]))
            for s in paginate(lambda: client.table("seances")
                              .select("cinema_id,date,heure").eq("film_id", canon_id))
        }

        for dup in dups:
            # Garde-fou année : divergence forte → on NE fusionne PAS.
            if not years_close(canon_year, dup.get("annee"), args.year_tol):
                print(f"    ⚠️  gardé séparé (année {dup.get('annee')} ≠ "
                      f"{canon_year}) : « {dup.get('titre')} » [{dup['id']}]")
                continue

            dup_seances = paginate(lambda: client.table("seances")
                                   .select("id,cinema_id,date,heure").eq("film_id", dup["id"]))
            moved = dropped = 0
            for s in dup_seances:
                k = (s["cinema_id"], s["date"], str(s["heure"]))
                if k in canon_keys:
                    # Collision : la canonique a déjà cette séance → supprimer la dup.
                    dropped += 1
                    if not dry:
                        client.table("seances").delete().eq("id", s["id"]).execute()
                else:
                    canon_keys.add(k)
                    moved += 1
                    if not dry:
                        client.table("seances").update(
                            {"film_id": canon_id}).eq("id", s["id"]).execute()

            print(f"    → fusion « {dup.get('titre')} » [{dup['id']}] : "
                  f"{moved} séance(s) déplacée(s), {dropped} en doublon supprimée(s)")
            total_moved += moved
            total_dropped += dropped
            total_merged += 1

            if not dry:
                # Séances restantes déjà déplacées/supprimées → suppression sûre.
                client.table("films").delete().eq("id", dup["id"]).execute()

    print(f"\n{tag}Bilan : {total_merged} lignes films fusionnées, "
          f"{total_moved} séances déplacées, {total_dropped} séances en doublon supprimées.")
    if dry:
        print("Relancer avec --apply pour exécuter.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
