#!/usr/bin/env python3
"""
One-time migration: programme.json → Supabase.
Idempotent: safe to re-run (upserts films, cinemas, seances).

Usage: python scripts/migrate_json_to_supabase.py [--json path/to/programme.json]
Requires: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY in env.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Add project root for imports
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Load .env from project root
from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from supabase import create_client, Client

# Cinema name → slug
CINEMA_SLUGS = {
    "Le Comoedia": "comoedia",
    "Lumière Terreaux": "lumiere-terreaux",
    "Lumière Bellecour": "lumiere-bellecour",
    "Lumière Fourmi": "lumiere-fourmi",
}


def get_slug(name: str) -> str:
    return CINEMA_SLUGS.get(name) or name.lower().replace(" ", "-").replace("è", "e")


def main() -> None:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required.", file=sys.stderr)
        sys.exit(1)

    json_path = Path(__file__).parent.parent / "programme.json"
    if "--json" in sys.argv:
        i = sys.argv.index("--json")
        if i + 1 < len(sys.argv):
            json_path = Path(sys.argv[i + 1])

    if not json_path.exists():
        print(f"Error: {json_path} not found.", file=sys.stderr)
        sys.exit(1)

    data = json.loads(json_path.read_text(encoding="utf-8"))
    films_raw = data.get("films", [])

    client: Client = create_client(url, key)

    # 1. Upsert cinemas, build name → id map
    cinema_ids: dict[str, str] = {}
    seen_cinemas: set[str] = set()
    for entry in films_raw:
        name = entry.get("cinema") or "Le Comoedia"
        if name not in seen_cinemas:
            seen_cinemas.add(name)
            slug = get_slug(name)
            r = client.table("cinemas").upsert(
                {"name": name, "slug": slug},
                on_conflict="name",
            ).execute()
            if r.data:
                cinema_ids[name] = r.data[0]["id"]
            else:
                # Fetch existing
                r2 = client.table("cinemas").select("id").eq("name", name).execute()
                if r2.data:
                    cinema_ids[name] = r2.data[0]["id"]

    # 2. Upsert films, build (titre, annee, realisateur) → id map
    film_ids: dict[tuple, str] = {}
    for entry in films_raw:
        titre = entry.get("titre") or ""
        annee = entry.get("annee")
        realisateur = entry.get("realisateur") or ""
        key = (titre, annee, realisateur)
        if key in film_ids:
            continue

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
            row,
            on_conflict="titre,annee,realisateur",
        ).execute()
        if r.data:
            film_ids[key] = r.data[0]["id"]
        else:
            r2 = client.table("films").select("id").eq("titre", titre).eq("annee", annee).eq("realisateur", realisateur).execute()
            if r2.data:
                film_ids[key] = r2.data[0]["id"]

    # 3. Upsert seances
    seances_inserted = 0
    for entry in films_raw:
        titre = entry.get("titre") or ""
        annee = entry.get("annee")
        realisateur = entry.get("realisateur") or ""
        cinema_name = entry.get("cinema") or "Le Comoedia"
        film_id = film_ids.get((titre, annee, realisateur))
        cinema_id = cinema_ids.get(cinema_name)
        if not film_id or not cinema_id:
            continue

        for s in entry.get("seances", []):
            date_val = s.get("date")
            heure = s.get("heure")
            if not date_val or not heure:
                continue
            # heure may be "20:50" → ensure TIME format
            if len(heure) == 5 and ":" in heure:
                heure = heure + ":00"  # "20:50" → "20:50:00" for TIME
            row = {
                "film_id": film_id,
                "cinema_id": cinema_id,
                "date": date_val,
                "heure": heure,
                "version": s.get("version"),
                "resa_url": s.get("resa_url"),
            }
            try:
                client.table("seances").upsert(
                    row,
                    on_conflict="film_id,cinema_id,date,heure",
                ).execute()
                seances_inserted += 1
            except Exception as e:
                print(f"Warning: seance insert failed: {e}", file=sys.stderr)

    print(f"Migrated: {len(cinema_ids)} cinemas, {len(film_ids)} films, {seances_inserted} seances")


if __name__ == "__main__":
    main()
