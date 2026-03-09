#!/usr/bin/env python3
"""
Test Supabase migration: verify data integrity and API access.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from supabase import create_client


def main() -> int:
    url = os.getenv("SUPABASE_URL")
    anon_key = os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not anon_key:
        print("Error: SUPABASE_URL and SUPABASE_ANON_KEY (or SERVICE_ROLE) required.")
        return 1

    client = create_client(url, anon_key)
    errors = []

    # 1. Cinemas
    r = client.table("cinemas").select("*").execute()
    cinemas = r.data or []
    if len(cinemas) < 4:
        errors.append(f"Expected ≥4 cinemas, got {len(cinemas)}")
    else:
        print(f"✓ Cinemas: {len(cinemas)} ({', '.join(c['name'] for c in cinemas)})")

    # 2. Films
    r = client.table("films").select("id,titre,annee,realisateur").execute()
    films = r.data or []
    if len(films) < 10:
        errors.append(f"Expected ≥10 films, got {len(films)}")
    else:
        print(f"✓ Films: {len(films)}")

    # 3. Seances with film + cinema join
    r = client.table("seances").select("*,films(titre),cinemas(name)").limit(5).execute()
    seances = r.data or []
    if not seances:
        errors.append("No seances found")
    else:
        s = seances[0]
        if "films" in s and "cinemas" in s:
            print(f"✓ Seances: join works (sample: {s['films']['titre']} @ {s['cinemas']['name']})")
        else:
            errors.append("Seances join missing films/cinemas")

    # 4. Data shape for frontend: film+cinema+seances per entry
    r = client.table("seances").select("*,films(*),cinemas(name)").execute()
    all_seances = r.data or []
    # Group by (film_id, cinema_id)
    groups = {}
    for s in all_seances:
        fid = s["film_id"]
        cid = s["cinema_id"]
        key = (fid, cid)
        if key not in groups:
            groups[key] = {"film": s["films"], "cinema": s["cinemas"]["name"], "seances": []}
        groups[key]["seances"].append({
            "date": s["date"],
            "heure": str(s["heure"])[:5] if s.get("heure") else None,
            "version": s.get("version"),
        })
    if len(groups) < 10:
        errors.append(f"Expected ≥10 film+cinema groups, got {len(groups)}")
    else:
        print(f"✓ Film-cinema groups: {len(groups)} (ready for frontend transform)")

    # 5. RLS: anon key can read (if using anon)
    if os.getenv("SUPABASE_ANON_KEY"):
        r = client.table("films").select("id", count="exact").limit(1).execute()
        if r.data is not None:
            print("✓ RLS: anon key can read films")
        else:
            errors.append("RLS: anon key cannot read films")

    if errors:
        print("\nFAILED:", "\n  - ".join(errors))
        return 1
    print("\nAll tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
