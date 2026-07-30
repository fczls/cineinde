#!/usr/bin/env python3
"""
Apply a migration to Supabase via direct PostgreSQL connection.
Requires: DATABASE_URL in .env (from Supabase Dashboard → Settings → Database → Connection string)

Usage:
    python3 scripts/apply_schema.py                     # 001_initial.sql (défaut historique)
    python3 scripts/apply_schema.py 005_evenements.sql  # une migration nommée
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

def main() -> None:
    url = os.getenv("DATABASE_URL")
    if not url:
        print(
            "Error: DATABASE_URL required.\n"
            "Get it from: Supabase Dashboard → Settings → Database → Connection string (URI)\n"
            "Format: postgresql://postgres.[ref]:[PASSWORD]@db.[ref].supabase.co:5432/postgres",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        import psycopg2
    except ImportError:
        print("Error: pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    name = sys.argv[1] if len(sys.argv) > 1 else "001_initial.sql"
    sql_path = project_root / "supabase" / "migrations" / name
    if not sql_path.exists():
        available = sorted(p.name for p in sql_path.parent.glob("*.sql"))
        print(f"Error: migration '{name}' introuvable.\nDisponibles : {', '.join(available)}",
              file=sys.stderr)
        sys.exit(1)
    sql = sql_path.read_text(encoding="utf-8")

    conn = psycopg2.connect(url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        print(f"Migration {name} appliquée.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
