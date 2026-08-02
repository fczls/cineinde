#!/usr/bin/env python3
"""
rapatrie_affiches.py — Rattrapage ponctuel des affiches déjà en base.

Recopie dans le bucket `affiches` les visuels encore servis depuis un CDN de
salle, et réécrit les URLs dans Supabase :

  • films.poster
  • evenements.affiche_url
  • evenement_films.affiche_url

POURQUOI. `cinemas-lumiere.com` ne sert AUCUN en-tête
`Access-Control-Allow-Origin`. Une image cross-origin dépourvue de cet en-tête
ne peut pas devenir une texture WebGL : l'apparition « tissu » de l'onglet
Évènements est donc silencieusement désactivée sur toutes les cartes qui en
dépendent. Accessoirement, ces CDN renvoient des 403 en hotlink et leurs URLs
tournent.

`scraper.py` fait désormais ce rapatriement à chaque run (voir
`rapatrie_affiches_films`). Ce script sert au RATTRAPAGE de l'existant, sans
attendre — et sans re-scraper quoi que ce soit, ce qui serait long et toucherait
bien plus de données.

Sûreté :
  • lecture seule par défaut — il faut `--apply` pour écrire ;
  • idempotent : le nom de fichier est le hash de l'URL source, ce qui est déjà
    dans le bucket est sauté, et une URL déjà sur Supabase est ignorée ;
  • sans perte : une copie qui échoue laisse l'URL d'origine intacte.

Usage :
    python3 scripts/rapatrie_affiches.py              # simulation
    python3 scripts/rapatrie_affiches.py --apply      # écrit
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

from supabase import create_client

# On réutilise les helpers du scraper : même nommage de fichier, même politique
# de copie. Les dupliquer ferait diverger les deux chemins au premier ajustement.
from scraper import _affiche_path, _rapatrie_affiche, _liste_affiches  # noqa: E402

PAGE = 1000


def paginate(make_query) -> list[dict]:
    out: list[dict] = []
    start = 0
    while True:
        rows = (make_query().range(start, start + PAGE - 1).execute().data) or []
        out.extend(rows)
        if len(rows) < PAGE:
            return out
        start += PAGE


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="écrit réellement (sans ce drapeau : simulation)")
    args = ap.parse_args()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        sys.exit("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY absents — rien à faire.")
    client = create_client(url, key)

    prefixe = f"{url.rstrip('/')}/storage/v1/object/public/affiches/"
    existants = _liste_affiches(client)
    print(f"Bucket `affiches` : {len(existants)} fichiers déjà présents.")

    # (table, colonne d'URL) — les trois chemins par lesquels une affiche
    # atteint le front (cf. evAffiche()).
    cibles = [
        ("films", "poster"),
        ("evenements", "affiche_url"),
        ("evenement_films", "affiche_url"),
    ]

    cache: dict = {}          # URL source → URL Supabase (ou None si échec)
    total_a_traiter = total_ecrit = total_echec = 0

    for table, colonne in cibles:
        try:
            lignes = paginate(lambda: client.table(table).select(f"id,{colonne}"))
        except Exception as e:
            print(f"  ⚠ {table} illisible ({e}) — ignorée")
            continue

        a_traiter = [r for r in lignes
                     if (r.get(colonne) or "").startswith("http")
                     and not (r.get(colonne) or "").startswith(prefixe)]
        total_a_traiter += len(a_traiter)
        print(f"\n{table}.{colonne} : {len(lignes)} lignes, "
              f"{len(a_traiter)} encore à la source.")
        if not a_traiter:
            continue

        # Aperçu par origine — c'est là qu'on voit qui refuse le CORS.
        from collections import Counter
        for hote, n in Counter(r[colonne].split("/")[2] for r in a_traiter).most_common():
            print(f"    {n:4d}  {hote}")

        if not args.apply:
            continue

        for r in a_traiter:
            src = r[colonne]
            if src not in cache:
                cache[src] = _rapatrie_affiche(client, src, existants)
            cible = cache[src]
            if not cible:
                total_echec += 1
                continue
            try:
                client.table(table).update({colonne: cible}).eq("id", r["id"]).execute()
                total_ecrit += 1
            except Exception as e:
                print(f"    ⚠ {table}#{r['id']} non mis à jour : {e}")
                total_echec += 1

    print()
    if args.apply:
        print(f"✓ {total_ecrit} URLs réécrites, {total_echec} laissées à la source "
              f"({len(cache)} visuels distincts traités).")
    else:
        print(f"Simulation : {total_a_traiter} lignes seraient traitées. "
              f"Relancer avec --apply pour écrire.")


if __name__ == "__main__":
    main()
