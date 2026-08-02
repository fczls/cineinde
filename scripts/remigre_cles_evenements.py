#!/usr/bin/env python3
"""
remigre_cles_evenements.py — Migration ponctuelle des clés d'évènements.

`event_dedup_key()` s'ancrait sur le mois de DÉBUT. Or `filter_events_current`
rogne le début à aujourd'hui : un évènement long qui franchit un 1er du mois
voyait sa clé changer, et l'upsert (`on_conflict="cle"`) créait une 2e ligne à
côté de la première — mécaniquement, tous les mois. L'ancre est passée au mois
de FIN, qui ne bouge pas avec le calendrier.

Ce script réaligne l'existant. Sans lui, la nouvelle clé ne s'appliquerait qu'aux
lignes réécrites au prochain run, et toutes les anciennes resteraient en base
avec leur clé périmée — le remède serait pire que le mal.

Ce qu'il fait, par groupe de lignes qui retombent sur la MÊME nouvelle clé :
  1. élit une survivante — la plus riche (films + créneaux), puis la plus ancienne ;
  2. y déplace les films et créneaux que les autres détiennent seules, en
     respectant les contraintes d'unicité des tables enfants ;
  3. élargit la période de la survivante et comble ses champs vides ;
  4. supprime les perdantes (les enfants restants tombent en CASCADE) ;
  5. réécrit `cle` sur la survivante.

Sûreté :
  • lecture seule par défaut — il faut `--apply` pour écrire ;
  • idempotent : relancé, il ne trouve plus rien à faire ;
  • aucune suppression sans fusion préalable, donc aucun film ni créneau perdu.

Usage :
    python3 scripts/remigre_cles_evenements.py            # simulation
    python3 scripts/remigre_cles_evenements.py --apply    # écrit
Requiert : SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY dans l'environnement.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from supabase import create_client

# Même fonction que le scraper : la migration ne peut pas diverger de ce qui
# sera écrit au prochain run.
from scraper import event_dedup_key  # noqa: E402

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


def richesse(ev: dict) -> tuple:
    """Critère d'élection : le plus de contenu, puis la plus ancienne ligne."""
    return (len(ev["_films"]) + len(ev["_creneaux"]), ev.get("created_at") or "")


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

    evs = paginate(lambda: client.table("evenements").select(
        "id,cle,type,titre,date_debut,date_fin,description,affiche_url,forme,source,source_url,created_at"))
    films = paginate(lambda: client.table("evenement_films").select(
        "id,evenement_id,titre,titre_key,film_id,affiche_url,ordre"))
    creneaux = paginate(lambda: client.table("evenement_seances").select(
        "id,evenement_id,cinema_id,date,heure,titre_film,invite,description,resa_url,seance_id,film_id"))

    par_ev_films = defaultdict(list)
    for f in films:
        par_ev_films[f["evenement_id"]].append(f)
    par_ev_creneaux = defaultdict(list)
    for c in creneaux:
        par_ev_creneaux[c["evenement_id"]].append(c)

    for ev in evs:
        ev["_films"] = par_ev_films.get(ev["id"], [])
        ev["_creneaux"] = par_ev_creneaux.get(ev["id"], [])
        # `event_dedup_key` attend la forme du scraper : une liste de films
        # portant un `titre`. `titre_key` de la table en est le miroir.
        ev["_nouvelle_cle"] = event_dedup_key({
            "type": ev["type"], "titre": ev["titre"],
            "films": [{"titre": f["titre"]} for f in ev["_films"]],
            "date_debut": ev.get("date_debut"), "date_fin": ev.get("date_fin"),
        })

    groupes: dict = defaultdict(list)
    for ev in evs:
        groupes[ev["_nouvelle_cle"]].append(ev)

    fusions = [g for g in groupes.values() if len(g) > 1]
    renommages = [g[0] for g in groupes.values()
                  if len(g) == 1 and g[0]["cle"] != g[0]["_nouvelle_cle"]]

    print(f"{len(evs)} évènements, {len(groupes)} clés distinctes après migration.")
    print(f"  • {len(fusions)} groupe(s) à fusionner "
          f"({sum(len(g) - 1 for g in fusions)} ligne(s) supprimée(s))")
    print(f"  • {len(renommages)} clé(s) simplement réécrite(s)")

    for g in fusions:
        g_tri = sorted(g, key=richesse, reverse=True)
        survivante, perdantes = g_tri[0], g_tri[1:]
        print(f"\n  « {survivante['titre'][:48]} » → {survivante['_nouvelle_cle']}")
        print(f"      garde  {survivante['cle']}  "
              f"({len(survivante['_films'])} films, {len(survivante['_creneaux'])} créneaux)")
        for p in perdantes:
            print(f"      fusionne {p['cle']}  "
                  f"({len(p['_films'])} films, {len(p['_creneaux'])} créneaux)")

    if not args.apply:
        print("\nSimulation — relancer avec --apply pour écrire.")
        return

    n_films = n_creneaux = n_suppr = n_cles = 0

    for g in fusions:
        g_tri = sorted(g, key=richesse, reverse=True)
        survivante, perdantes = g_tri[0], g_tri[1:]
        sid = survivante["id"]

        # Films : UNIQUE(evenement_id, titre_key) — on n'insère que l'inédit.
        vus_f = {f["titre_key"] for f in survivante["_films"]}
        # Créneaux : UNIQUE NULLS NOT DISTINCT sur (ev, cinema, date, heure, titre_film).
        sig = lambda c: (c["cinema_id"], c["date"], c["heure"], c["titre_film"])
        vus_c = {sig(c) for c in survivante["_creneaux"]}

        for p in perdantes:
            for f in p["_films"]:
                if f["titre_key"] in vus_f:
                    continue
                vus_f.add(f["titre_key"])
                try:
                    client.table("evenement_films").update(
                        {"evenement_id": sid}).eq("id", f["id"]).execute()
                    n_films += 1
                except Exception as e:
                    print(f"      ⚠ film {f['titre'][:30]} non déplacé : {e}")
            for c in p["_creneaux"]:
                if sig(c) in vus_c:
                    continue
                vus_c.add(sig(c))
                try:
                    client.table("evenement_seances").update(
                        {"evenement_id": sid}).eq("id", c["id"]).execute()
                    n_creneaux += 1
                except Exception as e:
                    print(f"      ⚠ créneau {c['date']} non déplacé : {e}")

        # Période élargie + champs comblés, avant de perdre les perdantes.
        maj: dict = {}
        debuts = sorted(x["date_debut"] for x in g if x.get("date_debut"))
        fins = sorted(x["date_fin"] for x in g if x.get("date_fin"))
        if debuts and debuts[0] != survivante.get("date_debut"):
            maj["date_debut"] = debuts[0]
        if fins and fins[-1] != survivante.get("date_fin"):
            maj["date_fin"] = fins[-1]
        for champ in ("description", "affiche_url", "forme", "source", "source_url"):
            if not survivante.get(champ):
                for x in perdantes:
                    if x.get(champ):
                        maj[champ] = x[champ]
                        break
        if maj:
            client.table("evenements").update(maj).eq("id", sid).execute()

        # Suppression APRÈS déplacement : les enfants restants tombent en CASCADE.
        for p in perdantes:
            client.table("evenements").delete().eq("id", p["id"]).execute()
            n_suppr += 1

        # `cle` est UNIQUE : la réécriture peut heurter une ligne qui détient
        # encore la clé visée et n'a pas encore été traitée. On le signale au
        # lieu de tomber — la fusion, elle, est déjà acquise, et un second
        # passage du script rattrapera la clé restée en arrière.
        if survivante["cle"] != survivante["_nouvelle_cle"]:
            try:
                client.table("evenements").update(
                    {"cle": survivante["_nouvelle_cle"]}).eq("id", sid).execute()
                n_cles += 1
            except Exception as e:
                print(f"      ⚠ clé non réécrite ({e}) — relancer le script")

    for ev in renommages:
        try:
            client.table("evenements").update(
                {"cle": ev["_nouvelle_cle"]}).eq("id", ev["id"]).execute()
            n_cles += 1
        except Exception as e:
            print(f"  ⚠ « {ev['titre'][:40]} » : clé non réécrite ({e})")

    print(f"\n✓ {n_suppr} doublon(s) supprimé(s), {n_cles} clé(s) réécrite(s), "
          f"{n_films} film(s) et {n_creneaux} créneau(x) déplacé(s).")


if __name__ == "__main__":
    main()
