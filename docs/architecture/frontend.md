# Architecture — Frontend (index.html)

> Le front est un **mono-fichier** (~110 Ko : markup + CSS + JS inline, pas de build). Sert de tout : chargement des données, dédup d'affichage, filtres, rendu semaine. Les invariants/contrats transverses vivent dans [Vue d'ensemble](README.md).
> Dernière mise à jour : 2026-07-10

---

## Couche de données (Supabase live + fallback)

1. **Lecture Supabase en direct** (index.html:1127+) : requête `seances` avec jointure `films (...)` et `cinemas (name)`, **filtrée `date >= today`** (index.html) — sinon l'historique (>5000 lignes) dépasse le plafond REST 1000 et tronque les séances récentes (invariant I8).
2. **Fallback `programme.json`** si Supabase est indisponible / vide (invariant I3). Même forme de « film dict » (contrat C4).
3. Le front **n'écrit jamais** — RLS lecture seule (invariant I7).

### Re-split par cinéma (subtilité C2)
Supabase **dédup** le film (une ligne `films` partagée entre salles). À la lecture, le front **regroupe par `${film.id}-${cinemaName}`** (index.html:1161) → un film joué dans 2 salles redevient **2 entrées d'affichage**, chacune avec ses séances. Le champ `cinema` est donc reconstruit côté front à partir de la jointure `cinemas(name)`.

### Dédup d'affichage
`normalizeTitle` (index.html:891) = équivalent front de `_normalize_title_key` (back). ⚠️ **Duplication à maintenir en phase** (contrat C3).

---

## Cinémas — le savoir codé en dur (⚠️ contrat C3)

Ajouter/retirer un cinéma touche **4 listes + 1 fonction**, autour de index.html:1267-1296 :

| Structure | Ligne | Rôle |
|---|---|---|
| `SHORT_CINEMA` | 1267 | libellés courts |
| `CINEMA_FILTERS` | 1274 | liste du sélecteur (pills) |
| `CINEMA_SHORT` | 1281 | libellés du picker mobile |
| `getCinemaSectionLabel()` | 1289 | **logique binaire** Lumière/Comédia — tout autre cinéma tombe dans « COMÉDIA » ⚠️ |
| texte « à propos » | 738 | phrase descriptive figée |

C'est le pendant front de `CINEMA_SLUGS` (scraper.py:1173). Détail du piège Zola : *Challenge — Ajout Cinéma Le Zola* (vault Obsidian).

---

## Rendu & état

- **État global** (index.html:1262-1265) : `filtreCinema` (`'tous'` | nom), `compactView` (détaillée/liste), `currentTab` (`'seances'` | `'events'`).
- **Semaine mer→mar** : `getWeekBounds()` (index.html:897) — miroir front de `get_last_wednesday()` back. Le tableau semaine s'affiche today→mardi.
- **Filtrage cinéma** au rendu : `f.cinema !== filtreCinema` (index.html:1523).

---

## Onglet Événements (⚠️ dette — chantier futur)

Aujourd'hui **100 % figé, aucun backend** :
- `EVENTS_DATA` = tableau en dur (index.html:870), 4 events périmés (reliquat de maquette).
- `renderEvents()` (index.html:2047) ne fait qu'afficher ce tableau statique.

**Cible** (voir *Exploration — Événements* (vault Obsidian)) : nouvelle table Supabase `evenements`, parsers côté scraper, et `renderEvents()` alimenté par requête Supabase + fallback JSON — en réutilisant le même schéma de champs (`titre, jour, mois, heure, lieu, desc, type, color`). C'est une feature **transverse** (Frontend + Pipeline + Infra).

---

## Preview locale (contournement sandbox macOS)

Le serveur de preview tourne sandboxé et ne lit pas `~/Documents` (TCC). On sert une copie depuis `/tmp` : `bash scripts/sync_preview.sh` → `/tmp/cineinde_preview/`, puis `preview_start` (port 4173). Données toujours live (le navigateur lit Supabase, non sandboxé). Détails : *Workflow* (vault Obsidian).
