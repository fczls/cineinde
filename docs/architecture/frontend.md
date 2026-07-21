# Architecture — Frontend (index.html)

> Le front est un **mono-fichier** (~120 Ko : markup + CSS + JS inline, pas de build). Sert de tout : chargement des données, dédup d'affichage, filtres, rendu semaine, deep-link réservation. Les invariants/contrats transverses vivent dans [Vue d'ensemble](README.md).
> Dernière mise à jour : 2026-07-20

---

## Couche de données (Supabase live + fallback)

1. **Lecture Supabase en direct** (`loadFromSupabase()`) : requête `seances` avec jointure `films (...)` et `cinemas (name)`, **filtrée `date >= today`** (index.html) — sinon l'historique (>5000 lignes) dépasse le plafond REST 1000 et tronque les séances récentes (invariant I8).
2. **Fallback `programme.json`** si Supabase est indisponible / vide (invariant I3). Même forme de « film dict » (contrat C4). Le chargeur `loadFromJson` **strippe `resa_url`** à la frontière (I5) : le repli ne sert jamais de deep-link figé/périmé.
3. Le front **n'écrit jamais** — RLS lecture seule (invariant I7).

### Re-split par cinéma (subtilité C2)
Supabase **dédup** le film (une ligne `films` partagée entre salles). À la lecture, le front **regroupe par `${film.id}-${cinemaName}`** (`loadFromSupabase()`) → un film joué dans 2 salles redevient **2 entrées d'affichage**, chacune avec ses séances. Le champ `cinema` est donc reconstruit côté front à partir de la jointure `cinemas(name)`.

### Dédup d'affichage
`normalizeTitle` = équivalent front de `_normalize_title_key` (back). ⚠️ **Duplication à maintenir en phase** (contrat C3).

**`dedupeFilms` (frontier de chargement, 2026-07-20)** — filet front contre les lignes `films` dupliquées en base (variantes de casse, ou même titre sans imdb_id qui a glissé sous la clé de repli). Sans lui, un même `(film, cinéma)` produit plusieurs entrées → **séances dédoublées** en vue « tous » (les `_day` se cumulent) et **film en double** en vue filtrée. Deux passes : (1) fusion par `(filmGroupKey, cinéma)` ; (2) rattrapage des mauvais appariements imdb_id par `(cinéma, titre normalisé, réalisateur compatible)` — même axe que le durcissement back (I1), homonymes réels préservés (réalisateurs différents). Puis dédoublonnage des séances (cinéma+date+heure+version).

---

## Cinémas — le savoir codé en dur (⚠️ contrat C3)

Ajouter/retirer un cinéma touche **4 listes + 1 fonction**, autour de ces symboles dans index.html (`CINEMA_FILTERS`, `SHORT_CINEMA`, `CINEMA_SHORT`, `getCinemaSectionLabel`) :

| Structure | Rôle |
|---|---|
| `SHORT_CINEMA` | libellés courts (chips des cartes) |
| `CINEMA_FILTERS` | liste du sélecteur (pills) — le pill « Tous » affiche un compte **dynamique** (`Les N cinémas`) |
| `CINEMA_SHORT` | libellés du picker mobile |
| `getCinemaSectionLabel()` | libellé de section — branches Lumière / **Le Zola** / défaut Comédia ⚠️ un nouveau cinéma sans branche tombe dans « COMÉDIA » |
| texte « à propos » | phrase descriptive figée |

C'est le pendant front de `CINEMA_SLUGS` (scraper.py). *(Le Zola ajouté partout le 2026-07-10 — le « piège » historique de la logique binaire de `getCinemaSectionLabel` reste vrai pour tout prochain cinéma.)*

---

## Rendu & état

- **État global** (variables en tête du `<script>`) : `filtreCinema` (`'tous'` | nom), `compactView` (détaillée/liste), `currentTab` (`'seances'` | `'events'`).
- **Semaine mer→mar** : `getWeekBounds()` — miroir front de `get_last_wednesday()` back. Le tableau semaine s'affiche today→mardi.
- **Filtrage cinéma** au rendu : `f.cinema !== filtreCinema` (au filtrage du rendu, `renderFilms()`).
- **Tri de la liste (2026-07-20)** : `getRowsForDate` classe, pour *aujourd'hui*, par **proximité de la prochaine séance** (`_nextUp` = 1re séance non passée) ; les films dont toutes les séances du jour sont passées (`_nextUp === null`) sont relégués en bas, et `renderFilmsStandard` insère un séparateur « Ces films n'ont plus de séance aujourd'hui » avant eux. Jours futurs : tri par 1re séance (tout est à venir).

---

## Réservation — deep-link billetterie (2026-07-20)

Le front rend le `resa_url` de chaque séance (option A « câbler l'existant » — cf. *Exploration — Accès billetterie*, vault). Deux composants « Réserver » révélés au survol (animés avec **Motion**, paquet `motion` chargé en ESM depuis jsdelivr, comme Supabase ; délégation `pointerover`/`focusin` survivant aux re-rendus `innerHTML` ; repli CSS `:hover` sous `html:not(.motion-ready)` + `prefers-reduced-motion`) :
- **`cardSlotHtml`** (bouton créneau des cartes) : l'horaire rétrécit et remonte à la place de la langue, « Réserver » + billet montent du bas.
- **`detailChipHtml`** (pastille de la fiche « Séances de la semaine ») : bascule langue → billet.

Un lien absent (Comoedia, ou séance passée) → bouton **informatif** qui ouvre la fiche, **sans** la fausse promesse « Réserver » (scénario S2). Drapeau de sortie : `ENABLE_RESA_LINKS` (1 booléen).

**Sécurité (⚠️ seule défense — pas de CSP posable sur GitHub Pages)** : `safeResaUrl` n'accepte qu'un `https` vers un hôte connu (`*.cotecine.fr`, `www.ticketingcine.com`) — rejette `javascript:`/`data:`/hôtes tiers. C'est la **re-validation front** du filtrage back `is_valid_resa_url` (contrat C3, dédup voulue : le front lit 2 sources, ne fait confiance à aucune). Plus `rel="noopener noreferrer"` + `<meta name="referrer" content="no-referrer">`.

---

## Onglet Événements (⚠️ dette — chantier futur)

Aujourd'hui **100 % figé, aucun backend** :
- `EVENTS_DATA` = tableau en dur, 4 events périmés (reliquat de maquette).
- `renderEvents()` ne fait qu'afficher ce tableau statique.

**Cible** (voir *Exploration — Événements* (vault Obsidian)) : nouvelle table Supabase `evenements`, parsers côté scraper, et `renderEvents()` alimenté par requête Supabase + fallback JSON — en réutilisant le même schéma de champs (`titre, jour, mois, heure, lieu, desc, type, color`). C'est une feature **transverse** (Frontend + Pipeline + Infra).

---

## Preview locale (contournement sandbox macOS)

Le serveur de preview tourne sandboxé et ne lit pas `~/Documents` (TCC). On sert une copie depuis `/tmp` : `bash scripts/sync_preview.sh` → `/tmp/cineinde_preview/`, puis `preview_start` (port 4173). Données toujours live (le navigateur lit Supabase, non sandboxé). Détails : *Workflow* (vault Obsidian).
