# Architecture — Frontend (index.html)

> Le front est **servi** comme un fichier autonome (`index.html`, ~126 Ko : markup + CSS + JS inline) — mais il est **généré** : depuis 2026-07-22 il est assemblé par `build_ui.py` à partir de `src/` (`template.html` + `components.css` + `tokens.css` généré depuis `design/tokens.json`). **Ne pas éditer `index.html` à la main** — éditer `src/`/`design/` puis `python3 build_ui.py`. Le runtime reste mono-fichier (bon pour GitHub Pages) ; seule la *source* est découpée. Rôle inchangé : chargement des données, dédup d'affichage, filtres, rendu semaine, deep-link réservation. Les invariants/contrats transverses vivent dans [Vue d'ensemble](README.md).
> Dernière mise à jour : 2026-07-29

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

## Onglet Événements (2026-07-29)

Alimenté par Supabase (`loadEventsFromSupabase`, 4 tables jointes) avec repli sur la clé `evenements` de `programme.json` (contrat C4). `EVENTS_DATA` a disparu.

**État propre à l'onglet** : `evData`, `evMois` (`YYYY-MM`), `filtreType`, `evSeeds`/`evResumes` (par mois), `evSel`/`evSelIdx` (éventail). ⚠️ **`filtreCinema` est PARTAGÉ** avec l'onglet Séances et persiste au basculement ; le filtre type et le mois se réinitialisent (c'est `swTab` qui le fait). La barre cinémas reste donc visible dans les deux modes — elle était masquée avant.

### Les fonctions pures (là où vivent les règles)

| Fonction | Règle portée |
|---|---|
| `eventDateChip(dates, mois, opts)` | Les **six formes** de chip + **règle du mois omis** (le mois ne s'écrit que si la période déborde du mois affiché). `opts.alwaysMonth` pour le niveau 2 (pas de mois affiché), `opts.et` pour « 15 et 24 juillet », `opts.mode` pour la bascule liste/période |
| `evChipDates(ev, cinema)` | La chip est **dérivée du périmètre, jamais stockée** : filtré sur Comoedia, *L'Inconnue* affiche `20` ; sur Terreaux, `24` ; sans filtre, `20 & 24`. Forme « liste » **seulement si** les créneaux datés (≤ 2) couvrent les bornes annoncées — sinon un festival dont une seule séance est connue s'écrirait « 1 juillet & 1 septembre » |
| `sortEvents(events, mois)` | Bloc large (couvre tout le mois, ou `saison`/`mois`/`en_cours`) trié par durée décroissante, puis bloc daté par `date_debut` (repli `date_fin`) |
| `monthsWithEvents(events, …)` | Alimente **les trois** chemins de navigation (flèches, sélecteur, « Mois suivant ») : un mois vide n'est atteignable par aucun |
| `pickSelection(events, seed, scope)` | Tirage stable (hash FNV-1a de `seed|scope|clé`), paliers 1⇒1, 2–3⇒1, 4–6⇒3, ≥7⇒5, appliqués **au périmètre filtré** — filtrer a posteriori un tirage global donnerait 0, 1 ou 2 cartes au hasard |

Ces fonctions sont délimitées par `// @test-block` dans `src/template.html` et testées par `node tests/chip_dates.test.mjs`, qui extrait le bloc et l'évalue (aucune dépendance npm, la contrainte mono-fichier reste intacte).

**Résumé du mois** : segments typés (`strong`/`mute`/`icon`) produits par le scraper. **Masqué dès qu'un filtre est actif** (il décrit le mois entier) et absent si le scraper n'a rien généré. Les icônes sont une **liste fermée** de SVG inline (`EV_ICONS`), miroir de `EVENT_ICONS` côté scraper — un nom hors liste est ignoré.

**Éventail** : trois niveaux de profondeur en CSS pur (`transform`/`opacity`), décalages et rotations en variables, `prefers-reduced-motion` respecté. Une affiche qui casse retombe sur le visuel local **sans** faire disparaître la carte (le nombre de cartes suit une règle, il ne doit pas dépendre d'un 403).

### Niveau 2 — détail d'un événement

`openEvent(cle)` rend dans le **panneau existant** (`.d-hero` + `.d-body`) : même grammaire deux blocs, même fermeture, même backdrop que la fiche film. Les dates de chaque film viennent des **créneaux** (`evFilmToutesDates`) — Supabase ne renvoie aucune date sur `evenement_films`, les lire là mettrait tous les films en « Séances non encore annoncées ».

Trois états d'affordance (`evFilmEtat`) : « Détails et séances » + `+` (film résolu dans `allFilms`, date à venir) · « Séance passée » · « Séances non encore annoncées » — ce dernier est le **régime normal** au-delà de la semaine scrapée.

⚠️ **Pièges d'intégration** : `swTab('events')` masque `#sidePanel` → `openFilm` et `openEvent` le ré-affichent, sinon le rendu se ferait dans un conteneur caché. `openFilm` reçoit le **titre canonique de `films`** (résolu via `allFilms`), jamais le titre affiché dans l'événement (le Comoedia titre en capitales). Le FLIP ne trouve pas de `.film-card` depuis cet onglet et se dégrade seul — c'est voulu.

---

## Routing par URL (2026-07-29)

L'app n'avait aucun routing ; le parcours a maintenant trois profondeurs (niveau 1 → détail événement → fiche film), donc une URL.

- `serializeState()` → `#/seances?cine=…&vue=…&periode=…&jour=…&film=…` ou `#/evenements?cine=…&type=…&mois=…&ev=…&film=…`.
- `pushRoute()` est appelé par **tous les mutateurs** (`swTab`, `setCinema`, `setViewMode`, `setCompactView`, `pickDay`, `navDay`, `setEvMois`, `setFiltreType`, `openFilm`, `openEvent`, `closeDetail`).
- `applyState(hash)` restaure l'état ; écouteur `popstate` + lecture du hash au boot.

⚠️ **La zone de régression n'est pas le volume, c'est l'atterrissage** — deux gardes portées par le drapeau `_restoring` :
1. **le FLIP est court-circuité** (`_flipEnabled()` renvoie `false`) : il mesure une `.film-card` qui n'existe pas encore au chargement à froid ;
2. **`_listScrollY` n'est pas écrasé** par le scroll (nul) d'une page en cours de restauration.

`closeDetail` capture le titre du film fermé **avant** de remettre l'état à zéro — sinon le FLIP de fermeture perd sa cible.

---

## Preview locale (contournement sandbox macOS)

Le serveur de preview tourne sandboxé et ne lit pas `~/Documents` (TCC). On sert une copie depuis `/tmp` : `bash scripts/sync_preview.sh` → `/tmp/cineinde_preview/`, puis `preview_start` (port 4173). Données toujours live (le navigateur lit Supabase, non sandboxé). Détails : *Workflow* (vault Obsidian).
