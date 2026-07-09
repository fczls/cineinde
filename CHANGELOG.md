# Changelog

Toutes les modifications notables du **code** de CinéInde sont consignées ici.
Les commits automatiques de données (`chore: mise à jour programme …`) ne sont
pas listés — ils sont quotidiens et n'affectent pas le comportement du projet.

**Organisation du fichier : chronologie → tag → détails.**
- **Chronologie** : une section par date (`## AAAA-MM-JJ`), la plus récente en haut.
- **Tag** : sous chaque date, regrouper les changements par type — `Ajouté`,
  `Modifié`, `Corrigé`, `Supprimé` (sections omises si vides).
- **Détails** : sous chaque tag, une puce par changement, avec le fichier ou le
  commit concerné entre parenthèses.

Convention de commits : `feat:` / `fix:` = code (listé ici), `chore:` = données (ignoré).
Format inspiré de [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/).

> ⚠️ **Doc d'architecture** ([`docs/architecture/`](docs/architecture/) — voir [`ARCHITECTURE.md`](ARCHITECTURE.md)) :
> ne la mettre à jour **que si** un changement touche l'un des **invariants (I1–I8)** ou
> **contrats (C1–C4)** listés dans le hub. Dans ce cas, mettre à jour la note concernée dans
> la même PR. Un `fix:` de parsing/sélecteur/casse qui ne touche aucun de ces points ne
> demande rien. Le CHANGELOG garde la *chronologie* ; l'architecture garde l'*état stable*.

## 2026-07-10

### Ajouté
- **Doc d'architecture fonctionnelle** (`docs/architecture/` + `ARCHITECTURE.md`).
  Référence de l'*état stable* du système, organisée en invariants (I1–I8),
  contrats/liants (C1–C4) et variants : hub `docs/architecture/README.md` + 3
  spokes (`pipeline.md`, `frontend.md`, `data-infra.md`). Comble le manque d'une
  vue « comment ça marche maintenant » distincte de la chronologie du CHANGELOG.
  `ARCHITECTURE.md` (racine) sert de point d'entrée. Rédigée d'abord dans le vault
  Obsidian puis rapatriée dans le repo pour être versionnée avec le code (relue
  dans la même PR, pas de dérive entre deux emplacements). La réflexion
  prospective (explorations, challenges) et le process (`Workflow`) restent dans
  le vault.
- **Règle de maintenance de l'architecture** (en tête de ce fichier +
  `ARCHITECTURE.md`) : mettre à jour `docs/architecture/` seulement quand un
  changement touche un invariant/contrat listé, pas à chaque entrée CHANGELOG.

## 2026-07-09

### Modifié
- **Refonte complète de la navigation (barre flottante style Airbnb).**
  (`index.html`, #5)
  - Sélecteur de cinémas en haut : pills scrollables au drag + flèches
    conditionnelles ; se masque au scroll vers le bas, réapparaît vers le haut.
  - Barre flottante regroupant Séances/Évènements, le sélecteur de date et le
    type de vue (Détaillée/Liste). Se **compacte en icônes au scroll** (textes →
    bobine/étincelles/stack/liste) avec une transition fluide (largeur animée +
    fade du strip cinémas), et reste fixe en haut, pleine largeur, avec une
    border-bottom ; le contenu passe dessous.
  - Sélecteur de date : flèches ‹ › conditionnelles regroupées avec le bouton
    date dans un conteneur, apparition/disparition **animée** (largeur + fondu).
    En vue 7J le bouton date reste visible en état **inactif** (ne pilote plus
    l'affichage) mais reste cliquable — y choisir une date **bascule sur la
    vue 1J** au bon jour.
  - Segment 1J/7J : apparition animée via technique grid `0fr` → `1fr` (largeur
    auto exacte + fondu), sans à-coup du conteneur parent.
  - Palette de la barre : fond `#262020`, stroke `#33302E`, stroke des éléments
    actifs `#55473A`.
  - Titre raccourci en « CinéIndé Lyon » ; vue mobile dédiée (logo court, bouton
    « Les cinés », burger, toolbar en icônes).
- **Layout des films en colonne unique centrée** (`index.html`, #5). Le panneau
  détail ne s'ouvre plus par défaut : il n'apparaît qu'à la sélection d'un film.
  Bordure des cartes passée en noir + effet **« spotlight » réactif à la souris**
  au survol (halo `#78695B` diffusé autour du curseur), desktop uniquement.

### Supprimé
- **Colonne détail « Sélectionnez un film » affichée par défaut** (remplacée par
  la colonne de films centrée) et **sous-titre de l'en-tête**. (`index.html`, #5)

## 2026-07-08

### Corrigé
- **Écriture conditionnelle neutralisée par un champ volatil** (`scraper.py`,
  `main` + nouveau `_films_sans_volatiles`). Le champ `resa_url` des séances
  Lumière (lien cotecine) embarque un token horaire que le site régénère à
  chaque fetch : la comparaison `previous == all_films` voyait donc toujours un
  changement et réécrivait/committait `programme.json` à chaque run — le
  bénéfice « runs silencieux » était nul (détecté en run réel : deux runs
  espacés de ~3 h ne différaient QUE par `resa_url`). La détection de changement
  ignore désormais les champs volatils (`VOLATILE_SEANCE_FIELDS`) ; le `resa_url`
  frais reste écrit lors d'une réécriture déclenchée par un vrai changement.

### Modifié
- **Cadence du scraper : passage à un run un jour sur deux**
  (`.github/workflows/scraper.yml`). Remplace les deux crons serrés (mardi 20h +
  mercredi 1h UTC, séparés de 5 h) par un seul `0 20 */2 * *`. Motif : les deux
  runs se chevauchaient sur le même créneau de publication puis laissaient un
  **angle mort de ~6,5 jours** — une publication tardive ou une correction en
  milieu de semaine n'était reprise qu'au mardi suivant. Le run à 20:00 UTC
  couvre la publication du mardi soir (pas de régression de fraîcheur) et reprend
  toute mise à jour sous ≤48 h. Le frontend lisant Supabase en direct, la
  fraîcheur = l'instant d'insertion, pas le commit. (Le workflow est aussi
  renommé « Scraper programme » — nom neutre vis-à-vis de la cadence.)
- **Garde-fou Comoedia rendu asymétrique** (`scraper.py`, fonction `main`). On
  n'échoue (`exit 4`) désormais que si **Lumière a publié la semaine courante
  mais pas Comoedia** — l'asymétrie qui signe une vraie panne du parser Comoedia.
  Si aucune source n'a publié (personne n'a encore mis en ligne), c'est un
  non-événement → pas d'échec. Indispensable pour que la cadence rapprochée ne
  parte pas au rouge avant publication. La présence est lue dans
  Supabase après l'upsert du run. Nouveau helper `count_week_seances(...,
  slug=/exclude_slug=)` factorisant le comptage des séances par semaine.
- **Écriture conditionnelle de `programme.json`** (`scraper.py`, fonction
  `main`). Le fichier n'est réécrit que si la liste des films change réellement.
  Auparavant le champ `generated_at` bougeait à chaque run et forçait un commit
  + un redéploiement GitHub Pages à chaque run inutilement ; les runs sans
  nouveauté sont maintenant totalement silencieux.

### Corrigé
- **Faux échec `exit 4` du scraper chaque semaine (garde-fou Comoedia).** Le
  garde-fou anti-échec silencieux concluait « pipeline Comoedia cassé » dès que
  0 film était scrapé *pendant ce run*. Or le PDF hebdo n'est publié qu'une fois
  par semaine mais le cron tourne 2× (mardi 20h + mercredi 1h UTC) : le second
  run dédupliquait légitimement le PDF déjà traité (« PDF déjà traité — ignoré »)
  et renvoyait 0 film — déclenchant `sys.exit(4)` à tort. Le garde-fou vérifie
  désormais le **vrai** critère de santé : la semaine courante a-t-elle des
  séances Comoedia *en base* (peu importe quel run les a insérées) ? Il ne
  échoue que si la semaine est réellement absente (site changé, PDF
  introuvable/illisible). La semaine de référence est celle **en cours** (sans
  saut au mercredi suivant le mardi), pour ne pas échouer le run anticipé du
  mardi quand Comoedia n'a pas encore publié la semaine à venir. (`scraper.py`,
  fonction `main`)

## 2026-07-04

### Ajouté
- **Correction de la casse des titres déjà en base** (`scripts/fix_titles_case.py`).
  Script one-shot qui réapplique `_titlecase_fr` aux titres stockés dans Supabase
  (corrige les all-caps Comoedia historiques). Les titres déjà bien casés ne sont
  pas touchés. Réutilise le `.env` et les secrets Supabase existants. (`b29081c`)
- **Garde-fou anti-échec silencieux Comoedia** (`scraper.py`). Si 0 séance
  Comoedia n'est remontée, le job publie quand même Lumière puis se termine en
  échec (`sys.exit(4)`) pour que la GitHub Action passe au rouge au lieu de
  publier sans Comoedia sans alerte. (`3cd4698`)
- **`--pdf-url` force désormais le retraitement** d'un PDF même s'il figure déjà
  dans `processed_urls` (indispensable au debug et à la reprise manuelle).
  (`fb88755`)

### Corrigé
- **Comoedia de nouveau à 0 séance affichée.** Le cinéma expose deux PDF CDN
  (nom en hash) répartis sur deux pages, et selon les semaines l'un OU l'autre
  porte la grille courante (l'autre est parfois en retard d'une semaine) :
  la page d'accueil portait la semaine en cours, tandis que
  `/horaires-semaine-complete/` servait encore la semaine précédente (déjà en
  base, donc filtrée par l'affichage « séances à venir »). `fetch_pdf_urls()`
  scanne désormais les **deux** pages et **agrège** tous les PDF candidats au
  lieu de s'arrêter au premier ; la déduplication par semaine (Supabase) garde
  la semaine nouvelle et ignore celle déjà en base. Les deux URL CDN restées
  coincées dans `processed_urls` (donc systématiquement sautées) ont été
  purgées. (`3cd4698`, `fb88755`)
- **Titres Comoedia en CAPITALES.** Les titres extraits du PDF étaient affichés
  en tout-capitales. Ajout de `_titlecase_fr` dans `clean_pdf_table` : casse
  « titre » adaptée au français (mots courts en minuscules, apostrophes et
  traits d'union gérés — `L'Amour`, `Jean-Pierre`), appliquée uniquement quand
  le titre est majoritairement en capitales (un titre déjà bien casé, ex. TMDB,
  n'est pas modifié). Historique corrigé via `scripts/fix_titles_case.py`.
  (`79f37bb`)

## 2026-06-30

### Ajouté
- **Nettoyage hebdomadaire de Supabase** (`scripts/cleanup_old_seances.py` +
  workflow `cleanup.yml`). Tous les jeudis 03:00 UTC : suppression des séances
  antérieures à J−10 puis des films orphelins (sans séance restante). Garde la
  table légère et sous le plafond de 1000 lignes de l'API REST. Déclenchable
  manuellement avec `days` et `dry_run`. Réutilise les secrets Supabase existants.

### Modifié
- **Refonte de la fiche film (desktop + mobile).** (`index.html`)
  - Panneau ancré au bord droit, largeur `clamp(560px, 46vw, 820px)` (au lieu
    d'une colonne ~432px centrée avec décalage à droite).
  - Fermeture au clic sur le fond assombri (backdrop) + touche `Échap` ; sur
    desktop, bouton fermer flottant centré verticalement, 40px à gauche du panneau.
  - **Hero = backdrop paysage TMDB** (`backdrop_path`, w1280) net, au lieu de
    l'affiche floutée ; repli sur l'affiche floutée si absent. Bouton
    bande-annonce centré dans le hero ; titre + note superposés en bas du hero
    (desktop) ou sous le hero (mobile, croix en coin).
  - Liste des séances en chips plus fines.
- **Vue liste : aperçu poster au survol agrandi ×1.75** (110×165 → 193×289 px).
  Taille mise à jour en CSS (`#compactPosterTooltip`) et dans le calcul de
  positionnement JS (`TW`/`TH`) pour garder le centrage. (`index.html`)

### Corrigé
- **Site vide alors que Supabase contient les séances du jour.** La requête
  frontend (`loadFromSupabase`) récupérait toutes les séances sans filtre de
  date ni limite. La table contient tout l'historique (>5700 lignes) et l'API
  REST Supabase plafonne à 1000 lignes : les séances récentes étaient tronquées,
  ne laissant quasi rien dans la fenêtre d'affichage (aujourd'hui → mardi).
  Ajout d'un filtre serveur `.gte('date', aujourd'hui).order('date')` →
  ~312 lignes pertinentes, bien sous le plafond. (`index.html`)
- **Scraping Comoedia rétabli** (produisait 0 film) après deux changements côté
  cinema-comoedia.com :
  - Migration des PDF hebdomadaires vers un CDN (`cms-assets.webediamovies.pro`)
    avec noms de fichiers opaques ; l'ancienne page `/programme-semaine/` renvoie
    404. `fetch_pdf_urls()` découvre désormais le lien depuis la page d'accueil,
    avec repli sur l'ancienne page de listing puis la prédiction d'URL.
  - Ordre des pages du PDF inversé (planning passé en page 1) :
    `parse_comoedia_pdf()` repère le tableau par son contenu (entête de jours)
    au lieu d'un index de page fixe, et lit la date sur n'importe quelle page.
  - Regex de date assouplie pour le format « du Mercredi 17 au Mardi 23 Juin 2026 ».
  - Résultat : 22 films Comoedia extraits, 49 films au total. (`e71e8fd`)
