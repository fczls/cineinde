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

## 2026-07-08

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
