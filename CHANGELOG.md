# Changelog

Toutes les modifications notables du **code** de CinéInde sont consignées ici.
Les commits automatiques de données (`chore: mise à jour programme …`) ne sont
pas listés — ils sont quotidiens et n'affectent pas le comportement du projet.

Format inspiré de [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/).
Convention de commits : `feat:` / `fix:` = code (listé ici), `chore:` = données (ignoré).

## [Non publié]

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

### Ajouté
- **Nettoyage hebdomadaire de Supabase** (`scripts/cleanup_old_seances.py` +
  workflow `cleanup.yml`). Tous les jeudis 03:00 UTC : suppression des séances
  antérieures à J−10 puis des films orphelins (sans séance restante). Garde la
  table légère et sous le plafond de 1000 lignes de l'API REST. Déclenchable
  manuellement avec `days` et `dry_run`. Réutilise les secrets Supabase existants.

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
