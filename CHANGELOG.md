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

## 2026-07-25

> Refonte de la **fiche détail desktop** (overlay deux blocs, backdrop + bande-annonce,
> images HD, transition FLIP) et assainissement du **parsing des titres Comœdia**.
> Ajout des colonnes `films.backdrop` / `films.trailer` (migration 004) : purement
> additif, ne touche aucun invariant (I1–I8) ni contrat (C1–C4).

### Ajouté
- **Fiche détail desktop en overlay plein écran, deux blocs** : visuel (affiche/backdrop) à
  gauche, détail à droite encadrant le bouton fermer ; coins 40px, liseré `--border`,
  marges/gaps 16px, responsive 3 phases (510px fixe → fill → 820px centré). Mobile inchangé.
  (`src/components.css`, `src/template.html`)
- **Bloc visuel** : backdrop TMDB sinon affiche ; fallback local `assets/visual-fallback.webp`
  (WebP ~17 Ko) + voile 60% ; `onerror` de secours.
- **Bouton bande-annonce** (trailer YouTube via TMDB `/videos`).
- **Transition FLIP liste ⇄ fiche** : l'affiche voyage carte ⇄ bloc visuel (Web Animations
  API, clone unique, rayon 8→40) ; dégradation propre (mobile / `prefers-reduced-motion`).
  (commit `6b4212e`)
- **Tokens** `--radius-2xl` (40px) et `--scrim-lg` (noir 60%). (`design/tokens.json`)
- **Enrichissement backdrop + bande-annonce côté serveur** : `backdrop_path` (w1280) et
  trailer YouTube dans `scraper.py` ; colonnes `films.backdrop` / `films.trailer`
  (`supabase/migrations/004_backdrop_trailer.sql`) + mapping front (`_backdrop` / `_trailer`).
- **Contrats de composants** rédigés dans `design/DSDS.md` (anatomie, états, variantes, a11y,
  do/don't par famille `card-` / `d-` / `compact-` / `ev-`).
- **En-têtes no-cache** sur le serveur de dev (`tools/serve.py`).
- **Groupe de tokens `fontFamily`** (généré en `--font-{clé}`) et premier token
  `--font-display-italic` (Riegraf italique). Fonte `assets/fonts/Riegraf-Italic.otf` +
  `@font-face` italique. (`design/tokens.json`, `design/build_tokens.py`, `src/template.html`)

### Modifié
- **Séparateur « plus de séance aujourd'hui »** : les deux filets et le label capitales
  laissent place à une illustration (`assets/noMoreMovie.png`, 390px de haut, opacité 70%)
  surmontée d'une pilule en Riegraf italique `--fs-lg`, centrée horizontalement et calée à
  40px du bas du bloc. Libellé « Les films suivants n'ont plus de séance aujourd'hui ».
  Illustration réduite à 220px sous 820px.
  (`src/components.css`, `src/template.html`)
- **Séances de la fiche** : nom du cinéma en colonne fixe à gauche + horaires en grille de
  chips à largeur fixe (au lieu de passer sous le nom du cinéma). (`src/components.css`)
- **Images de la fiche en pleine résolution** (`/t/p/original/`) → fin du flou retina/2K ;
  vignettes de liste inchangées (w500). (commit `3ded7ac`, `src/template.html`)
- **Scroll de la liste préservé** à l'ouverture / fermeture d'une fiche. (commit `7931134`)

### Corrigé
- **Titres Comœdia mal parsés** bloquant l'enrichissement TMDB (`scraper.py`, commit `c304955`) :
  marqueur « JP » (Jeune Public) retiré (préfixe / suffixe / ligne isolée), numéros romains
  préservés dans `_titlecase_fr` (« II » ne devient plus « Ii »), note « * » finale retirée,
  aller-retour TMDB pour une note « chiffre isolé » (ex. « Memento 1 » → « Memento »).
- **Backfill `backdrop` / `trailer`** sur les films déjà en base (l'upsert ne remplissait ces
  champs qu'à la création). (commit `8028e76`, `scraper.py`)

## 2026-07-22

> Réintégration d'une **logique de design system** (code-first) + hygiène du dépôt.
> Le retrait de l'enrichissement TMDB **côté client** ne touche aucun invariant (I1–I8)
> ni contrat (C1–C4) : l'enrichissement canonique reste **côté serveur** (`scraper.py`,
> cf. [pipeline](docs/architecture/pipeline.md)). `docs/architecture/` inchangée sur le
> fond — seules les ancres de ligne ont été fiabilisées.

### Ajouté
- **Socle documentaire du design system** : `docs/design-system/STRATEGY.md` (stratégie
  code-first, pont Figma via Tokens Studio sur plan Pro, garde-fous CI/hook) et
  `AUDIT.md` (refacto priorisé). `design/DSDS.md` : convention de nommage des classes
  (3 natures — composant / état / variante ; vocabulaire des préfixes ; dette
  `d-`/`dc-`/`detail-` documentée mais **non renommée**).
- **Règle d'hygiène des dossiers** (`ARCHITECTURE.md` + README du vault Obsidian) :
  lisibilité d'un coup d'œil, *règle de trois* avant de créer un sous-dossier.
- **Regroupement des outils dev** dans `tools/` (`inspect_html.py`, `serve.py`,
  `setup_cron.ai.sh`), racine allégée de 12 → 8 fichiers (commit `52cddcf`).
- **Pipeline de tokens code-first** : `design/tokens.json` (source de vérité DTCG :
  couleurs, échelles typo/rayons, overlays/scrims/neutres, + grille spacing & breakpoints
  en doc-only), `design/build_tokens.py` (→ `src/tokens.css`), `build_ui.py` (assemble
  `src/` → `index.html`, mode `--check` pour la CI).
- **Découpage du frontend en `src/`** : `template.html` + `components.css` + `tokens.css`.
  `index.html` devient un **fichier généré** — garde-fous : bandeau « généré »,
  `.gitattributes` (`linguist-generated`), `build_ui.py --check` branché sur le déploiement Pages.
- **Système d'overlays t-shirt** : `--overlay-*`, `--scrim-*`, `--overlay-gold-*`, mini-ramp
  neutre `--neutral-*`. `docs/design-system/color-mapping.md` (table de mapping) ;
  docs de revue rangées dans `review tech/`.
- **Galerie du design system** (`design.html`) : référence vivante des tokens (couleurs par
  familles, typo, polices, rayons, spacing, breakpoints), valeurs live + notes d'usage.
  Générée par `design/build_gallery.py` (données inlinées, sans `fetch`), relancée par
  `build_ui.py`, `--check` en CI.
- **Règles d'usage des tokens** : une note (`$description`) par token dans `tokens.json`
  (affichée sur la galerie) + `DSDS.md` §2 « Règles d'usage » (familles couleur, a11y,
  anti-patterns) et §3 « Contrats de composants » (à venir).
- **Verrouillage du design system (P3)** : lint `design/check_tokens.py` (refuse couleur brute
  hors allowlist, px de police/rayon non-`var`, spacing hors grille 2px) — branché en **CI**
  (déploiement Pages) et en **hook Claude Code `PostToolUse`** (`.claude/settings.json`,
  `exit 2` → les violations reviennent à l'agent). `CLAUDE.md` : manuel de l'agent (build, règles
  DS, fichiers générés à ne pas éditer). `.claude/settings.json` désormais versionné
  (`.gitignore` : `settings.local.json` reste ignoré).

### Modifié
- **Tokenisation & échelles (`index.html`)** :
  - `:root` consolidé en un seul bloc ; littéraux CSS égaux à un token existant
    remplacés par `var(--…)` ; remplissage des tickets SVG piloté par `--resa-tkt`
    (règle CSS, plus d'attribut `fill`). *(iso-visuel)*
  - **Échelle typo : 19 → 7 tailles** — `10 · 12 · 14 · 16 · 18 · 24 · 38`. *(change le rendu)*
  - **Rayons : ~10 → 4 valeurs** — `4 · 8 · 12 · 16`, + `999px` unique pour pill **et**
    cercle (`50%`, `100px`, `40px` unifiés). `--r` = 8px. *(quasi iso ; `2/3→4`, `6/10→8` volontaires)*
  - **Breakpoints : 5 → 2** — `480` + `820/821` (`400→480`, `600→820`). *(change le rendu tablette)*
- **Ancres de doc fiabilisées** (`docs/architecture/README.md`, `frontend.md`) :
  `index.html:NNN` → ancres de symbole (`loadFromSupabase()`…), stables face au futur build.
- **Couverture des tokens** : valeurs d'échelle (font-size, border-radius) passées en
  `var(--…)` ; couleurs brutes **58 → 11 one-offs** (snapping imperceptible, ≤ .05 alpha) ;
  spacing de rythme calé sur la grille 2px (impairs `3/5/7/9` → pair) — positions
  d'animation et hairlines `1px` préservées. *(changements visuels délibérés mais imperceptibles)*
- **`index.html` désormais généré** (ne plus l'éditer à la main) ; `README.md` gagne une
  section « Build du frontend » ; `docs/architecture/frontend.md` corrigé (n'est plus « sans build »).
- **Clarification des tokens couleur** : `--text-mid`→`--text-2`, `--text-dim`→`--text-3`
  (rampe de prominence) ; `--resa-tkt`→`--gold-light` (famille Marque) ; `--compact-times`
  fondu dans `--neutral-hi`. Familles de la galerie réorganisées (Surfaces / Texte / Bordures /
  Marque / Scrims) ; scrims réduits à `sm/md/xl`.
- **`tools/serve.py` corrigé** : sert la **racine** du dépôt (servait `tools/` après le déplacement).

### Supprimé
- **Enrichissement TMDB côté client** (`index.html`) : `TMDB_KEY` (ancienne clé
  **révoquée**), les 2 `fetch` navigateur et la branche TMDB d'`enrichFilm`. Les
  métadonnées sont déjà produites côté serveur → **aucun changement de rendu** ;
  supprime une cascade de requêtes 401 à chaque chargement **et** la clé du fichier
  public. OMDb (clé démo publique) conservé comme filet d'enrichissement.
- **6 classes CSS mortes** : `.card-actors`, `.card-lang-chip`, `.compact-day-column`,
  `.ctx`, `.ctx-date`, `.m-back`.
- **Tokens couleur retirés** (avec migration) : `--accent`→`--gold`, `--white`→`--text`,
  `--scrim-lg`→`--scrim-md`, `--scrim-2xl`→`--scrim-xl`. *(changements visuels délibérés, imperceptibles)*

## 2026-07-21

> Livraison purement présentationnelle : aucun invariant (I1–I8) ni contrat
> (C1–C4) touché — `docs/architecture/` n'a pas à être répercutée.

### Ajouté
- **Logo SVG + police d'affichage des titres** (`index.html`, `assets/`). Le logo
  texte `CinéIndé Lyon` cède la place à `assets/logo.svg` (rendu en `<img>`,
  hauteur 28 px). Il est agrandi à `scale(1.38)` en haut de page et ramené à
  `scale(1)` à l'état `.scrolled` — animé en **`transform`** et non en `height`,
  pour rester sur le compositeur ; neutralisé sous `prefers-reduced-motion`.
  L'échelle étant relative, le mobile hérite du même ratio sans règle dédiée.
  Les titres de films (`.card-title` en liste, `.d-title` en fiche) passent sur
  **Riegraf** (`@font-face`, `assets/fonts/Riegraf-Bold.otf`), `'Playfair
  Display'` conservé en repli dans la pile `font-family`.
  > ⚠️ **Police en version d'essai (TRIAL)** commitée dans un dépôt **public**
  > servi par GitHub Pages — licence à acquérir. Sans le fichier, le repli
  > Playfair Display s'applique sans rien casser.

### Modifié
- **Header transparent au repos** (`index.html`). Le fond et la bordure basse du
  `.nav-shell` passent de l'aplat permanent à une opacité nulle par défaut, et
  n'apparaissent qu'à l'état `.scrolled`, sur la même courbe que le compactage de
  la toolbar (`.35s cubic-bezier(.33,0,.2,1)`) — fondu et glissement synchronisés.
- **Cartes films — affiche et rythme vertical** (`index.html`). Affiche agrandie
  de 10 % (192×288 → 211×317 px ; colonne de grille et skeleton alignés).
  Espacements du bloc texte repris à **16/8/16/8/16 px** (au-dessus du titre,
  puis entre titre, métadonnées, réalisateur, acteurs et séances) : `.card-header`
  passe en `gap:0` et chaque élément porte sa propre marge. La grille de la carte
  passe en `grid-template-rows:auto 1fr` pour que le surplus de hauteur de
  l'affiche soit absorbé **sous** les séances — sinon les rangées s'étiraient et
  gonflaient l'écart acteurs→séances à 19 px.
- **Tailles de titres** (`index.html`) : −4 px en liste
  (`clamp(24px,calc(4vw - 4px),36px)` — le `calc` garantit le −4 px sur toute la
  plage responsive, pas seulement aux bornes) ; +4 px en fiche détail
  (34→38 px desktop, 32→36 px mobile).
- **Fiche détail — bloc du jour** (`index.html`) : toutes les polices de `.d-day`
  +2 px (libellé du jour, tag cinéma, pastilles horaire et langue).
- **Pastille cinéma active** (`index.html`) : l'effet « verre » (dégradé
  spéculaire, `backdrop-filter`, triple `box-shadow`, liseré clair) est remplacé
  par un **aplat uni `#D0A636`**.
- **Bandeau des cinémas** (`index.html`) : zone réduite de 32 px (`margin:0 16px`).
- **Indice de réservation** (`index.html`) : l'emoji 🎫 cède la place à l'icône
  billet vectorielle du design system (`TICKET_SVG_HINT`, 14 px de haut), alignée
  à gauche du texte.

### Corrigé
- **Séances tronquées sur mobile** (`index.html`). Les deux lignes du bouton
  réservable (`.card-time-btn.resa`) sont positionnées en **absolu** avec des
  `top` calibrés pour la hauteur desktop (64 px) ; sur les 48 px du mobile, la
  mention de version (`VOSTFR`) débordait et se faisait couper par
  l'`overflow:hidden`, sans être centrée. Les `top` sont recalés (11 px / 29 px).
  Sans effet de bord sur l'animation Motion, qui n'anime que `y`/`opacity`.

### Supprimé
- **Repli `prefers-reduced-transparency`** de la pastille active (`index.html`),
  sans objet une fois l'effet verre retiré. Le garder aurait servi `var(--gold)`
  (#c9a84c) — donc une couleur **différente** de `#D0A636` — aux utilisateurs
  ayant réduit la transparence.

## 2026-07-20

> ⚠️ Cette livraison touche **I1**, **I5** et **C3** — la doc `docs/architecture/`
> (frontend, pipeline) reste à répercuter (voir *Exploration — Accès billetterie*
> dans le vault, section « Suite donnée »).

### Ajouté
- **Réservation deep-link billetterie** (`index.html`, `scraper.py`). Le front
  rend le lien `resa_url` de chaque séance (option A « câbler l'existant », cf.
  *Exploration — Accès billetterie*, vault). Deux composants « Réserver » révélés
  au survol : bouton créneau des cartes (l'horaire se réduit et remonte à la
  place de la langue, « Réserver » + billet montent du bas — animé avec **Motion**,
  paquet `motion` chargé en ESM) et pastille de la fiche « Séances de la semaine »
  (bascule langue → billet). Derrière le flag `ENABLE_RESA_LINKS`. **Sécurité**
  (seule défense, aucune CSP posable sur GitHub Pages) : allowlist `https` + hôte
  (`safeResaUrl` au front, `is_valid_resa_url` au scrape — rejette
  `javascript:`/`data:`/hôtes tiers), `rel="noopener noreferrer"`,
  `<meta name="referrer" content="no-referrer">`. Touche **I5**, **C3**.
- **Tri de la liste par proximité de la prochaine séance** + séparateur « Ces
  films n'ont plus de séance aujourd'hui » (`index.html`, `getRowsForDate`,
  `renderFilmsStandard`). Le film dont la prochaine séance est la plus proche de
  l'heure courante remonte en haut ; les films dont toutes les séances du jour
  sont passées sont relégués sous le séparateur.

### Modifié
- **Dédoublonnage des films à la frontière du chargement** (`index.html`,
  `dedupeFilms`). Deux passes : fusion par `(filmGroupKey, cinéma)`, puis
  rattrapage des mauvais appariements imdb_id par `(cinéma, titre normalisé,
  réalisateur compatible)`. Corrige les séances dédoublées (vue « tous ») et les
  films en double (vue filtrée) sans fusionner les homonymes réels (réalisateurs
  différents : *La Chaleur* 1938/2026, *Le Tombeau des lucioles* 1988/2005…).
- **Durcissement du matching film à l'upsert** (`scraper.py`,
  `upsert_all_to_supabase`). Index des films existants par titre normalisé :
  quand l'imdb_id manque (enrichissement intermittent) ou que la clé de repli
  brute dérive (année/réalisateur), on se rattache à la ligne existante au lieu
  d'en créer une nouvelle. Garde-fous anti-homonyme (`_years_close` +
  `_reals_compatible`) + backfill imdb_id. Empêche la ré-apparition des doublons
  et des liens de réservation périmés. Touche **I1**.
- **Repli JSON : strip de `resa_url`** (`index.html`, `loadFromJson`). Le fallback
  ne sert plus de deep-links figés (dont le `D{epoch}` pourrit avec le temps) —
  les séances dégradent en bouton informatif. Touche **I5**.

### Corrigé
- **Parser Lumière : lien de réservation lu au périmètre du `<time>`** et non du
  `<td>` (`scraper.py`, `_lumiere_parse_schedule_td`). Avant, toutes les séances
  d'un jour héritaient du lien de la 1re séance → « séance passée » sur les
  séances suivantes. Spike SP1 : chaque `<a>` cotecine est imbriqué dans son
  `<time>` (275/275) — le fix retire du code.
- **Langue restant en blanc après survol** d'une pastille réservable
  (`index.html`, animation Motion `leave`) : l'opacité de la langue n'était pas
  restaurée à sa valeur de repos (0.5).

### Supprimé
- **État « soon » (rouge/orangé)** retiré du design system (`index.html` :
  `.card-time-btn.soon`, `.d-chip.soon`, `.cw-times .t-soon`). Les séances
  imminentes s'affichent comme les autres.

_Note données (hors code) : nettoyage one-shot des doublons `films` en base
Supabase, 112 → 87 lignes (fusion imdb via `scripts/merge_duplicate_films.py`
puis fusion par titre ; 3 homonymes préservés)._

## 2026-07-10

### Modifié
- **Dédup films par `imdb_id` (identifiant canonique TMDB/OMDb), repli sur la
  clé brute** (`scraper.py`, `upsert_all_to_supabase`). Réécrit l'invariant
  **I1** : deux sources décrivant le même film avec une casse/orthographe/année
  différente créaient jusqu'ici des lignes `films` en double (mesuré : 18 titres
  dédoublés sur ~46, 100 % Comoedia+Lumière). L'`imdb_id` devient la clé de
  dédup primaire ; repli sur `(titre normalisé, annee, realisateur)` quand
  l'id manque. Garde-fou `_years_close` : on ne fusionne sur un imdb_id partagé
  que si les années restent proches (un mauvais match TMDB ne doit pas fusionner
  deux films distincts). Genèse : *Exploration — Dédup inter-sources* (vault
  Obsidian). Touche I1, C2, C3 (`docs/architecture/` mis à jour).
- **Front : regroupement « Tous les cinémas » par `imdbId`** (`index.html`,
  nouveau `filmGroupKey`, utilisé par `getRowsForDate` et `openFilm`). Aligne
  l'affichage sur la dédup back : fusionne les variantes de casse mais **sépare
  les vrais homonymes** (2 imdbId ≠ → 2 cartes, ex. *La Chaleur* 1938 vs 2026).
  Avant, `normalizeTitle` groupait par titre seul et fusionnait à tort les
  homonymes (bug d'affichage latent).

### Ajouté
- **Migration `003_dedup_imdb_id.sql`** : index unique **partiel**
  `films_imdb_id_key` sur `imdb_id` (où non nul) — filet de sécurité DB de la
  dédup I1. ⚠️ À appliquer **après** le script de fusion (l'index échoue tant
  que des doublons subsistent en base).
- **Script `scripts/merge_duplicate_films.py`** : fusion one-shot des films déjà
  dédoublés en base par `imdb_id` (réassigne les séances vers la ligne
  canonique — la plus complète —, supprime les surnuméraires). **Dry-run par
  défaut**, `--apply` pour exécuter, garde-fou `--year-tol`. À lancer sur la
  prod **avant** la migration 003.
- **Le Zola (Villeurbanne) — 5e cinéma de la plateforme** (`scraper.py`,
  `index.html`). Nouveau module `scrape_zola()` : index `/films-a-laffiche/` →
  fiches `/movies/{slug}/` (WordPress rendu serveur, sélecteurs documentés en
  tête du module). ~12 films, horaires en carrousel ~15 jours, liens de
  réservation TicketingCiné par séance (URL stable, pas de token volatil).
  `annee`/`realisateur` volontairement laissés à `None` pour la convergence de
  la clé de dédup (invariant I2) — vérifié sur 4 films partagés Zola↔Lumière
  cette semaine (In Waves, Jim Queen, Disclosure Day, Entroncamento : 1 seule
  ligne `films` chacun après propagation). Flag `--no-zola`. Frontend : pill,
  libellés, section « LE ZOLA », texte à-propos.
- **Garde-fou Comoedia : Zola exclu de la preuve « semaine publiée »**
  (`count_week_seances` : `exclude_slug` → `exclude_slugs` liste). Le Zola
  publie ~15 jours en avance : le compter comme « quelqu'un a publié » aurait
  déclenché des `exit 4` accusant Comoedia à tort (piège B du challenge Zola).
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
