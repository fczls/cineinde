# DSDS — Design System, Documentation & Standards

> Le contrat du design system CineInde : principes, conventions, et règles que le code (et l'agent qui l'écrit) doit respecter. Complément de [STRATEGY.md](../docs/design-system/STRATEGY.md) et [AUDIT.md](../docs/design-system/AUDIT.md).
>
> Ce document grandira par sections. La première, ci-dessous, fixe le **nommage** — parce que c'est la règle la plus souvent enfreinte quand on ajoute un composant.

---

## 1. Convention de nommage des classes CSS

### L'idée en une phrase

Le **nom d'une classe dit trois choses** : à quel composant elle appartient, quelle partie de ce composant elle habille, et dans quel état il se trouve. Un lecteur doit pouvoir répondre à « c'est quoi, ça ? » rien qu'en lisant le nom.

### Les trois natures d'un nom

Il existe exactement trois familles de classes. Ne pas les mélanger.

| Nature | À quoi ça sert | Forme | Exemple |
|---|---|---|---|
| **Composant** | Nomme un bloc réutilisable et ses parties | `bloc` puis `bloc-partie` | `.card`, `.card-title`, `.card-poster` |
| **État** | Décrit une condition temporaire (souvent posée par le JS) | préfixe `is-` ou `has-` | `.is-active`, `.has-trailer` |
| **Variante** | Un mode d'affichage alterné du même composant | suffixe après `--` | `.card--compact` |

Règle de lecture rapide :

- un tiret simple (`card-title`) = **une partie** d'un composant ;
- `is-` / `has-` au début = **un état** ;
- double tiret (`card--compact`) = **une variante**.

### Le vocabulaire des composants (préfixes canoniques)

Ces préfixes existent déjà dans le code et forment le vocabulaire officiel. **Un nouveau composant réutilise un préfixe existant s'il en relève, sinon en crée un nouveau — jamais un synonyme d'un préfixe déjà là.**

| Préfixe | Composant | Exemple |
|---|---|---|
| `card-` | Carte film (vue détaillée) | `.card-poster`, `.card-slots` |
| `compact-` | Ligne film (vue liste) | `.compact-row`, `.compact-title` |
| `cw-` | Grille de la semaine | `.cw-grid`, `.cw-times` |
| `d-` | Panneau de détail | `.d-hero`, `.d-synopsis` |
| `ev-` | Événement | `.ev-date`, `.ev-desc` |
| `day-` | Navigation par jour | `.day-nav-btn` |
| `nav-` | En-tête / barre flottante | `.nav-shell` |
| `resa-` | Réservation (deep-link au survol) | `.resa-tkt` |

### La règle quand on ajoute une classe

Avant d'écrire une nouvelle classe, se poser ces questions dans l'ordre :

1. **Est-ce une partie d'un composant existant ?** → `préfixe-partie` (ex. une nouvelle ligne dans la carte = `.card-badge`).
2. **Est-ce un état ?** → `.is-…` ou `.has-…`, jamais un préfixe de composant.
3. **Est-ce une variante d'un composant existant ?** → `.bloc--variante`.
4. **Est-ce un composant vraiment nouveau ?** → nouveau préfixe court et parlant, ajouté au tableau ci-dessus **dans la même modification**.

Si aucune ne s'applique proprement, c'est le signe qu'il faut clarifier le design avant de coder — pas inventer un nom au hasard.

### ⚠️ Dette connue (à ne PAS reproduire, mais à ne pas renommer non plus)

Le panneau de détail est aujourd'hui désigné par **trois préfixes concurrents** : `d-`, `dc-`, et `detail-`. C'est une incohérence héritée.

- **Convention officielle : `d-`** pour tout ce qui touche le panneau de détail.
- On **ne renomme pas** l'existant (risque élevé, gain purement cosmétique, et le rendu doit rester identique).
- Mais **tout nouveau code** utilise `d-`. `dc-` et `detail-` sont gelés : on ne les étend pas.

C'est le principe général de cette dette : *documenter la bonne règle, l'appliquer au neuf, laisser le vieux tranquille tant qu'on n'a pas une raison forte d'y toucher.*

### Pourquoi cette section existe

Sans convention écrite, chaque nouveau composant — surtout quand c'est un agent qui l'écrit — invente sa propre logique de nommage. Au bout de quelques itérations, on se retrouve avec `d-`, `dc-` et `detail-` pour la même chose. Cette page est là pour que ça n'arrive plus : elle donne **une** réponse à « comment je nomme ça ? ».

## 2. Règles d'usage des tokens

> **Deux sources, complémentaires.** L'usage *court* de chaque token (« quand l'utiliser ») vit dans `design/tokens.json` (champ `$description`) et s'affiche sur la galerie `design.html` — c'est la source unique, à jour automatiquement. Cette section couvre ce qui **ne tient pas en une ligne** : combinaisons, accessibilité, anti-patterns.

### Couleur

- **Deux familles, à ne pas mélanger.** La palette de marque est **chaude** (`--text-*`, `--gold*`, bruns). Les gris **neutres** ont leur propre rampe (`--neutral-lo/mid/hi`). Un gris neutre ne se remplace pas par un token chaud (décalage de teinte visible), et inversement.
- **`--gold` se dose.** C'est l'accent de marque : état actif, note, élément clé. Pas de doré décoratif partout — sinon il ne veut plus rien dire. Pour un doré discret au repos → `--gold-dim`.
- **Scrims vs overlays.** `--scrim-*` (noir) = assombrir : ombres portées, fonds de modale/hero. `--overlay-*` (blanc) = éclaircir légèrement : fills et survols subtils. Ne pas détourner l'un pour l'autre.
- **Bordures.** `--border` par défaut, `--border2` pour survol/actif. Ne pas réinventer une bordure en `rgba(255,255,255,.X)` brut.

### Accessibilité

- **Hiérarchie de texte = hiérarchie de contraste.** `--text` pour l'essentiel, `--text-2` pour le secondaire, `--text-3` pour l'accessoire. **Ne pas** mettre une information essentielle en `--text-3` sur petit corps (`--fs-xs/sm`) : contraste trop faible.
- Vérifier le contraste d'un texte doré (`--gold`) sur fond clair avant de l'utiliser hors accent.

### Typographie & rayons

- **`--fs-3xl` = la taille du titre dominant**, un rôle par vue : le titre de la fiche film, ou celui de l'évènement mis en avant dans la sélection. Ne pas empiler deux tailles display dans un même bloc. *(La formulation précédente le réservait au titre de fiche — l'onglet Évènements a un titre dominant qui n'est pas une fiche.)*
- **Familles (`--font-*`) — la police d'affichage ne s'écrit jamais en clair.**
  - `--font-display` = **Riegraf romain**, pour **tous les titres sans exception** (fiche film, évènement, section, modale).
  - `--font-display-italic` = Riegraf italique, voix éditoriale (étiquettes de section, mentions). Toujours accompagné de `font-style:italic` — c'est ce qui déclenche la fonte italique *et* aligne le repli.
  - ⚠️ **« Playfair Display » est un REPLI de chargement, pas un choix typographique.** Un titre qui l'écrit en dur rate la police de marque, en silence et pour toujours. Le lint refuse désormais toute famille littérale (`'Playfair Display'`, `'Riegraf'`).
  - ⚠️ **Riegraf n'a qu'une graisse.** Poser `font-weight:normal` : un `700` déclenche un gras synthétique par-dessus une fonte déjà grasse (et c'est ce qui faisait retomber certains titres sur le repli).
- **Un composant = un palier de rayon cohérent.** `--radius-pill` uniquement pour du pleinement arrondi (pills, boutons carrés → cercle).

### Espacements

- **Grille 2px, valeurs paires uniquement.** Se caler sur `--sp-*` (documentés dans `tokens.json`). Les impairs (3, 5, 7…) sont des accidents — le lint P3 les refusera. Exceptions assumées : positions précises d'animation et hairlines `1px`.

### Anti-patterns (ce que le lint P3 bloquera)

- ❌ Valeur brute (`#hex`, `rgba()`, `px` de police/rayon/spacing) au lieu d'un `var(--…)` — sauf les one-offs documentés (voir `color-mapping.md`).
- ❌ Éditer `index.html` ou le bloc `:root` (générés). On édite `src/` / `design/tokens.json` puis `python3 build_ui.py`.
- ❌ Créer un token pour une valeur utilisée une seule fois — d'abord se demander si un token existant convient (règle des deux).

## 3. Contrats de composants

> Un **contrat** décrit ce qu'un composant garantit : son anatomie (parties), ses états, ses variantes, ses règles d'accessibilité, et ses do/don't. Les composants **montrables** sont rendus en vrai dans `design.html` (section Composants) ; les **complexes** (dépendants du contexte de l'app) sont documentés ici mais pas rendus isolés.

### Carte film — `card-`  *(montrable)*

- **Anatomie** : `.film-card` › `.card-poster-wrap`>`.card-poster` · `.card-body`>`.card-header`>(`.card-title`, `.card-meta`, `.card-director`, `.card-cast`) · `.card-slots`>`.card-slot-row`>(`.card-cinema-chip`, boutons créneau).
- **États** : `.active` (fiche ouverte) ; mise en avant au survol.
- **A11y** : la carte entière ouvre la fiche (`onclick`). Les créneaux réservables sont des `<a>`/`<button>` internes qui **stoppent la propagation** — sinon un clic sur « Réserver » ouvrirait aussi la fiche.
- **Do** : un seul titre (`.card-title`, `--fs-3xl`/Riegraf). **Don't** : imbriquer une action cliquable sans `stopPropagation`.

### Bouton créneau — `.card-time-btn`  *(montrable)*

- **Anatomie** : horaire + `.slot-ver` (version : VF/VOSTF).
- **États** : défaut · `.past` (atténué, non réservable) · `.soon`.
- **Variante `.resa`** : deep-link billetterie, rendu en `<a>` avec `.rz-time`/`.rz-ver`/`.rz-lbl`/`.rz-tkt-wrap` ; au survol l'horaire se réduit et « Réserver » + billet apparaissent (Motion, repli CSS sous `html:not(.motion-ready)`).
- **A11y** : variante `.resa` = `<a>` avec `aria-label` « Réserver {heure} — {film} », `target=_blank` + `rel=noopener`. Sinon `<button>` qui ouvre la fiche.
- **Do** : n'afficher « Réserver » **que** si un lien valide existe (invariant I5 — pas de fausse promesse). **Don't** : un `<a>` sans href réservable.

### Puce cinéma — `.card-cinema-chip`  *(montrable)*

Étiquette de salle devant une rangée de créneaux, en mode « Tous les cinémas ». Décorative (pas d'interaction).

### Pills de filtre — `.c-pill`  *(montrable)*

- **États** : `.active` — **exclusif** (une seule à la fois).
- **A11y** *(dette à corriger)* : ajouter `aria-pressed`/`aria-current` sur l'actif.

### Ligne liste — `compact-`  *(montrable)*

- **Anatomie** : `.compact-row` › `.compact-title` (CAPITALES) · `.compact-poster-slot` (affiche au survol) · `.compact-times` (horaires condensés).
- **États** : `.active`. La ligne ouvre la fiche.

### Événement — `ev-`  *(montrable)*

Le préfixe couvre **tout l'onglet Évènements**, des deux niveaux. Réécrit le 2026-07-29 (l'ancienne carte `.ev` à colonne de date a disparu avec les données figées).

- **Colonne de contenu** : `.ev-col`, `max-width:820px`, centrée. Le desktop est un **centrage doublé d'un élargissement**, pas un passage multi-colonnes : le chrome (barre cinémas, toolbar) reste pleine largeur et la liste reste **mono-colonne à toutes les largeurs**.
- **Résumé — `.ev-summary`** : segments `.ev-seg-strong` (`--text`) / `.ev-seg-mute` (`--text-3`) et pastilles `.ev-ico` (cercle `--text`, glyphe `--bg`). **Do** : n'accepter qu'une icône de la liste fermée `EV_ICONS`. **Don't** : styler un segment autrement que par ces deux classes — le rendu multicolore doit rester déterministe.
- **Étiquette de section — `.ev-label`** (« Sélection », « Toute la programmation du mois ») : pill en `--font-display-italic` + `font-style:italic`, la voix éditoriale du système. Réservée aux respirations narratives.
- **Éventail — `.ev-fan` › `.ev-card`** : trois niveaux de profondeur (`data-niveau` 0/1/2, `data-cote` -1/0/1), décalages et rotation en variables (`--ev-dx1`, `--ev-dx2`, `--ev-rot`), opacité 70 % au niveau 2. Affiche centrale : une seule dimension fixée (le ratio est préservé) ; cartes latérales **carrées** en `object-fit:cover`, `object-position:top` sur les portraits (préserve le bloc-titre de l'affiche), `center` sur les paysages.
  **Do** : n'animer que `transform`/`opacity`, et respecter `prefers-reduced-motion`. **Don't** : masquer une carte dont l'image casse — le **nombre** de cartes suit une règle produit, il ne doit pas dépendre d'un 403 (repli sur le visuel local).
  **État `.is-solo`** (un seul évènement éligible) : ni cartes latérales, ni flèches, ni swipe.
- **Ligne de liste — `.ev-row`** : `[.ev-chip] [.ev-cat] [.ev-sep-dot] [.ev-row-titre] [.ev-rule] [.ev-plus-sm]`. L'étiquette de catégorie se lit **avant** le titre : c'est une information structurante, lue à chaque item. Le `+` est en absolu à droite pour rester sur la première ligne quand le titre passe à la ligne (mobile).
- **Niveau 2** : réutilise la grammaire deux blocs de la fiche (`.d-hero` + `.d-body`) ; seules les parties propres portent `ev-` — `.ev-pills` (`.ev-pill-cine` en aplat `--text`, puis type et forme en contour), `.ev-films` › `.ev-film`. **Do** : `.ev-film-etat.is-inactif` (`--text-3`) pour « Séance passée » / « Séances non encore annoncées » — une information, pas une affordance. **Don't** : afficher un `+` sans destination.
- **A11y** : `:focus-visible` doré sur les cartes, flèches et `+` ; flèches ‹ › au clavier quand l'éventail a le focus.

---

### Composants complexes *(doc-only — pas rendus isolés)*

- **Barre de nav — `.nav-shell`** : `position:fixed`, logo qui rétrécit au scroll (`.scrolled`, animé en `transform`). Dépend du scroll.
- **Toolbar — `tb-`** : au scroll, textes → icônes, tout réduit à la hauteur du bouton À propos (48px). Dépend du scroll + JS.
- **Grille semaine — `cw-`** : tableau films × jours ; dédup d'affichage ; deep-link **par séance** (invariant I5/contrat C3). Dépend des données.

#### Fiche détail — `d-` / `detail-`

Ouverte via `.detail-open` sur `<body>` + `.has-detail` sur `.app`. Dépend de l'état global + JS.

- **Desktop (≥821px) — overlay plein écran, deux blocs.** `.app.has-detail` passe en flex et masque la liste (`.main{display:none}`) et le backdrop. Trois enfants : `.detail-visual` (visuel, gauche) · `#detailClose` (croix, centrée verticalement) · `.side` (détail, droite). Marges et gaps de 16px, hauteur `100vh`, coins `--radius-3xl`, liseré `--border`.
- **Largeurs (3 phases)** : détail figé à 510px puis élastique puis figé à 820px — `flex:0 0 clamp(510px, (100vw - 112px)/2, 820px)` ; le visuel est en `flex:1` plafonné à 820px, l'ensemble se centrant une fois les deux plafonds atteints. `112` = marges + gaps + largeur du bouton fermer : **le recalculer si l'une de ces valeurs bouge**.
- **Bloc visuel** : `.detail-visual` › `.dv-img` (affiche, `object-fit:cover`) [+ `.dv-scrim` = voile `--scrim-lg` **uniquement** sur l'image de repli]. Vidé/rempli par `renderDetail()`.
- **Bloc détail** : `.side` › `.detail-wrap` (scroll interne) › `.d-hero` + `.d-body` (padding 24px). Contient la pastille réservation (`.d-chip.resa` + `.dc-tkt`) qui morphe au survol.
- **Séances** : `.d-day-row` en grille `100px 1fr` (cinéma en colonne fixe) ; `.d-chips` en grille de chips à **largeur fixe** (140px) qui passent à la ligne. **Don't** : `1fr` sur les chips — elles s'étireraient à la largeur de la colonne.
- **Mobile (≤820px)** : même grammaire deux blocs que le desktop, empilés. `.side.open` occupe la vue avec 8px de padding et `overflow:hidden` ; `.d-hero` (visuel, sans dégradé) et `.d-body` (contenu, padding 16px) portent chacun `--radius-2xl` + liseré `--border`, séparés par 8px. **Le panneau ne scrolle pas** : chaîne flex `.side.open` › `.detail-wrap` › `#detailContent` (avec `min-height:0` à chaque maillon) pour que seul `.d-body` scrolle, son bas restant dans la vue. **Don't** : oublier un `min-height:0` — le bloc contenu reprend sa hauteur naturelle et sort de l'écran. Fermeture par `.d-back-btn`, backdrop actif.
- **Visuel réduit au scroll (mobile) — `.hero-min`** : passé ~48px de scroll dans `.d-body`, `.d-hero` passe de `--hero-h` (200px) à `--hero-min-h` (56px) et devient une barre pleinement arrondie ; la croix se cale à 4px du bord gauche, la bande-annonce quitte le centre pour 4px du bord droit (les deux à 48px de haut, l'image reste nette — pas de flou en V1). Motion écrit hauteur / rayon / transforms en inline ; les règles `html:not(.motion-ready)` ne sont que le repli. **Do** : garder les deux hauteurs dans `--hero-h` / `--hero-min-h` — le JS les relit. **Don't** : réduire quand le gain de hauteur suffirait à annuler le scroll (garde `room > gain + seuil`), sinon le scrollTop est écrêté et le bloc rebondit entre les deux états.
- **Info réservation — `.d-resa-info`** : pastille pleine largeur sous le label « Séances de la semaine ». Billet dégradé (même SVG que la vue liste) dans `.d-resa-ico` — 40px desktop / 56px mobile, le billet gardant ses 24×20 — puis la consigne (`--text-2`) et la restriction Comœdia (`--text-3`, italique) qui passe à la ligne quand la largeur manque. Conteneur : padding 4px, `--radius-pill`, liseré `--border2`.
- **Transition liste ⇄ fiche** : l'affiche voyage entre `.card-poster` et `.dv-img` (FLIP — clone `position:fixed` animé via la Web Animations API, géométrie + rayon `--radius-md` → `--radius-3xl`). **Do** : garder la même image des deux côtés (sinon le morph n'a plus de sens). Repli automatique en ouverture instantanée si mobile / `prefers-reduced-motion` / API absente. Le scroll de la liste est mémorisé à l'ouverture et restauré à la fermeture.

⚠️ Rappel §1 : `d-`/`dc-`/`detail-` coexistent — n'étendre que `d-`. **Dette ajoutée en 2026-07-25** (refonte deux blocs) : `.detail-visual` étend `detail-`, et `.dv-img`/`.dv-scrim` introduisent un préfixe `dv-` de plus. À renommer en `d-visual*` lors d'une passe de nettoyage — ne pas prendre ces noms pour modèle.
