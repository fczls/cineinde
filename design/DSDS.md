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

- **Un seul `--fs-3xl` par vue** (taille display, réservée au titre de fiche). Ne pas empiler plusieurs tailles display.
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

- **Anatomie** : `.ev` › `.ev-date`(`.ev-n`/`.ev-m`) · `.ev-info`(`.ev-type`, `.ev-title`, `.ev-desc`, `.ev-meta`).
- **Do** : `.ev-type` coloré via un **token** (`--gold` par défaut). **Don't** : couleur brute en style inline.

---

### Composants complexes *(doc-only — pas rendus isolés)*

- **Barre de nav — `.nav-shell`** : `position:fixed`, logo qui rétrécit au scroll (`.scrolled`, animé en `transform`). Dépend du scroll.
- **Toolbar — `tb-`** : au scroll, textes → icônes, tout réduit à la hauteur du bouton À propos (48px). Dépend du scroll + JS.
- **Grille semaine — `cw-`** : tableau films × jours ; dédup d'affichage ; deep-link **par séance** (invariant I5/contrat C3). Dépend des données.
- **Panneau détail — `d-`** : ouvert via `.detail-open` sur `<body>`, backdrop cliquable, ancré à droite (desktop). Contient la pastille réservation (`.d-chip.resa` + `.dc-tkt`) qui morphe au survol. Dépend de l'état global + JS. ⚠️ Rappel §1 : `d-`/`dc-`/`detail-` coexistent — n'étendre que `d-`.
