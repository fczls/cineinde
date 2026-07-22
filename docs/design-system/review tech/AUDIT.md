# Audit de refacto — préalable au design system

> 21/07/2026 · Périmètre : `index.html` (2 591 l.) + hygiène repo · Complément à [STRATEGY.md](STRATEGY.md)

---

## 🔴 P0 — Hors sujet DS, mais bloquant : secret exposé

Le fichier `Untitled` à la racine est **tracké par git** et contient :

```
TMDB_API_KEY=v6235c8724e2a2574a056a230f6a271ff
```

Commit d'origine : `b8706e9` · Remote : `github.com/fczls/cineinde` (public, GitHub Pages actif).

**La clé est publiquement lisible dans l'historique.** `git rm` seul ne suffit pas — elle reste dans les commits antérieurs.

Actions, dans l'ordre :

1. **Révoquer et régénérer** la clé TMDB (elle doit être considérée comme compromise, pas comme « à retirer »)
2. `git rm Untitled` + ajouter `Untitled` au `.gitignore`
3. Purger l'historique (`git filter-repo --path Untitled --invert-paths`) puis force-push
4. Vérifier qu'aucun autre fichier tracké ne porte de secret

À traiter avant toute autre chose. Le reste du rapport peut attendre ; pas ça.

---

## Synthèse DS

| # | Constat | Sévérité | Effort | Phase |
|---|---|---|---|---|
| 1 | Couleurs en dur dans le **JS** — invisibles à un lint CSS | 🟠 Haute | M | P1 |
| 2 | Tokens existants **redéfinis en littéral** à côté d'eux-mêmes | 🟠 Haute | S | P1 |
| 3 | 19 tailles de police, 13 rayons, 5 breakpoints — aucune échelle | 🟠 Haute | M | P1 |
| 4 | `:root` fragmenté en 2 blocs | 🟡 Moyenne | S | P1 |
| 5 | La doc référence des **numéros de ligne** d'`index.html` | 🟡 Moyenne | S | P2 |
| 6 | Conventions de nommage mélangées (structure / état / variante) | 🟡 Moyenne | M | P2 |
| 7 | 7 classes CSS mortes | 🟢 Basse | S | P1 |
| 8 | Fichiers de travail trackés (`pdf.html`, `inspect_html.py`) | 🟢 Basse | S | P0 |

**Bonne nouvelle d'ensemble** : seulement **6 `!important`** sur 716 lignes de CSS. Il n'y a pas de dette de spécificité — le refacto est une opération de *normalisation de valeurs*, pas de démêlage de cascade. C'est le scénario le plus favorable.

---

## 1 — 🟠 Les couleurs vivent aussi dans le JS

**16 valeurs hex** hors du bloc `<style>`, dont deux catégories bien distinctes :

| Type | Exemple | Enjeu |
|---|---|---|
| **Palette de genres** (8 couleurs) | `color:"#2a3040"`, `color:"#1e1608"`… | C'est un **jeu de tokens de données** à part entière, non déclaré comme tel |
| **SVG inline** | `fill="#CFBFAF"` dans `TICKET_SVG_DETAIL` / `TICKET_SVG_HINT` | Duplique `--resa-tkt`, qui vaut exactement `#CFBFAF` |

Et **15 sites `innerHTML`** génèrent du markup en JS — le CSS n'est donc pas le seul lieu où la couche présentation se décide.

**Conséquence sur la stratégie** : un lint qui ne surveille que `src/components.css` laisserait passer la moitié du problème.

**Correctifs :**

- La palette de genres devient un groupe DTCG (`color.genre.*`) → généré à la fois en CSS custom properties **et** en constante JS par `build_tokens.py`. Une source, deux sorties.
- Les SVG inline utilisent `fill="currentColor"` et héritent du token via CSS. Supprime la duplication à la racine.
- `check_tokens.py` couvre `src/**.css` **et** `src/app.js`.

---

## 2 — 🟠 Les tokens sont contournés par leur propre valeur

Cas les plus nets :

| Token existant | Valeur | Écrit en littéral ailleurs |
|---|---|---|
| `--border` | `rgba(255,255,255,.07)` | 2× |
| `--border2` | `rgba(255,255,255,.13)` | 1× |
| `--resa-tkt` | `#CFBFAF` | 5× (dont 4 en JS) |
| `--r` | `10px` | 3× |

**`--r` est utilisé 2 fois sur 33 déclarations de `border-radius`.** Le token existe, il est simplement ignoré.

Ce n'est pas un problème d'architecture — c'est ce que le hook Claude Code corrige structurellement. Ces cas sont du remplacement mécanique, à faire en premier : rendement immédiat, risque nul.

---

## 3 — 🟠 Aucune échelle : trois axes à rationaliser

### Typographie — 19 tailles distinctes

```
8 · 9 · 10 · 11 · 11.5 · 12 · 13 · 14 · 15 · 16 · 17 · 18 · 24 · 26 · 28 · 36 · 38 px
+ clamp(24px, calc(4vw - 4px), 36px)
```

`11.5px` est presque certainement un accident. `8`/`9` et `17`/`18` sont des paires non distinguables à l'œil.
→ **Cible : 7 pas** (`xs 10 · sm 12 · base 14 · md 16 · lg 18 · xl 24 · 2xl 36`).

Aussi : `font-weight: normal` (3×) coexiste avec `400` (4×) — même valeur, deux orthographes.

### Rayons — 13 valeurs, dont un doublon d'intention

`100px` (6×) et `999px` (4×) expriment **la même chose** : une pilule. Deux conventions pour un seul concept.
→ **Cible : 4 pas** (`sm 6 · md 10 · lg 16 · pill 999`) + `circle: 50%`.

### Breakpoints — 5 valeurs pour 3 intentions

`400` · `480` · `600` · `820` · `821` px, répartis sur 10 media queries.

Trois seuils « petit écran » différents, sans logique documentée. `820`/`821` est le couple mobile/desktop cohérent ; le reste est de l'ajustement ponctuel accumulé.
→ **Cible : 3 tokens** (`sm 480 · md 820 · lg 1200`), les cas restants traités en `clamp()` plutôt qu'en palier.

### Notation

`rgba(0,0,0,0.20)` et `rgba(255,255,255,.04)` cohabitent — décimales tantôt préfixées, tantôt non. À normaliser au passage (gratuit si scripté).

---

## 4 — 🟡 `:root` fragmenté

Deux blocs : l. 17 (les 17 tokens principaux) et l. 562 (`--resa-line`, `--resa-tkt`, isolés au milieu du fichier, près du composant qui les consomme).

L'intention est bonne — colocation token/composant — mais elle contredit le principe de source unique. Ces deux tokens rejoignent `tokens.json` comme les autres.

---

## 5 — 🟡 La doc pointe des numéros de ligne d'`index.html`

`docs/architecture/` contient **11 références** de forme `index.html:1127`, `index.html:1161`.

`index.html` devenant un fichier généré, ces ancres deviennent **fausses au premier build**. C'est le coût caché de l'étape de build, et il est réel : cette doc est manifestement à jour et utile.

**Correctif (P2)** : remplacer les ancres de ligne par des ancres de fichier source — `src/app.js › loadFromSupabase()`. Plus stable de toute façon, y compris sans build step.

---

## 6 — 🟡 Conventions de nommage mélangées

Une vingtaine de préfixes, relevant de **trois natures différentes** sans distinction typographique :

| Nature | Exemples | Nombre |
|---|---|---|
| Famille de composant | `card-` (49), `d-` (61), `compact-` (36), `cw-` (26), `ev-` (10) | ~11 |
| État | `has-` (16), `active-` (8), `scrolled` | ~4 |
| Variante technique | `motion-` (17), `rz-` (15) | ~2 |

Par ailleurs `d-`, `dc-`, `detail-` désignent tous les trois le panneau détail.

Pas urgent — mais c'est exactement le genre de règle que `DSDS.md` doit fixer, sinon Claude Code inventera sa propre convention à chaque nouveau composant. **Ne pas renommer l'existant** (risque élevé, gain cosmétique) : documenter la convention et l'appliquer au neuf.

---

## 7 — 🟢 Classes mortes

Définies en CSS, jamais référencées dans le markup ni le JS :

`card-actors` · `card-lang-chip` · `compact-day-column` · `ctx` · `ctx-date` · `m-back`

À supprimer en P1. (`otf` remonté par le détecteur est un faux positif : `.otf` provient du `src:url(…Riegraf-Bold.otf)` de la `@font-face`.)

---

## 8 — 🟢 Fichiers de travail trackés

| Fichier | Statut |
|---|---|
| `Untitled` | 🔴 secret — voir P0 |
| `pdf.html` (17 Ko) | Tracké, semble être un échantillon de debug |
| `inspect_html.py` | Outil de debug, documenté dans le README — à déplacer dans `scripts/` |
| `serve.py` + `.claude/serve.py` | Deux serveurs de dev, dont un dans un dossier gitignoré |

---

## Impact sur la stratégie

Trois ajustements à [STRATEGY.md](STRATEGY.md) :

1. **P0 sécurité** s'insère avant tout le reste.
2. **`build_tokens.py` produit deux sorties**, pas une : `tokens.css` **et** `tokens.js` (palette de genres). Le périmètre de `check_tokens.py` s'étend au JS.
3. **P2 inclut la reprise des ancres de doc** — sinon l'étape de build casse une documentation vivante.

Ce qui ne change pas : le diagnostic reste « design system commencé puis contourné ». L'absence de dette de spécificité (6 `!important`) confirme que P1 est un travail de substitution mécanique, largement automatisable, à rendu iso-visuel.
