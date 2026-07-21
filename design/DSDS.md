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
