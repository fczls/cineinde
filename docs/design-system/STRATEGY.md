# Stratégie Design System — CineInde

> Statut : proposition · Décidé le 21/07/2026 · Plan Figma : **Professional**

---

## 1. Audit de l'existant

Mesuré sur `index.html` (2 591 lignes, dont 716 de CSS entre les lignes 10 et 726).

| Indicateur | Valeur | Lecture |
|---|---|---|
| Custom properties définies | **19** | Une base existe déjà (`--bg`, `--text`, `--gold`, `--accent`…) |
| Occurrences de hex bruts | **75** | Les tokens sont contournés 4× plus souvent qu'utilisés |
| Doublons de casse | `#A09080` / `#a09080` | Symptôme classique de copier-coller sans référentiel |
| Valeurs `px` en dur | **503** | Aucune échelle d'espacement ni de rayon |
| `font-size` distincts | **18** | Pour un site à 3 vues — échelle typographique inexistante |
| Classes CSS distinctes | **119** | ~11 familles de composants, pas 6 |

**Conclusion de l'audit** : le problème n'est pas l'absence de design system, c'est un design system commencé puis abandonné. La couche tokens existe mais n'a jamais été rendue contraignante. Reconstruire à partir de zéro serait une erreur ; il faut **finir** et **verrouiller**.

### Familles de composants identifiées

`card-*` · `compact-*` · `cw-*` (grille semaine) · `d-*` (panneau détail) · `ev-*` (événements) · `day-*` (navigation) · `cinema-strip` · `about-modal` · `c-pill` · `ctx-*` · `empty-day-*`

---

## 2. Contraintes qui structurent la solution

### 2.1 Le pont Figma est limité par le plan Professional

| Mécanisme | Disponibilité | Verdict |
|---|---|---|
| API REST Variables (écriture) | Enterprise uniquement | ❌ Hors de portée |
| Code Connect | Organization / Enterprise + siège Full ou Dev | ❌ Hors de portée |
| Tokens Studio (plugin gratuit, sync GitHub) | Tous plans, mono-fichier | ✅ **Voie retenue** |

**Conséquence directe** : sans Code Connect, il n'existe aucun lien machine entre un composant Figma et son implémentation. La **galerie de composants devient le contrat de référence** — ce n'est pas un confort de documentation, c'est le seul artefact qui tient ce rôle.

### 2.2 Le principe « single-file » est préservé en sortie, pas en source

`index.html` reste un fichier unique et autonome servi par GitHub Pages. Il devient un **artefact généré**. L'édition se fait sur des sources découpées.

### 2.3 Le risque principal est l'agent, pas le designer

Le workflow est : scoping avec Claude → implémentation avec **Claude Code**. Le premier vecteur de dérive est un agent qui écrit `#1a1a1a` dans un nouveau composant. **Le design system doit être lisible par la machine avant d'être lisible par l'humain.**

Bonne nouvelle : Claude Code dispose de deux leviers d'application absents d'un éditeur classique — `CLAUDE.md` chargé automatiquement en contexte, et des **hooks** capables d'interrompre la boucle d'écriture. Voir section 5.

---

## 3. Architecture cible

```
design/
  tokens.json          DTCG · 2 tiers (primitive → semantic) · SOURCE DE VÉRITÉ
  DSDS.md              Principes, naming, contrat de chaque composant
  build_tokens.py      tokens.json → src/tokens.css
  gallery.html         Galerie générée, publiée sur /cineinde/design/

src/
  tokens.css           GÉNÉRÉ — ne jamais éditer
  components.css       Les 11 familles, une section par famille
  app.js               Logique applicative
  template.html        Squelette avec marqueurs <!--CSS--> / <!--JS-->

build_ui.py            src/* → index.html
index.html             GÉNÉRÉ — ne jamais éditer
```

### Flux de vérité — unidirectionnel

```
design/tokens.json
   ├─→ build_tokens.py → src/tokens.css ─┬─→ build_ui.py → index.html   (prod)
   │                                     └─→ design/gallery.html        (référence)
   └─→ Tokens Studio (sync GitHub) ──────→ Variables Figma
```

Figma est **consommateur**, jamais émetteur. Une seule direction = aucun conflit de merge, aucune question de « qui a raison ».

---

## 4. Décisions techniques et arbitrages

| Décision | Alternative écartée | Raison |
|---|---|---|
| **Python** pour le transform (~40 lignes) | Style Dictionary v4 | Le repo n'a aucune dépendance npm. Une seule sortie (CSS vars) ne justifie pas un runtime Node dans la CI. |
| **2 tiers** de tokens (primitive → semantic) | 3 tiers (+ component) | 11 familles ne justifient pas une couche component-level. À ajouter le jour d'un thème clair. |
| **Galerie sur le déploiement Pages existant** (`/design/`) | Storybook, site séparé | Consomme le **même** `src/tokens.css` que la prod. Une galerie avec son propre CSS ment. |
| **Règle des deux** : un pattern devient composant à sa 2ᵉ utilisation | Composantiser d'emblée | Évite de construire une bibliothèque pour un site à 3 vues. |
| **Tokens Studio mono-fichier** | Multi-fichier (payant) | `tokens.json` unique suffit largement à cette échelle. |

---

## 5. Garde-fous Ops — le vrai livrable

Un design system sans mécanisme d'application redevient de la documentation morte en trois semaines. L'outil d'implémentation étant **Claude Code**, l'application se fait à trois profondeurs — de la plus tardive à la plus précoce.

### 5.1 `check_tokens.py` — le contrôle, écrit une fois

Un script unique, réutilisé par les trois niveaux. Échoue si `src/components.css` contient :

- un hex brut (`#[0-9a-fA-F]{3,8}`)
- une valeur `px` hors de la whitelist de l'échelle d'espacement

### 5.2 Trois niveaux d'application

| Niveau | Mécanisme | Moment | Effet |
|---|---|---|---|
| **Contexte** | `CLAUDE.md` | Chargé à chaque session | Préventif — la règle est connue avant d'écrire |
| **Boucle** | Hook `PostToolUse` Claude Code | À chaque `Edit`/`Write` sur `src/**.css` | **Correctif immédiat** — sortie en erreur, le message revient à l'agent qui corrige seul |
| **Filet** | GitHub Actions | Au push | Rattrape les modifs hors Claude Code (édition manuelle, autre machine) |

**Le niveau « boucle » est le gain majeur du passage à Claude Code.** Avec un lint CI seul, une valeur en dur survit jusqu'au push et se corrige dans une session séparée, souvent des jours plus tard. Avec un hook, l'agent reçoit l'erreur dans le même tour et corrige avant même que tu voies le diff. La dérive n'atteint jamais le disque.

`CLAUDE.md` (à créer — absent du repo) :

```
## Design system
Avant d'écrire du CSS : lire design/DSDS.md.
Jamais de valeur littérale — uniquement var(--*).
Ne jamais éditer index.html ni src/tokens.css : fichiers générés.
Nouveau token nécessaire → l'ajouter à design/tokens.json, pas au CSS.
Après toute modif de src/ : lancer python build_ui.py.
```

Hook dans `.claude/settings.json` — matcher `Edit|Write`, commande `python3 scripts/check_tokens.py`, sortie non nulle pour renvoyer l'erreur à l'agent. La syntaxe exacte sera vérifiée contre la doc Claude Code au moment de l'implémentation (P3).

### 5.3 ⚠️ `.claude/` est actuellement dans `.gitignore`

Conséquence : les hooks ne seraient pas versionnés — ils disparaîtraient sur une autre machine et ne survivraient pas à un clone. À corriger en P3 :

```gitignore
.claude/*
!.claude/settings.json
```

`settings.local.json` (préférences personnelles) reste ignoré. Sans ce correctif, le niveau « boucle » est un garde-fou local et non un garde-fou de projet.

### 5.4 Protection des fichiers générés

- Bandeau `<!-- GENERATED — edit src/ instead -->` en tête d'`index.html`
- `index.html linguist-generated=true` dans `.gitattributes`
- Le seul vrai piège de l'étape de build : éditer `index.html` à la main et perdre sa modif au build suivant.

### 5.5 Changelog

Les évolutions du design system vont dans le `CHANGELOG.md` existant. Pas de second fichier à oublier de mettre à jour.

---

## 6. Séquençage

| Phase | Contenu | Critère de sortie |
|---|---|---|
| **P1 — Extraction** | Extraire les 75 hex et les 503 px vers `tokens.json` + `build_tokens.py`. Normaliser les doublons de casse. | Rendu **identique au pixel près** à l'existant. Diff visuel nul. |
| **P2 — Découpage** | `src/` + `build_ui.py` + intégration au workflow Pages. `DSDS.md` couvrant `card-*`, `d-*`, `compact-*`. | `python build_ui.py` reproduit l'`index.html` courant. |
| **P3 — Verrouillage** | Galerie `/design/` + `check_tokens.py` + hook Claude Code + `CLAUDE.md` + désignorer `.claude/settings.json`. | Demander à Claude Code d'écrire un hex brut : il le corrige de lui-même dans le même tour. |
| **P4 — Figma** | Tokens Studio → Variables Figma. | Les couleurs Figma correspondent à `tokens.json`. |

**P4 en dernier, sans exception.** Synchroniser vers Figma avant P1–P3 revient à synchroniser du désordre.

---

## 7. Ce que cette stratégie ne fait pas

Écarté volontairement, avec la raison :

- **Tests de régression visuelle** (Playwright / Chromatic) — coût d'infra disproportionné à cette échelle. La galerie couvre l'inspection manuelle.
- **Versionnage sémantique du DS** — un seul consommateur, aucune contrainte de compatibilité.
- **Couche de composants JS** — le frontend est en vanilla assumé ; introduire un framework pour le DS contredirait le principe de simplicité du projet.
- **Design tokens multi-thèmes** — pas de besoin light mode identifié. L'architecture 2 tiers permet de l'ajouter sans refonte.
