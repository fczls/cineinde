# Revue critique P1–P2 — regard staff engineer / CTO

> 2026-07-22 · Revue de la réintégration du design system (P1 tokens + P2 build).
> Objectif : challenger la stratégie et l'implémentation pour renforcer l'existant et
> préparer le scale. Verdict d'abord, preuves ensuite.

## Verdict en une phrase

**On a construit une bonne machine, mais elle ne gouverne aujourd'hui qu'une minorité des décisions de design** — et le prochain lot (P3 : lint + hook) *dépend* de corriger ça d'abord, sinon le lint est ingérable dès le jour 1.

Le pipeline (P1/P2) est sain, léger, proportionné. Le problème n'est pas *comment* on tokenise, c'est *combien* on tokenise.

---

## Ce qui est solide (à garder)

- **Format DTCG** : standard, portable (Tokens Studio, Figma, Style Dictionary si besoin un jour). Bon pari.
- **Zéro dépendance runtime**, `--check` pour la CI, build **idempotent**, et surtout **preuve d'iso-octet** au découpage. Rare et rigoureux.
- **Flux unidirectionnel** `tokens.json → build → index.html`. Pas d'ambiguïté sur la source de vérité.
- Le runtime reste mono-fichier (bon pour Pages) alors que la source est découpée. Le bon compromis.

---

## Les 3 failles critiques (à traiter avant d'ajouter de la machinerie)

### 1. 🔴 La couverture couleur est minoritaire — fausse confiance

| | Nombre |
|---|---|
| Couleurs **définies** comme tokens | 17 |
| Couleurs **brutes distinctes encore dans `components.css`** | **58** |

Le système gouverne ~**23 %** des couleurs réellement utilisées. `#fff`, `#111`, `#000`, `#3a3a3a`, `#55473A`, `#D0A636`, et ~50 `rgba()` d'alphas variés flottent hors système. **Un token system qui capture un quart des valeurs donne une fausse assurance** : on croit avoir une palette, on a 17 couleurs élues au milieu de 58 autres.

*Correctif* : auditer les 58 valeurs distinctes → soit les fondre dans la palette (primitives), soit les assumer comme one-offs documentés. Cible : couleurs brutes ≈ 0 dans `components.css`.

### 2. 🔴 Le spacing n'est pas tokenisé du tout — le plus gros angle mort

**380 occurrences de `px`** dans `components.css`, dont **23 valeurs de spacing distinctes** (0,1,2,3,4,5,6,7,8,9,10,12,14,16,18,20,22,24,26,28,32,40,90px) sur `padding`/`margin`/`gap`. Aucune échelle.

Le spacing est la **source de dérive visuelle la plus fréquente** d'une UI (rythme, alignement), et c'est la plus grosse surface non gouvernée — bien plus que les rayons (~30 usages) qu'on a, eux, tokenisés. On a traité le petit avant le gros.

*Correctif* : échelle de spacing base-4 (`4·8·12·16·20·24·32·40·48`), tokens `--sp-*`, remplacement. Les outliers (1,2,3,5,6,7,9,22,26,90) sont à trancher un par un — ce sont souvent des accidents.

### 3. 🟠 Les garde-fous arrivent APRÈS le piège (erreur de séquencement)

P2 a rendu `index.html` **généré**, mais **aucun** filet n'est en place :

| Garde-fou | État |
|---|---|
| Bandeau « généré » en tête d'`index.html` | ❌ absent |
| `.gitattributes` (`linguist-generated`) | ❌ absent |
| `CLAUDE.md` / hook | ❌ absent |
| `--check` branché en CI | ❌ pas encore |
| `.claude/` | toujours gitignoré |

Résultat : aujourd'hui, on **peut** éditer `index.html` à la main, committer, et perdre la modif au prochain build — ou pire, laisser `index.html` diverger de `src/` sans que rien ne le voie. **P2 sans P3 est un solde net négatif sur la sécurité** : on a ajouté un footgun sans le cran de sûreté.

*Correctif* : remonter les garde-fous **bon marché** (bandeau, `.gitattributes`, `--check` en CI) **maintenant**, pas en P3.

---

## Dette d'architecture (à adresser au scale, pas maintenant)

### 4. 🟠 Le JS est enterré dans `template.html`

`template.html` = **1788 lignes**, dont ~1528 de JS (177 fonctions) dans un fichier `.html`. La chose la plus susceptible d'avoir besoin de modularisation (la logique) est la moins accessible : pas de coloration JS, pas de lint, pas de typage. La décision « laisser le JS pour l'iso-octet » était raisonnable pour *ce* pas, mais c'est **la prochaine priorité d'archi**, pas un report indéfini. Les 3 blocs `<script>` s'extraient avec la même approche à marqueurs (le module ESM est le plus facile).

### 5. 🟡 Tokens mono-tier avec abstractions qui fuient

- `--bg2`/`--bg3`/`--bg4` : échelle numérotée, non sémantique (« à quoi sert bg3 ? »).
- `--compact-red`, `--compact-times`, `--resa-line`, `--resa-tkt` : tokens **de composant** logés dans la palette **globale** — violation de tier.

OK pour un site 1 page. Mais définir dès maintenant le **chemin de migration 2-tiers** (primitive → sémantique) pour le jour où un thème clair / rebrand arrive. Ne pas le construire aujourd'hui (YAGNI).

### 6. 🟡 `components.css` monolithique (681 lignes)

Sectionné (24 blocs de commentaires), donc lisible — mais un seul fichier. Au 2ᵉ contributeur ou à la croissance, le découper par famille DSDS (`card.css`, `detail.css`, `week.css`…) avec concaténation au build. Pas avant.

---

## Lentille CTO — stratégie

### 7. L'iso-octet est le bon critère de *migration*, pas le bon *contrat permanent*

Prouver l'iso-octet valide le découpage — excellent. Mais ça **fige la sortie non optimisée** : `index.html` servi = 130 Ko, 64 commentaires CSS, zéro minification. Au scale, on veut que le build **améliore** la sortie (minify, autoprefix, purge du CSS mort). Le contrat « reproduire à l'octet » doit être abandonné dès qu'un vrai bundler entre.

### 8. Build bespoke = zéro dépendance mais zéro savoir transférable

Le pipeline maison (Python + marqueurs) est parfait pour un solo : rien à installer, tout est lisible. Mais un nouveau contributeur doit apprendre `build_ui.py` + `build_tokens.py` + le système de marqueurs, là où Vite + Style Dictionary sont des standards connus. *Trigger de bascule* : 2ᵉ contributeur régulier **ou** besoin de références/thèmes dans les tokens (que le générateur maison ne sait pas résoudre).

### 9. Discipline de ROI — savoir s'arrêter

C'est un site 1 page, solo, dont la roadmap produit (plus de cinémas, données festivals, sources d'avis) est la vraie valeur. P1/P2 étaient **proportionnés** (légers, sans dépendance). Aller plus loin sur la sophistication DS *maintenant* serait du sur-investissement. Le bon move : **fermer les 2 trous de couverture (couleur, spacing) + les garde-fous, puis STOP** sur le DS et livrer du produit jusqu'à ce que la complexité de l'app tire vers plus.

---

## Reco séquencée (corrigée)

1. **Maintenant** (avant tout le reste) — garde-fous bon marché : bandeau généré, `.gitattributes`, `build_ui.py --check` en CI. ~30 min, supprime le footgun.
2. **Compléter la substance de P1** — audit couleur (58→~0 brutes) + échelle de spacing. **C'est ce qui rend `tokens.json` réellement source de vérité.**
3. **Puis P3** (lint `check_tokens.py` + hook + galerie + `CLAUDE.md`). ⚠️ **Dépendance** : le lint doit venir *après* l'étape 2, sinon il signale 58 couleurs + 23 spacings dès le jour 1 et devient inutilisable (ou croule sous une allowlist).
4. **Dette d'archi, dans l'ordre** : extraction JS (bientôt) → split `components.css` (2ᵉ contributeur) → 2-tiers tokens (thème) → bundler (perf/scale).

## Ce qu'il ne faut PAS faire (YAGNI)

- Pas de Style Dictionary / bundler tant qu'on est solo et sans besoin de thème.
- Pas de 2ᵉ tier de tokens tant qu'il n'y a qu'un thème.
- Pas de split `components.css` tant qu'il n'y a qu'un éditeur.
- Pas de tests de régression visuelle automatisés (Playwright/Chromatic) — la galerie P3 + l'œil suffisent à cette échelle.
