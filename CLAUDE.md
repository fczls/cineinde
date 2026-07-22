# CLAUDE.md — instructions pour l'agent

CinéInde : agrégateur de programmes des cinémas indépendants de Lyon. Avant de travailler,
lire `README.md` (vue d'ensemble), `ARCHITECTURE.md` → `docs/architecture/` (invariants I1–I8,
contrats C1–C4) et `CHANGELOG.md` (chronologie).

## Frontend : `index.html` est GÉNÉRÉ — ne jamais l'éditer à la main

`index.html` est assemblé par `build_ui.py`. Toute édition directe est écrasée au prochain build.

Chaîne de build :

```
design/tokens.json ──build_tokens.py──▶ src/tokens.css ─┐
src/components.css ─────────────────────────────────────┤──build_ui.py──▶ index.html
src/template.html  ─────────────────────────────────────┘   (+ design.html : galerie)
```

- **Couleur / taille de police / rayon / spacing** → éditer `design/tokens.json`, puis `python3 build_ui.py`.
- **Style d'un composant** → `src/components.css`.
- **Markup ou JS** → `src/template.html`.
- Après toute modif de `src/` ou `design/tokens.json` : **`python3 build_ui.py`**, puis commit `index.html` avec les sources.

Fichiers générés à ne pas éditer : `index.html`, `src/tokens.css`, le bloc `:root` (marqueurs `@tokens`).

## Design system : jamais de valeur brute

Lire `design/DSDS.md` avant d'écrire du CSS. Dans `src/components.css` :

- **Couleurs, tailles de police, rayons** → toujours `var(--…)`, jamais `#hex` / `rgba()` / `Npx`.
  Nouveau besoin → ajouter le token à `design/tokens.json`, pas une valeur en dur.
- **Espacements** (`padding`/`margin`/`gap`) → grille 2px, valeurs paires (littéral px toléré, pas de var).
- Exceptions couleur (one-offs) : allowlist dans `design/check_tokens.py` + `docs/design-system/color-mapping.md`.

Le lint `python3 design/check_tokens.py` fait foi. Un hook `PostToolUse` (`.claude/settings.json`)
le lance après chaque édition et renvoie les violations — les corriger avant de continuer.

## Galerie

`design.html` = référence vivante des tokens (valeurs + notes d'usage), régénérée par `build_ui.py`.
Voir via `python3 tools/serve.py` → `http://localhost:4173/design.html`.

## Pipeline de données (rappel)

`scraper.py` (Comoedia PDF + Lumière) → Supabase (source) + `programme.json` (fallback). Ne jamais
committer de clé : secrets via GitHub Actions. Détails : `docs/architecture/`.
