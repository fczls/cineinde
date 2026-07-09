# Architecture — commencer ici

> **Avant de travailler sur ce projet, lis la doc d'architecture** — elle donne le contexte fonctionnel (invariants, contrats, variants) que ni le code ni le `CHANGELOG` ne rendent explicites.

## Où lire

1. **Contexte fonctionnel (à lire en premier)** → [`docs/architecture/`](docs/architecture/) :
   - [Vue d'ensemble](docs/architecture/README.md) — hub : carte du système + invariants (I1–I8) + contrats (C1–C4)
   - puis le spoke utile : [Pipeline de données](docs/architecture/pipeline.md), [Frontend](docs/architecture/frontend.md), ou [Données & Infra](docs/architecture/data-infra.md).
2. **Chronologie des changements (« pourquoi on a changé X »)** → [`CHANGELOG.md`](CHANGELOG.md).
3. **Comment on travaille (branches, preview, CI)** → `Workflow.md` (vault Obsidian).

## Deux lieux de savoir, un chacun pour ce qu'il fait le mieux

- **Repo** (ici) : le code, le `CHANGELOG` (chronologie), et **`docs/architecture/`** (état stable) — le tout **versionné ensemble**, donc doc et code se relisent dans la même PR.
- **Vault Obsidian** (`~/Documents/Obsidian/CineInde`) : la **réflexion** (explorations de faisabilité, challenges de plans) et le **process** (`Workflow`). Liens/graphe, orienté humain, non versionné avec le code.

Le vault a son propre `README.md` expliquant cette coexistence.

## Règle de maintenance (allégée)

**Déclencheur : uniquement quand un changement touche l'un des invariants (I1–I8) ou contrats (C1–C4)** listés dans [le hub](docs/architecture/README.md). Alors, mettre à jour la note concernée **dans la même PR** que le code, et rafraîchir sa ligne « Dernière mise à jour ».

Un `fix:` de parsing, de sélecteur HTML ou de casse qui ne touche aucun de ces points **ne demande pas** de mise à jour de l'architecture. Inutile de se poser la question à chaque commit — seulement quand on modifie l'une de ces ~12 lignes structurantes.
