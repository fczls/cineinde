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

## Hygiène des dossiers

**Objectif : chaque dossier — à commencer par la racine — reste lisible d'un coup d'œil.** Un dossier où l'on ne retrouve plus un fichier en une seconde a échoué.

Avant d'**ajouter un fichier** : vérifier qu'il a une place évidente dans l'arborescence actuelle. S'il n'en a pas, c'est le signal d'une décision à prendre — pas d'un dépôt à la racine « en attendant ».

Avant de **créer un sous-dossier** : peser son utilité réelle. Deux dérives symétriques à éviter :

- **Sur-fragmenter** — un dossier pour 1 seul fichier ajoute un niveau de navigation sans rien clarifier. *Règle de trois* : on ne crée un sous-dossier qu'à partir de ~3 fichiers d'une même nature, ou quand une nature distincte apparaît clairement (ex. `tools/` pour le dev/debug).
- **Sous-organiser** — laisser une racine ou un dossier accumuler des fichiers de natures mélangées (code, données, debug, doc) jusqu'à devenir un fourre-tout.

En cas de doute, préférer laisser le fichier à sa place logique existante plutôt que d'inventer un dossier ; regrouper *a posteriori* quand un motif se dégage coûte moins cher que défaire une arborescence prématurée.
