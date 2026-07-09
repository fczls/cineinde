# Architecture — Pipeline de données (scraper.py)

> Fonctionnement interne du scraper : les sources (*variants*), les étapes de `main()`, l'enrichissement, le garde-fou. Les invariants/contrats transverses vivent dans [Vue d'ensemble](README.md).
> Dernière mise à jour : 2026-07-10

---

## Les 3 sources (variants)

Chaque source a son parser dédié mais produit le **même « film dict »** (contrat C1 du hub). Ce qui *varie* d'une source à l'autre :

| Source | Fonction | Format brut | Modèle de semaine | Métadonnées | Fragilité |
|---|---|---|---|---|---|
| **Comoedia** | `scrape_comoedia_pdf()` (scraper.py:1303) | **PDF** hebdo (magazine 2 colonnes) | 1 PDF = 1 semaine, publié 1×/sem | pauvres (titre en CAPS → `_titlecase_fr`) | 🔴 haute (layout PDF, découverte d'URL CDN) |
| **Lumière** | `scrape_lumiere()` (scraper.py:1610) | **HTML** rendu serveur, param `?week=YYYY-MM-DD` | semaine explicite (`get_last_wednesday`) | pauvres à la liste, enrichies par page détail | 🟡 moyenne (redesign HTML) |
| **Le Zola** *(à venir)* | `scrape_zola()` | **HTML** WordPress, liste roulante `/films-a-laffiche/` → `/movies/{slug}/` | pas de param semaine — liste roulante qui déborde la semaine | riches (mais **à laisser vides** pour I2) | 🟡 à confirmer — *Challenge — Ajout Cinéma Le Zola* (vault Obsidian) |

**Point clé (variant piégeux) :** le *modèle de semaine* diffère. Lumière prend une semaine ; le PDF Comoedia EST une semaine ; Zola est une liste roulante coupée après coup par `filter_current_week` (scraper.py:2049). Un `scrape_X()` ne « calque » un autre que sur la *structure* (liste → détail), pas sur le modèle temporel.

---

## Le pipeline `main()` (scraper.py:2098)

Ordre **significatif** (certains invariants en dépendent) :

1. **Scrape Comoedia PDF** (sauf `--no-comoedia-pdf`). 0 film ≠ panne : le PDF hebdo peut être déjà traité (dédup) ou pas encore publié → santé évaluée en fin de run (I6).
2. **Scrape Lumière** (sauf `--no-lumiere`), override `--lumiere-week`.
3. **Fusion** `all_films = comoedia + lumiere`. Vide → `exit 2`.
4. **Enrichissement TMDB/OMDb** (sauf `--no-omdb`) — voir § ci-dessous.
5. **Upsert Supabase** — **AVANT** le filtrage (invariant I4) : on archive tout l'historique, pas juste la semaine.
6. **`filter_current_week`** (sauf `--no-filter`) : fenêtre today → J+7.
7. **Écriture conditionnelle** `programme.json` (invariant I5).
8. **Garde-fou de santé** (I6).

*(Le futur wiring Zola s'insère en 2bis + fusion en 3 + flag `--no-zola`.)*

---

## Enrichissement (scraper.py:1875 `enrich_omdb`)

- **TMDB en premier** (`_enrich_tmdb_first`), **OMDb en fallback** pour les champs restants (`_enrich_omdb_fallback`).
- Complète : `imdbId, poster, imdbRating, genres, synopsis, cast`, et surtout `annee, realisateur, titreOriginal, duree`.
- **Dédup des appels** : un seul appel par **titre normalisé** (`_normalize_title_key`, scraper.py:1870), cache inter-cinémas (scraper.py:2168).
- **Propagation bidirectionnelle** (scraper.py:2197-2208) : pour chaque groupe de même titre normalisé, on collecte la meilleure valeur de chaque champ *dans tout le groupe* et on la copie aux membres **dont le champ est vide**. → C'est le moteur de l'invariant I2 : ce qui fait qu'un film Comoedia sans réalisateur hérite du réalisateur canonique TMDB, et donc dédup avec la copie Lumière.

⚠️ **Deux dédups distinctes à ne pas confondre :**
- `_normalize_title_key` (normalisée) → regroupe pour l'**enrichissement**.
- `on_conflict="titre,annee,realisateur"` (brute) → dédup l'**upsert Supabase** (I1).
Elles ne coïncident pas : un groupe enrichi ensemble peut quand même créer 2 lignes `films` si le titre brut diffère.

---

## Garde-fou de santé asymétrique (scraper.py:2263-2300)

But : détecter une **vraie** panne Comoedia sans crier au loup avant publication.

- Semaine de référence = mercredi de la semaine **en cours** (sans saut au mercredi suivant).
- `comoedia_live` = des films Comoedia ce run **ou** en base pour la semaine.
- `lumiere_live` = `count_week_seances(..., exclude_slug="comoedia")` > 0 → « quelqu'un d'autre a publié ».
- **Échec `exit 4` uniquement si** `not comoedia_live and lumiere_live` (asymétrie = signe d'une panne du parser Comoedia).
- Si personne n'a publié → non-événement, pas d'échec (indispensable à la cadence J/2).

Helper : `count_week_seances(week_start, week_end, slug=/exclude_slug=)` (scraper.py:715). `None` = état « inconnu » (pas de creds) distinct de 0.

⚠️ **Limite connue (impact Zola) :** `exclude_slug` ne gère **qu'un seul** slug. Ajouter Zola fait compter ses séances comme `lumiere_live` → faux rouge possible, aggravé car Zola publie en avance. À corriger lors de l'intégration Zola (cf. *Challenge — Ajout Cinéma Le Zola* (vault Obsidian) piège B).

---

## Options CLI utiles

| Flag | Effet |
|---|---|
| `--dry-run` | aucune écriture (ni JSON ni Supabase) ; imprime le JSON |
| `--no-omdb` | désactive l'enrichissement (tests rapides) |
| `--no-filter` | ne filtre pas par semaine |
| `--no-lumiere` / `--no-comoedia-pdf` | désactive une source |
| `--pdf-file` / `--pdf-url` | PDF Comoedia local / URL directe (contourne la découverte CDN) |
| `--lumiere-week YYYY-MM-DD` | force la semaine Lumière |

Codes de sortie : `1` args invalides · `2` aucune source · `4` panne Comoedia (I6).
