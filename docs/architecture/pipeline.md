# Architecture — Pipeline de données (scraper.py)

> Fonctionnement interne du scraper : les sources (*variants*), les étapes de `main()`, l'enrichissement, le garde-fou. Les invariants/contrats transverses vivent dans [Vue d'ensemble](README.md).
> Dernière mise à jour : 2026-07-25

---

## Les 3 sources (variants)

Chaque source a son parser dédié mais produit le **même « film dict »** (contrat C1 du hub). Ce qui *varie* d'une source à l'autre :

| Source | Fonction | Format brut | Modèle de semaine | Métadonnées | Fragilité |
|---|---|---|---|---|---|
| **Comoedia** | `scrape_comoedia_pdf()` (scraper.py:1303) | **PDF** hebdo (magazine 2 colonnes) | 1 PDF = 1 semaine, publié 1×/sem | pauvres (titre en CAPS → `_titlecase_fr`) | 🔴 haute (layout PDF, découverte d'URL CDN) |
| **Lumière** | `scrape_lumiere()` (scraper.py:1610) | **HTML** rendu serveur, param `?week=YYYY-MM-DD` | semaine explicite (`get_last_wednesday`) | pauvres à la liste, enrichies par page détail | 🟡 moyenne (redesign HTML) ; résa cotecine = deep-link **par séance** — le `<a>` est lu au périmètre du `<time>` (`_lumiere_parse_schedule_td`), **pas** du `<td>`, sinon toutes les séances du jour héritent du lien de la 1re → « séance passée » (bug corrigé 2026-07-20, spike SP1). `resa_url` volatil (token horaire `D{epoch}`, I5), filtré par `is_valid_resa_url` (allowlist, C3) |
| **Le Zola** | `scrape_zola()` | **HTML** WordPress, index `/films-a-laffiche/` → fiches `/movies/{slug}/` (sélecteurs documentés en tête du module dans scraper.py) | pas de param semaine — carrousel roulant ~15 jours, borné ensuite par `filter_current_week` | riches, mais `annee`/`realisateur`/`genres` **volontairement non ingérés** (I2 — année de sortie FR ≠ année de production ; genres FR ≠ vocabulaire OMDb anglais des autres films) | 🟡 moyenne (thème WordPress maison) ; résa TicketingCiné **stable** (pas de token volatil, contrairement au cotecine Lumière → rien à ajouter aux champs volatils I5) |

**Point clé (variant piégeux) :** le *modèle de semaine* diffère. Lumière prend une semaine ; le PDF Comoedia EST une semaine ; Zola est une liste roulante coupée après coup par `filter_current_week` (scraper.py:2049). Un `scrape_X()` ne « calque » un autre que sur la *structure* (liste → détail), pas sur le modèle temporel.

---

## Le pipeline `main()` (scraper.py:2098)

Ordre **significatif** (certains invariants en dépendent) :

1. **Scrape Comoedia PDF** (sauf `--no-comoedia-pdf`). 0 film ≠ panne : le PDF hebdo peut être déjà traité (dédup) ou pas encore publié → santé évaluée en fin de run (I6).
2. **Scrape Lumière** (sauf `--no-lumiere`), override `--lumiere-week`.
2bis. **Scrape Le Zola** (sauf `--no-zola`).
3. **Fusion** `all_films = comoedia + lumiere + zola`. Vide → `exit 2`.
4. **Enrichissement TMDB/OMDb** (sauf `--no-omdb`) — voir § ci-dessous.
5. **Upsert Supabase** — **AVANT** le filtrage (invariant I4) : on archive tout l'historique, pas juste la semaine.
6. **`filter_current_week`** (sauf `--no-filter`) : fenêtre today → J+7.
7. **Écriture conditionnelle** `programme.json` (invariant I5).
8. **Garde-fou de santé** (I6).

---

## Enrichissement (scraper.py:1875 `enrich_omdb`)

- **TMDB en premier** (`_enrich_tmdb_first`), **OMDb en fallback** pour les champs restants (`_enrich_omdb_fallback`).
- Complète : `imdbId, poster, backdrop, trailer, imdbRating, genres, synopsis, cast`, et surtout `annee, realisateur, titreOriginal, duree`.
  - `backdrop` (visuel paysage `w1280`) vient de l'objet movie TMDB ; `trailer` d'un appel dédié `/movie/{id}/videos` (`_tmdb_trailer` : trailer YouTube officiel FR, puis FR, puis EN, puis teaser). Servent le bloc visuel et le bouton bande-annonce de la fiche.
  - **Repli « note de bas de page »** (2026-07-25) : si la recherche TMDB échoue, réessai en retirant un renvoi final isolé (`Memento 1` → `Memento`) ; en cas de match, le titre propre de TMDB est adopté. Les vrais numéros de suite (`Toy Story 5`) ne sont jamais nettoyés — leur recherche directe aboutit.
- **Dédup des appels** : un seul appel par **titre normalisé** (`_normalize_title_key`, scraper.py:1870), cache inter-cinémas (scraper.py:2168).
- **Propagation bidirectionnelle** (scraper.py:2197-2208) : pour chaque groupe de même titre normalisé, on collecte la meilleure valeur de chaque champ *dans tout le groupe* et on la copie aux membres **dont le champ est vide**. → C'est le moteur de l'invariant I2 : ce qui fait qu'un film Comoedia sans réalisateur hérite du réalisateur canonique TMDB, et donc dédup avec la copie Lumière.

⚠️ **Deux dédups distinctes à ne pas confondre :**
- `_normalize_title_key` (normalisée) → regroupe pour l'**enrichissement**.
- Dédup de l'**upsert Supabase** (I1) : `imdb_id` en clé primaire (garde-fou `_years_close`), repli sur `(titre normalisé, annee, realisateur)`, puis — **durcissement 2026-07-20** — rattachement à une ligne existante par **titre normalisé** (index préchargé) quand l'imdb_id manque ou que la clé de repli dérive, garde-fous `_years_close` + `_reals_compatible`.

Depuis la dédup par imdb_id (2026-07-10), les deux **convergent mieux** : l'enrichissement pose l'`imdb_id`, et l'upsert dédup dessus — deux copies enrichies ensemble ne créent plus 2 lignes `films` même si le titre brut diffère (casse). Restent séparés : les vrais homonymes (imdb_id ≠, ou années trop éloignées via `_years_close` / réalisateurs incompatibles). **Pourquoi le durcissement** : l'enrichissement TMDB/OMDb est **intermittent** — un run sans imdb_id retombait sur la clé brute et, si l'année/réalisateur avaient dérivé, créait un doublon en laissant la ligne canonique figée (symptôme : liens de réservation périmés). Genèse : *Exploration — Dédup inter-sources* & *Accès billetterie* (vault Obsidian).

---

## Garde-fou de santé asymétrique (scraper.py:2263-2300)

But : détecter une **vraie** panne Comoedia sans crier au loup avant publication.

- Semaine de référence = mercredi de la semaine **en cours** (sans saut au mercredi suivant).
- `comoedia_live` = des films Comoedia ce run **ou** en base pour la semaine.
- `lumiere_live` = `count_week_seances(..., exclude_slugs=["comoedia", "le-zola"])` > 0 → « Lumière a publié ». **Zola est exclu de cette preuve** : il publie ~15 j en avance, le compter accuserait Comoedia à tort (I6).
- **Échec `exit 4` uniquement si** `not comoedia_live and lumiere_live` (asymétrie = signe d'une panne du parser Comoedia).
- Si personne n'a publié → non-événement, pas d'échec (indispensable à la cadence J/2).

Helper : `count_week_seances(week_start, week_end, slug=/exclude_slugs=)`. `None` = état « inconnu » (pas de creds) distinct de 0. *(2026-07-10 : `exclude_slug` singulier → `exclude_slugs` liste, précisément pour isoler Zola du signal — le « piège B » du challenge Zola.)*

---

## Options CLI utiles

| Flag | Effet |
|---|---|
| `--dry-run` | aucune écriture (ni JSON ni Supabase) ; imprime le JSON |
| `--no-omdb` | désactive l'enrichissement (tests rapides) |
| `--no-filter` | ne filtre pas par semaine |
| `--no-lumiere` / `--no-comoedia-pdf` / `--no-zola` | désactive une source |
| `--pdf-file` / `--pdf-url` | PDF Comoedia local / URL directe (contourne la découverte CDN) |
| `--lumiere-week YYYY-MM-DD` | force la semaine Lumière |

Codes de sortie : `1` args invalides · `2` aucune source · `4` panne Comoedia (I6).
