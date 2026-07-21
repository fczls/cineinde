# Architecture — Vue d'ensemble

> **Hub** de la connaissance fonctionnelle CinéInde. Décrit l'**état stable** du système et surtout les **contrats & invariants transverses** (les *liants*) qui n'ont de maison nulle part ailleurs.
> Pour la chronologie « pourquoi on a changé X le jour J » → [`CHANGELOG.md`](../../CHANGELOG.md) (repo). Pour *comment on travaille* → *Workflow* (vault Obsidian).
> Dernière mise à jour : 2026-07-20

> 📍 Ces notes d'architecture vivent **dans le repo** (`docs/architecture/`), versionnées avec le code — à mettre à jour dans la même PR quand un invariant/contrat change. La réflexion prospective (explorations, challenges) et le process restent dans le **vault Obsidian**.

---

## À quoi sert cette note

Le « pourquoi » de l'évolution du projet est bien documenté mais **chronologique** (CHANGELOG, commentaires inline, commits). Il manquait une référence de l'**état stable + invariants**. Cette note comble ce trou. Elle est organisée selon trois lentilles :

- **Invariants** — règles qui *doivent* tenir, sinon ça casse (§ Invariants).
- **Variants** — ce qui change par source / par cinéma (détaillé dans [Pipeline de données](pipeline.md)).
- **Liants** — les contrats de couplage entre composants (§ Contrats).

Notes sœurs : [Pipeline de données](pipeline.md) · [Frontend](frontend.md) · [Données & Infra](data-infra.md).

---

## Carte du système & flux de données

```
   Sites externes                    scraper.py (pipeline)              Stockage            Front
┌──────────────────┐         ┌────────────────────────────────┐    ┌────────────┐   ┌──────────────┐
│ Comoedia (PDF)   │──┐      │ 1. scrape_* → film dicts        │    │  Supabase  │   │  index.html  │
│ Lumière (HTML)   │──┼────▶ │ 2. fusion all_films             │─┬─▶│ (primaire) │◀──│ lit en direct│
│ Le Zola (à venir)│──┘      │ 3. enrichissement TMDB/OMDb     │ │  │ films/     │   │              │
└──────────────────┘         │ 4. upsert Supabase (AVANT filtre)│ │  │ seances/   │   │  fallback ▼  │
   TMDB / OMDb ───────────── │ 5. filter_current_week          │ │  │ cinemas    │   │ programme.json│
   (enrichissement)          │ 6. écriture conditionnelle JSON │ └─▶└────────────┘   └──────────────┘
                             └────────────────────────────────┘   programme.json = fallback commité
```

**Sens de lecture :** les sites publient → le scraper normalise vers un **format commun** → upsert dans Supabase → le front lit Supabase (fallback `programme.json` si indispo). CI orchestre le tout (voir [Données & Infra](data-infra.md)).

---

## Invariants (ce qui doit tenir)

| # | Invariant | Où c'est ancré | Casse si… |
|---|---|---|---|
| I1 | **Dédup film = `imdb_id` en clé primaire, repli sur `(titre, annee, realisateur)`** — l'imdb_id (identifiant canonique TMDB/OMDb) fusionne les variantes de casse/format/année ; garde-fou `_years_close` : on ne fusionne sur un imdb_id partagé que si les années restent proches (un mauvais match TMDB ne doit pas fusionner deux vrais films). Sans imdb_id → repli sur la clé brute (titre **normalisé** casse/espaces + annee + realisateur), qui préserve les vrais homonymes (ex. *La Chaleur* 1938 vs 2026). **Durcissement 2026-07-20** : quand l'imdb_id manque (enrichissement intermittent) **ou** que cette clé de repli dérive (année 2025↔2026, réalisateur tronqué), l'upsert se **rattache à la ligne existante par titre normalisé** au lieu d'en créer une — garde-fous anti-homonyme `_years_close` + `_reals_compatible`, + backfill imdb_id. | `films_imdb_id_key` index unique partiel (003_dedup_imdb_id.sql) + `films UNIQUE(titre,annee,realisateur)` (001_initial.sql) ; upsert `upsert_all_to_supabase` (scraper.py : imdb → clé brute → index titre) ; garde-fous `_years_close` + `_reals_compatible` (scraper.py) ; miroir front `dedupeFilms` (index.html, 2 passes) | un mauvais match TMDB partage un imdb_id entre deux films distincts → fusion à tort (mitigé par `_years_close`) ; **avant le durcissement** : un scrape sans imdb + clé dérivée créait un doublon et laissait la ligne canonique figée (symptôme observé : liens de réservation périmés sur *Le Héros de Berlin*) |
| I2 | **Convergence par le vide** : l'enrichissement (TMDB/OMDb) et la propagation inter-sources ne remplissent QUE les champs **vides** — rien n'écrase jamais une valeur existante. Une **nouvelle source** doit donc laisser `annee`/`realisateur` à `None` : sa copie hérite de la valeur du groupe (détail Lumière, PDF Comoedia ou OMDb) et la clé I1 converge. *(Précision 2026-07-10 : Lumière remplit bien ces champs depuis ses pages détail — la règle porte sur le « remplir-si-vide », pas sur « Lumière laisse vide ».)* | `_apply_tmdb_movie`/`_enrich_omdb_fallback` (fill-if-empty) ; propagation (scraper.py `main`, groupes de titres normalisés) ; Zola laisse `annee`/`realisateur` à None (`_zola_extract_film`) | une source remplit ces champs avec sa propre variante (ex. année de sortie FR ≠ année de production) → I1 diverge → doublon |
| I3 | **Supabase = source primaire, `programme.json` = fallback** | front (`loadFromSupabase()`) lit Supabase, retombe sur JSON | — |
| I4 | **Ordre : upsert AVANT filtrage semaine** | main (scraper.py:2210 puis 2215) | filtrer avant → on n'archiverait que la semaine courante, cleanup casse l'historique |
| I5 | **Écriture JSON conditionnelle, hors champs volatils** (`resa_url` = token horaire cotecine). `resa_url` reste **volatil** (exclu de la comparaison qui décide de réécrire le JSON). **Depuis 2026-07-20** il est en plus (a) **rendu par le front** (deep-link billetterie, cf. C3) et (b) **strippé au chargeur JSON de repli** (`loadFromJson`) — le fallback ne sert donc jamais un lien figé/périmé (le `D{epoch}` cotecine pourrit avec le temps). | `_films_sans_volatiles` (scraper.py) ; compare (scraper.py) ; **front** : strip dans `loadFromJson` + re-validation `safeResaUrl` (index.html) | inclure `resa_url` dans la comparaison → réécriture+commit à chaque run pour rien ; **ou** ne pas stripper au repli → deep-links périmés servis en mode fallback (S3) |
| I6 | **Garde-fou de santé asymétrique** : échec (`exit 4`) seulement si **Lumière** a publié la semaine mais pas Comoedia. **Le Zola est exclu de la preuve « semaine publiée »** (`exclude_slugs=["comoedia","le-zola"]`) : il publie ~15 j en avance, le compter accuserait Comoedia à tort | main (scraper.py, garde-fou de fin de run) ; `count_week_seances(exclude_slugs=…)` | réintégrer Zola dans le comptage → faux rouge dès que Zola seul a des séances |
| I7 | **RLS : lecture publique, écriture service-role only** | 001_initial.sql:50-57 | le front n'écrit jamais ; seul le scraper (clé service) écrit |
| I8 | **Table `seances` gardée légère** (< plafond REST 1000) via cleanup J−10 | cleanup_old_seances.py ; front filtre `date >= today` (index.html) | table qui gonfle → séances récentes tronquées à l'affichage |

---

## Contrats (les *liants* — couplages à maintenir en phase)

### C1 — Le « film dict » (contrat scraper → upsert)
Chaque `scrape_X()` **doit** produire des dicts de cette forme, consommés tels quels par `upsert_all_to_supabase` et sérialisés dans `programme.json` :

```python
{
  "titre": str, "titreOriginal": str|None, "annee": int|None,
  "realisateur": str|None, "duree": int|None, "genres": list[str],
  "synopsis": str|None, "imdbId": str|None, "poster": str|None,
  "imdbRating": float|None, "cast": str|None,
  "source": str,            # "comoedia" | "lumiere" | "zola" ...
  "cinema": str,            # nom exact, ex. "Le Zola"
  "seances": [ {"date": "YYYY-MM-DD", "heure": "HH:MM", "version": str|None, "resa_url": str|None} ],
}
```
**C'est ce contrat qui rend une nouvelle source (Zola) faisable sans toucher au schéma.** Respecter I2 : laisser `annee`/`realisateur` à `None` plutôt que deviner.

### C2 — La frontière Supabase (contrat scraper → front)
Le **schéma** (`cinemas`/`films`/`seances`) EST l'interface entre back et front. Subtilité : Supabase **dédup** le film (une ligne `films` partagée) mais le front **re-splitte** par `(film.id, cinema)` à la lecture (`loadFromSupabase()`, regroupement par cinéma) — un même film joué dans 2 salles redevient 2 cartes. Voir [Frontend](frontend.md).

**Regroupement d'affichage aligné sur la dédup back (2026-07-10) :** en vue « Tous les cinémas », le front regroupe les films par `imdbId` quand il existe (repli titre normalisé) — `filmGroupKey` (index.html), utilisé par `getRowsForDate` et `openFilm`. Même axe que la dédup back (I1) : fusionne les variantes de casse, **sépare les vrais homonymes** (2 imdbId ≠ → 2 cartes, ex. *La Chaleur* 1938 vs 2026). Avant, `normalizeTitle` groupait par titre seul → il fusionnait à tort les homonymes.

### C3 — Duplications à modifier ensemble ⚠️
Deux savoirs sont codés **en double**, sans garde qui le signale :

| Savoir | Back | Front |
|---|---|---|
| Mapping nom cinéma ↔ slug / libellés | `CINEMA_SLUGS` (scraper.py) | `CINEMA_FILTERS`, `SHORT_CINEMA`, `CINEMA_SHORT`, `getCinemaSectionLabel` (index.html) |
| Normalisation de titre | `_normalize_title_key` (scraper.py) | `normalizeTitle` (index.html) |
| Clé de dédup/regroupement film | `imdb_id` puis `(titre, annee, real)` — upsert (scraper.py) | `filmGroupKey` : `imdbId` puis titre normalisé (index.html) |
| **Allowlist des liens de réservation** (`https` + hôte cotecine/ticketingcine) | `is_valid_resa_url` (scraper.py, au scrape) | `safeResaUrl` (index.html, au rendu) |

Ajouter un cinéma = toucher **les deux colonnes**. (C'est le piège frontend de *Challenge — Ajout Cinéma Le Zola* (vault Obsidian).)

> **Allowlist billetterie (2026-07-20)** — dédup *voulue* : aucune CSP n'est posable sur GitHub Pages, l'allowlist EST la seule défense contre un `href` tiers/`javascript:` injecté. Le front **re-valide** (`safeResaUrl`) ce que le back a déjà filtré (`is_valid_resa_url`) car il lit **deux** sources (Supabase + JSON) et ne doit faire confiance à aucune. Élargir un hôte = toucher **les deux**. Genèse : *Exploration — Accès billetterie* (vault Obsidian).

### C4 — Le contrat `programme.json` (fallback)
Forme : `{generated_at, sources:[...], films:[<film dict>]}`. Consommé par le front uniquement si Supabase est indisponible. Le `cinema` y est au **niveau film** (pas séance).

---

## Développements futurs — où ça se branche

| Chantier | Invariants/contrats concernés | Notes |
|---|---|---|
| ~~**Le Zola** (nouvelle source)~~ ✅ **intégré 2026-07-10** | C1 respecté, I2 appliqué (annee/realisateur à None), C3 mis à jour, I6 isolé (`exclude_slugs`) | Voir [pipeline.md](pipeline.md) § sources. Genèse : *Challenge — Ajout Cinéma Le Zola* (vault Obsidian) |
| ~~**Dédup inter-sources par imdb_id**~~ ✅ **traité 2026-07-10** | I1 réécrit (imdb_id primaire + repli), C2/C3 mis à jour, garde-fou `_years_close` | migration 003 + `scripts/merge_duplicate_films.py` (à lancer sur la prod dans cet ordre). Genèse : *Exploration — Dédup inter-sources* (vault Obsidian) |
| ~~**Billetterie deep-link (`resa_url` rendu au front)**~~ ✅ **traité 2026-07-20** | I5 étendu (rendu front + strip au repli JSON), I1 durci (rattachement par titre), C3 (allowlist dupliquée front/back) | Option A « câbler l'existant » : parser Lumière lit le `<a>` au **périmètre du `<time>`** (fin du bug « séance passée »), allowlist sécu, flag `ENABLE_RESA_LINKS`. Nettoyage one-shot des doublons `films` (112→87). Genèse : *Exploration — Accès billetterie* (vault Obsidian) |
| **Événements** (nouvel onglet automatisé) | Nouvelle table `evenements` (Infra), nouveaux parsers (Pipeline), remplace `EVENTS_DATA`/`renderEvents` (Frontend) | Feature *transverse* — touche les 3 spokes. Éval : *Exploration — Événements* (vault Obsidian). **Découverte 2026-07-10 : Le Zola a une page `/events/`** (« Les événements ») — 3e source potentielle, contrairement à ce que disait l'exploration |

**Principe :** avant de toucher une pièce, vérifier ici quels invariants/contrats elle porte — pour ne pas ré-inspecter le code à chaque fois.
