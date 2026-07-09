# Architecture — Vue d'ensemble

> **Hub** de la connaissance fonctionnelle CinéInde. Décrit l'**état stable** du système et surtout les **contrats & invariants transverses** (les *liants*) qui n'ont de maison nulle part ailleurs.
> Pour la chronologie « pourquoi on a changé X le jour J » → [`CHANGELOG.md`](../../CHANGELOG.md) (repo). Pour *comment on travaille* → *Workflow* (vault Obsidian).
> Dernière mise à jour : 2026-07-10

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
| I1 | **Dédup film = clé brute `(titre, annee, realisateur)`** | `films UNIQUE(...)` (001_initial.sql:31) ; `on_conflict` (scraper.py:1251) | deux sources écrivent le même film avec une casse/orthographe différente → doublon |
| I2 | **Convergence par le vide** : une source qui ne connaît pas `annee`/`realisateur` les laisse à `None` ; TMDB fournit la valeur canonique, propagée au groupe de même titre normalisé (remplissage des **vides seulement**) | Lumière (scraper.py:1541-1542) ; propagation (scraper.py:2197-2208) | une source remplit ces champs avec sa propre valeur riche → I1 diverge → doublon (cf. *Challenge — Ajout Cinéma Le Zola* (vault Obsidian)) |
| I3 | **Supabase = source primaire, `programme.json` = fallback** | front (index.html:1127+) lit Supabase, retombe sur JSON | — |
| I4 | **Ordre : upsert AVANT filtrage semaine** | main (scraper.py:2210 puis 2215) | filtrer avant → on n'archiverait que la semaine courante, cleanup casse l'historique |
| I5 | **Écriture JSON conditionnelle, hors champs volatils** (`resa_url` = token horaire) | `_films_sans_volatiles` (scraper.py:2082) ; compare (scraper.py:2246) | inclure `resa_url` → réécriture + commit à chaque run pour rien |
| I6 | **Garde-fou de santé asymétrique** : échec (`exit 4`) seulement si une autre source a publié la semaine mais pas Comoedia | main (scraper.py:2263-2300) | compter Zola comme « autre source » → faux rouge (cf. *Challenge — Ajout Cinéma Le Zola* (vault Obsidian)) |
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
Le **schéma** (`cinemas`/`films`/`seances`) EST l'interface entre back et front. Subtilité : Supabase **dédup** le film (une ligne `films` partagée) mais le front **re-splitte** par `(film.id, cinema)` à la lecture (index.html:1161) — un même film joué dans 2 salles redevient 2 cartes. Voir [Frontend](frontend.md).

### C3 — Duplications à modifier ensemble ⚠️
Deux savoirs sont codés **en double**, sans garde qui le signale :

| Savoir | Back | Front |
|---|---|---|
| Mapping nom cinéma ↔ slug / libellés | `CINEMA_SLUGS` (scraper.py:1173) | `CINEMA_FILTERS`, `SHORT_CINEMA`, `CINEMA_SHORT`, `getCinemaSectionLabel` (index.html:1267-1296) |
| Normalisation de titre | `_normalize_title_key` (scraper.py:1870) | `normalizeTitle` (index.html:891) |

Ajouter un cinéma = toucher **les deux colonnes**. (C'est le piège frontend de *Challenge — Ajout Cinéma Le Zola* (vault Obsidian).)

### C4 — Le contrat `programme.json` (fallback)
Forme : `{generated_at, sources:[...], films:[<film dict>]}`. Consommé par le front uniquement si Supabase est indisponible. Le `cinema` y est au **niveau film** (pas séance).

---

## Développements futurs — où ça se branche

| Chantier | Invariants/contrats concernés | Notes |
|---|---|---|
| **Le Zola** (nouvelle source) | C1 (film dict), I1+I2 (dédup), C3 (front), I6 (garde-fou à isoler) | *Variant* pur. Checklist : *Challenge — Ajout Cinéma Le Zola* (vault Obsidian) |
| **Événements** (nouvel onglet automatisé) | Nouvelle table `evenements` (Infra), nouveaux parsers (Pipeline), remplace `EVENTS_DATA`/`renderEvents` (Frontend) | Feature *transverse* — touche les 3 spokes. Éval : *Exploration — Événements* (vault Obsidian) |

**Principe :** avant de toucher une pièce, vérifier ici quels invariants/contrats elle porte — pour ne pas ré-inspecter le code à chaque fois.
