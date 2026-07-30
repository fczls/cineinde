# Architecture — Données & Infra

> Schéma Supabase, clés de dédup, RLS, workflows CI, scripts de maintenance. Les invariants transverses vivent dans [Vue d'ensemble](README.md).
> Dernière mise à jour : 2026-07-29

---

## Schéma Supabase (`supabase/migrations/001_initial.sql`)

Trois tables, modèle **film dédupliqué / séances par cinéma** :

```
cinemas (id, name UNIQUE, slug UNIQUE)
   ▲
   │ cinema_id (FK, ON DELETE CASCADE)
films (id, titre, titre_original, annee, realisateur, duree,
       genres[], synopsis, imdb_id, poster, backdrop, trailer,
       imdb_rating, "cast", source,
       UNIQUE(titre, annee, realisateur),           ← clé de repli (invariant I1)
       UNIQUE(imdb_id) WHERE imdb_id IS NOT NULL)   ← clé primaire (003, index partiel)
   ▲
   │ film_id (FK, ON DELETE CASCADE)
seances (id, film_id, cinema_id, date, heure, version, resa_url,
         UNIQUE(film_id, cinema_id, date, heure))  ← clé de dédup séance
```

- **Index** : `seances(film_id)`, `seances(cinema_id)`, `seances(date)`.
- **RLS** (invariant I7) : policies `SELECT USING (true)` sur les 3 tables → **lecture publique**. Aucune policy d'écriture → seul le **service-role** (scraper) écrit. Le front (clé anon) ne peut que lire.
- **Seed** : les 4 cinémas actuels sont insérés `ON CONFLICT DO NOTHING`. Mais l'upsert du scraper crée aussi les cinémas à la volée (`on_conflict="name"`, scraper.py:1213) → **ajouter Le Zola ne nécessite pas de migration**, juste un `entry["cinema"]="Le Zola"`.
- `002_storage_pdfs.sql` : bucket de stockage pour les PDF Comoedia.
- `003_dedup_imdb_id.sql` : index unique **partiel** `films_imdb_id_key` sur `imdb_id` (où non nul) → filet de sécurité DB de la dédup par imdb_id (I1). ⚠️ **À appliquer APRÈS** `scripts/merge_duplicate_films.py --apply` (l'index échoue tant que des doublons d'imdb_id subsistent). Genèse : *Exploration — Dédup inter-sources* (vault Obsidian).
- `004_backdrop_trailer.sql` (2026-07-25) : colonnes `films.backdrop` (visuel paysage TMDB) et `films.trailer` (URL YouTube) pour la fiche détail. Purement **additif** — ne touche ni invariant ni clé de dédup. Idempotent (`ADD COLUMN IF NOT EXISTS`), mais **à appliquer avant** le scrape suivant, sinon l'upsert `films` échoue sur des colonnes inconnues.
- `005_evenements.sql` (2026-07-29) : le modèle de l'onglet Événements — voir § dédié ci-dessous.

---

## Schéma Événements (`005_evenements.sql`)

```
evenements (id, cle UNIQUE, type, forme, titre, description,
            date_debut, date_fin, "precision", affiche_url, source, source_url)
   ▲                                   ▲
   │ evenement_id                      │ evenement_id
evenement_films (film_id NULLABLE →films, titre, titre_key, affiche_url, ordre,
                 UNIQUE(evenement_id, titre_key))
evenement_seances (cinema_id →cinemas, date, heure,
                   seance_id NULLABLE →seances, film_id NULLABLE →films,
                   titre_film, invite, description, resa_url,
                   UNIQUE NULLS NOT DISTINCT (evenement_id, cinema_id, date, heure, titre_film))

evenement_mois (mois PK 'YYYY-MM', selection_seed, resume_segments jsonb, resume_generated_at)
```

⚠️ **`evenement_seances` est dénormalisée, et ce n'est pas un raccourci.** Les scrapers ne ramènent qu'**une semaine** de séances (Lumière `?week=`, PDF hebdo Comoedia) alors que les pages événement annoncent jusqu'à ~9 semaines : une FK obligatoire vers `seances` serait insatisfiable pour la majorité des dates affichées — et un film sans séance de la semaine n'existe même pas dans `films` (les films naissent des séances). L'événement est donc **autoportant** : il porte ses propres dates, et `seance_id`/`film_id` se remplissent **opportunistement**, run après run.

Trois conséquences à ne pas défaire :
- `evenement_films.titre` existe **parce que** `film_id` peut rester NULL : sans lui, un film hors semaine scrapée serait invisible au niveau 2, alors que l'interface lui réserve l'état « Séances non encore annoncées » (le régime normal pour septembre-octobre).
- `NULLS NOT DISTINCT` sur la clé des créneaux : sans ça, deux créneaux sans date (fréquent) ne dédupliqueraient jamais et l'upsert empilerait des doublons à chaque run.
- `evenements.cle` (clé de dédup `type|identité|mois`, cf. `event_dedup_key`) rend l'upsert **idempotent** entre deux runs. Bucket mensuel volontaire : une source qui corrige sa date d'un jour ne doit pas créer une 2e ligne.
- Contrainte `forme` : renseignée **uniquement** quand `type = 'festival'`, NULL sinon (`evenements_forme_chk`).

**Bucket `affiches`** (public read, écriture service-role) : les affiches maison des festivals vivent sur des CDN qui refusent le hotlink (403) et dont les URLs tournent → `_rapatrie_affiche()` les copie au premier run et sert l'URL publique Supabase. Même motif que `002_storage_pdfs.sql`, **sans** l'upload anonyme.

**Application** : `python3 scripts/apply_schema.py 005_evenements.sql` (nécessite `DATABASE_URL` dans `.env`), ou copier-coller dans l'éditeur SQL Supabase. Tant que la migration n'est pas appliquée, le scraper log des avertissements et le front sert le repli `programme.json` — rien ne casse.

---

## Upsert (`upsert_all_to_supabase`, scraper.py:1185)

Pour chaque film : upsert `cinemas` (par nom) → upsert `films` (clé I1) → upsert `seances` (clé film+cinema+date+heure). Les `heure` sont normalisées en `HH:MM:00`. Chaque upsert a un fallback `select` si `.data` revient vide.

---

## CI — 3 workflows (`.github/workflows/`)

| Workflow | Cron | Rôle | Notes |
|---|---|---|---|
| **scraper.yml** | `0 20 */2 * *` (un jour sur deux, 20h UTC) | run scraper → commit `chore: mise à jour programme` sur `main` | Cadence J/2 choisie pour reprendre toute maj sous ≤48h sans angle mort (CHANGELOG 2026-07-08). Secrets : OMDB/TMDB/SUPABASE + **ANTHROPIC_API_KEY (optionnel**, résumé du mois de l'onglet Événements : absent ⇒ warning et bloc absent, rien d'autre ne change). Peut `exit 4` (garde-fou I6). |
| **cleanup.yml** | `0 3 * * 4` (jeudi 3h UTC) | purge séances anciennes + films orphelins | `scripts/cleanup_old_seances.py --days 10`. Garde la table sous le plafond REST (invariant I8). Dispatch manuel avec `dry_run`. |
| **pages.yml** | sur push `main` | déploie `index.html` sur GitHub Pages | Pages ne déploie que `main` → pas d'URL de preview PR (cf. *Workflow* (vault Obsidian)) |

⚠️ **Un bot commit sur `main` en continu** (scraper) → on ne peut pas verrouiller `main` derrière des PR obligatoires. Convention : préfixes `chore:` = data (bruit bot), `feat:`/`fix:` = vrai code. Détails process : *Workflow* (vault Obsidian).

---

## Scripts (`scripts/`)

| Script | Usage | Récurrence |
|---|---|---|
| `cleanup_old_seances.py` | purge J−N (défaut 10) + films orphelins ; pagine pour contourner le plafond 1000 | via cleanup.yml (hebdo) |
| `apply_schema.py` | applique les migrations SQL | one-shot / setup |
| `migrate_json_to_supabase.py` | import initial `programme.json` → Supabase | one-shot (historique) |
| `fix_titles_case.py` | ré-applique `_titlecase_fr` aux titres déjà en base (corrige les all-caps Comoedia) | one-shot |
| `merge_duplicate_films.py` | fusionne les films dédoublés déjà en base par `imdb_id` (réassigne les séances vers la ligne canonique, supprime les surnuméraires) ; **dry-run par défaut**, `--apply` pour exécuter ; garde-fou `--year-tol` | one-shot (à lancer avant la migration 003) |
| `test_supabase.py` | vérif connexion / creds | debug |
| `sync_preview.sh` | copie vers `/tmp/cineinde_preview/` (sandbox macOS) | preview locale |

---

## Chantier futur — table `evenements`

Pour automatiser l'onglet Événements (*Exploration — Événements* (vault Obsidian)) : nouvelle table `evenements (id, cinema_id, type, titre, date, heure, lieu, description, source, url_resa)` + parsers scraper + purge des events passés (réutiliser le cleanup). Migration `003_*` à créer. Feature **transverse** — voir aussi [Frontend](frontend.md) (remplacement de `EVENTS_DATA`) et [Pipeline de données](pipeline.md) (nouveaux parsers).
