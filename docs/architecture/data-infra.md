# Architecture — Données & Infra

> Schéma Supabase, clés de dédup, RLS, workflows CI, scripts de maintenance. Les invariants transverses vivent dans [Vue d'ensemble](README.md).
> Dernière mise à jour : 2026-07-10

---

## Schéma Supabase (`supabase/migrations/001_initial.sql`)

Trois tables, modèle **film dédupliqué / séances par cinéma** :

```
cinemas (id, name UNIQUE, slug UNIQUE)
   ▲
   │ cinema_id (FK, ON DELETE CASCADE)
films (id, titre, titre_original, annee, realisateur, duree,
       genres[], synopsis, imdb_id, poster, imdb_rating, "cast", source,
       UNIQUE(titre, annee, realisateur))          ← clé de dédup (invariant I1)
   ▲
   │ film_id (FK, ON DELETE CASCADE)
seances (id, film_id, cinema_id, date, heure, version, resa_url,
         UNIQUE(film_id, cinema_id, date, heure))  ← clé de dédup séance
```

- **Index** : `seances(film_id)`, `seances(cinema_id)`, `seances(date)`.
- **RLS** (invariant I7) : policies `SELECT USING (true)` sur les 3 tables → **lecture publique**. Aucune policy d'écriture → seul le **service-role** (scraper) écrit. Le front (clé anon) ne peut que lire.
- **Seed** : les 4 cinémas actuels sont insérés `ON CONFLICT DO NOTHING`. Mais l'upsert du scraper crée aussi les cinémas à la volée (`on_conflict="name"`, scraper.py:1213) → **ajouter Le Zola ne nécessite pas de migration**, juste un `entry["cinema"]="Le Zola"`.
- `002_storage_pdfs.sql` : bucket de stockage pour les PDF Comoedia.

---

## Upsert (`upsert_all_to_supabase`, scraper.py:1185)

Pour chaque film : upsert `cinemas` (par nom) → upsert `films` (clé I1) → upsert `seances` (clé film+cinema+date+heure). Les `heure` sont normalisées en `HH:MM:00`. Chaque upsert a un fallback `select` si `.data` revient vide.

---

## CI — 3 workflows (`.github/workflows/`)

| Workflow | Cron | Rôle | Notes |
|---|---|---|---|
| **scraper.yml** | `0 20 */2 * *` (un jour sur deux, 20h UTC) | run scraper → commit `chore: mise à jour programme` sur `main` | Cadence J/2 choisie pour reprendre toute maj sous ≤48h sans angle mort (CHANGELOG 2026-07-08). Secrets : OMDB/TMDB/SUPABASE. Peut `exit 4` (garde-fou I6). |
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
| `test_supabase.py` | vérif connexion / creds | debug |
| `sync_preview.sh` | copie vers `/tmp/cineinde_preview/` (sandbox macOS) | preview locale |

---

## Chantier futur — table `evenements`

Pour automatiser l'onglet Événements (*Exploration — Événements* (vault Obsidian)) : nouvelle table `evenements (id, cinema_id, type, titre, date, heure, lieu, description, source, url_resa)` + parsers scraper + purge des events passés (réutiliser le cleanup). Migration `003_*` à créer. Feature **transverse** — voir aussi [Frontend](frontend.md) (remplacement de `EVENTS_DATA`) et [Pipeline de données](pipeline.md) (nouveaux parsers).
