# Cineinde — Cinémas Lyon

Site programme des cinémas Le Comoedia et Cinémas Lumière (Terreaux, Bellecour, Fourmi) à Lyon.

---

## Vue d'ensemble

| Composant | Description |
|-----------|-------------|
| **Frontend** | `index.html` — programme (navigation flottante, vues Détaillée/Liste & 1J/7J) et **onglet Évènements** (résumé du mois, sélection, programmation mensuelle, détail) ; routing par URL ; charge Supabase (source principale), fallback `programme.json` |
| **Scraper** | `scraper.py` — scrape Comoedia PDF + Lumière + Zola (séances) et Comoedia + Lumière (évènements), produit `programme.json` et upsert Supabase |
| **Base de données** | Supabase (PostgreSQL) — source principale du frontend |
| **CI** | GitHub Actions — scraper un jour sur deux (20h UTC) |

---

## Structure du projet

```
cineinde/
├── index.html              # Frontend — ⚙️ GÉNÉRÉ par build_ui.py, ne pas éditer à la main
├── build_ui.py             # Assemble src/ → index.html
├── src/                    # Sources du frontend (à éditer ici)
│   ├── template.html       # Squelette : markup + JS + emplacements CSS
│   ├── components.css      # Styles des composants (édition manuelle)
│   └── tokens.css          # ⚙️ GÉNÉRÉ depuis design/tokens.json
├── design/                 # Design system
│   ├── tokens.json         # Source de vérité des tokens (DTCG)
│   ├── build_tokens.py     # tokens.json → src/tokens.css
│   └── DSDS.md             # Conventions (nommage, etc.)
├── programme.json          # Données scrapées (committées par CI)
├── pdf_state.json          # État du scraper (committé par CI)
├── scraper.py              # Scraper principal (Comoedia + Lumière)
├── tests/                  # Tests (stdlib unittest + node, aucune dépendance)
│   ├── test_events.py      # Pipeline évènements : catégorisation, dédup, jointure
│   └── chip_dates.test.mjs # Fonctions pures du front (chip de date, tri, tirage)
├── requirements.txt        # Dépendances Python
├── .env.example            # Template variables d'environnement
│
├── .github/workflows/
│   └── scraper.yml         # Cron hebdomadaire + workflow_dispatch
│
├── tools/                  # Outils de dev / debug (hors pipeline)
│   ├── inspect_html.py     # Debug : analyse la structure HTML
│   ├── serve.py            # Serveur de dev local
│   └── setup_cron.ai.sh    # Installation cron local (mercredi 1h)
│
├── scripts/                # Scripts ops (schéma, migration, nettoyage…)
│
├── docs/                   # architecture/ + design-system/
│
└── supabase/
    └── migrations/         # Schéma : cinemas, films, seances
```

## Build du frontend

`index.html` est un **fichier généré** — ne pas l'éditer directement (toute modif serait
écrasée au prochain build). On édite les sources dans `src/` et `design/`, puis :

```bash
python3 build_ui.py          # tokens.css + index.html + galerie design.html
python3 build_ui.py --check  # (CI) échoue si index.html n'est pas à jour avec src/
```

`build_ui.py` régénère aussi la **galerie du design system** (`design.html`) : `python3 tools/serve.py`
puis ouvrir `http://localhost:4173/design.html`.

- **Changer une couleur / taille / rayon** → `design/tokens.json`, puis `python3 build_ui.py`.
- **Changer un style de composant** → `src/components.css`.
- **Changer le markup ou le JS** → `src/template.html`.
```

---

## Démarrage rapide

### 1. Environnement

```bash
python3 -m venv .venv
source .venv/bin/activate   # ou .venv\Scripts\activate sur Windows
pip install -r requirements.txt
cp .env.example .env
# Éditer .env avec vos clés (voir Variables d'environnement)
```

### 2. Lancer le scraper

```bash
# Test sans écriture
python3 scraper.py --dry-run --debug

# Générer programme.json
python3 scraper.py --output programme.json
```

### 3. Consulter le frontend

Ouvrir `index.html` dans un navigateur (ou via un serveur local). Le frontend charge **Supabase** en priorité ; en cas d'indisponibilité, il utilise `programme.json` puis les données de démonstration.

Pour activer Supabase : renseigner `SUPABASE_URL` et `SUPABASE_ANON_KEY` dans la section CONFIG de `index.html` (clé anon = publique, safe pour le frontend).

---

## Scraper

### Sources

**Séances**

- **Le Comoedia** : PDF hebdomadaire (`/horaires-semaine-complete/`)
- **Cinémas Lumière** : `https://www.cinemas-lumiere.com/calendrier-general.html`
- **Le Zola** : `https://www.lezola.com/films-a-laffiche/`

**Évènements**

- **Le Comoedia** : `https://www.cinema-comoedia.com/tous-les-evenements` (+ le JSON de chaque fiche)
- **Cinémas Lumière** : `evenement.html` et `rendez-vous.html` (⚠️ jamais `avant-premieres.html`, périmée)

### Options

| Option | Description |
|--------|-------------|
| `--output PATH` | Chemin du fichier JSON (défaut : `programme.json`) |
| `--dry-run` | Ne pas écrire le fichier |
| `--debug` | Logs verbeux |
| `--no-omdb` | Désactiver l'enrichissement OMDb |
| `--no-lumiere` | Ne pas scraper les Cinémas Lumière |
| `--no-filter` | Ne pas filtrer par semaine (pour tests) |
| `--file PATH` | Utiliser un fichier HTML local (Comoedia) |
| `--no-zola` | Ne pas scraper Le Zola |
| `--no-events` | Ne pas scraper les évènements |
| `--events-only` | Ne scraper QUE les évènements (films relus depuis `programme.json`) |
| `--force-resume` | Régénérer le résumé du mois même s'il est récent |

### Enrichissement

Le scraper enrichit les films via **OMDb** et **TMDB** (posters, synopsis, notes). Optionnel : sans clés API, les champs restent vides.

Le **résumé du mois** de l'onglet Évènements est produit par l'API Claude (`ANTHROPIC_API_KEY`, optionnel) : sans clé, le bloc est simplement absent du site.

---

## Tests

Aucune dépendance à installer — stdlib Python et node.

```bash
python3 -m unittest discover -s tests   # pipeline évènements (catégorisation, dédup, jointure)
node tests/chip_dates.test.mjs          # fonctions pures du front (chip de date, tri, tirage)
python3 design/check_tokens.py          # lint du design system
```

---

## Supabase (optionnel)

### Schéma

- **cinemas** : Le Comoedia, Lumière Terreaux/Bellecour/Fourmi
- **films** : titre, année, réalisateur, synopsis, poster, etc.
- **seances** : film_id, cinema_id, date, heure, version (VF/VOSTFR)

### Workflow

1. **Appliquer le schéma** (connexion directe PostgreSQL) :
   ```bash
   # Définir DATABASE_URL dans .env (Supabase Dashboard → Settings → Database)
   python3 scripts/apply_schema.py
   ```

2. **Migrer programme.json → Supabase** :
   ```bash
   python3 scripts/migrate_json_to_supabase.py [--json path/to/programme.json]
   ```

3. **Vérifier** :
   ```bash
   python3 scripts/test_supabase.py
   ```

---

## Variables d'environnement

| Variable | Requis | Description |
|----------|--------|-------------|
| `SUPABASE_URL` | Supabase | URL du projet |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase | Clé service (migration) |
| `DATABASE_URL` | apply_schema | Connexion PostgreSQL directe |
| `OMDB_API_KEY` | Optionnel | Enrichissement OMDb |
| `TMDB_API_KEY` | Optionnel | Enrichissement TMDB |
| `SUPABASE_ANON_KEY` | Optionnel | Lecture publique (frontend) |

---

## Cron local

Pour exécuter le scraper chaque mercredi à 1h00 :

```bash
bash tools/setup_cron.ai.sh [chemin_scraper] [chemin_sortie_json]
# Exemple : bash tools/setup_cron.ai.sh /srv/comedia/scraper.py /var/www/comedia/programme.json
```

---

## CI (GitHub Actions)

- **Déclenchement** : un jour sur deux à 20h00 UTC (`cron: '0 20 */2 * *'`) + manuel (`workflow_dispatch`)
- **Actions** : lance le scraper, commit et push `programme.json`
- **Secrets** : `OMDB_API_KEY`, `TMDB_API_KEY` (optionnels)

---

## Debug HTML

Si la structure des sites sources change :

```bash
python3 tools/inspect_html.py [--url URL] [--file fichier.html]
```

Affiche les classes et IDs pertinents pour adapter le scraper.

---

## Format programme.json

```json
{
  "generated_at": "2026-03-08T23:49:51",
  "sources": ["https://...", "https://..."],
  "films": [
    {
      "titre": "...",
      "titreOriginal": null,
      "annee": 2025,
      "realisateur": "...",
      "duree": 100,
      "genres": [],
      "synopsis": null,
      "imdbId": null,
      "seances": [
        { "date": "2026-03-08", "heure": "20:50", "version": "VOSTFR" }
      ],
      "source": "comoedia",
      "cinema": "Le Comoedia"
    }
  ]
}
```

---

## Dépendances

- `supabase` — client Supabase
- `python-dotenv` — chargement `.env`
- `psycopg2-binary` — connexion PostgreSQL (apply_schema)

Python ≥ 3.9 recommandé.
