# Cineinde — Cinémas Lyon

Site programme des cinémas Le Comoedia et Cinémas Lumière (Terreaux, Bellecour, Fourmi) à Lyon.

---

## Vue d'ensemble

| Composant | Description |
|-----------|-------------|
| **Frontend** | `index.html` — programme (navigation flottante, vues Détaillée/Liste & 1J/7J, onglet événements) ; charge Supabase (source principale), fallback `programme.json` |
| **Scraper** | `scraper.py` — scrape Comoedia PDF + Lumière, produit `programme.json` et upsert Supabase |
| **Base de données** | Supabase (PostgreSQL) — source principale du frontend |
| **CI** | GitHub Actions — scraper un jour sur deux (20h UTC) |

---

## Structure du projet

```
cineinde/
├── index.html              # Frontend (programme + onglet événements)
├── programme.json          # Données scrapées (committées par CI)
├── pdf_state.json          # État du scraper (committé par CI)
├── scraper.py              # Scraper principal (Comoedia + Lumière)
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

- **Le Comoedia** : `https://www.cinema-comoedia.com/programme-accessible/` (Gatsby)
- **Cinémas Lumière** : `https://www.cinemas-lumiere.com/calendrier-general.html`

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

### Enrichissement

Le scraper enrichit les films via **OMDb** et **TMDB** (posters, synopsis, notes). Optionnel : sans clés API, les champs restent vides.

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
