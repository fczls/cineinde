# Cineinde — Cinémas Lyon

Site programme des cinémas Le Comoedia et Cinémas Lumière (Terreaux, Bellecour, Fourmi) à Lyon.

---

## Vue d'ensemble

| Composant | Description |
|-----------|-------------|
| **Frontend** | `index.html` — charge Supabase (source principale), fallback `programme.json` |
| **Scraper** | `scraper.py` — scrape Comoedia PDF + Lumière, produit `programme.json` et upsert Supabase |
| **Base de données** | Supabase (PostgreSQL) — source principale du frontend |
| **CI** | GitHub Actions — scraper hebdomadaire (mercredi 1h UTC) |

---

## Structure du projet

```
comedia/
├── index.html              # Frontend (programme + onglet événements)
├── programme.json          # Données scrapées (committées par CI)
├── scraper.py              # Scraper principal (Comoedia + Lumière)
├── inspect_html.py         # Outil debug pour analyser la structure HTML
├── setup_cron.ai.sh        # Installation cron local (mercredi 1h)
├── requirements.txt       # Dépendances Python
├── .env.example            # Template variables d'environnement
│
├── .github/workflows/
│   └── scraper.yml         # Cron hebdomadaire + workflow_dispatch
│
├── scripts/
│   ├── apply_schema.py     # Applique le schéma SQL sur Supabase
│   ├── migrate_json_to_supabase.py  # Migration programme.json → Supabase
│   └── test_supabase.py    # Vérifie données et API Supabase
│
└── supabase/
    └── migrations/
        └── 001_initial.sql # Schéma : cinemas, films, seances
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
bash setup_cron.ai.sh [chemin_scraper] [chemin_sortie_json]
# Exemple : bash setup_cron.ai.sh /srv/comedia/scraper.py /var/www/comedia/programme.json
```

---

## CI (GitHub Actions)

- **Déclenchement** : mercredi 1h00 UTC (`cron: '0 1 * * 3'`) + manuel (`workflow_dispatch`)
- **Actions** : lance le scraper, commit et push `programme.json`
- **Secrets** : `OMDB_API_KEY`, `TMDB_API_KEY` (optionnels)

---

## Debug HTML

Si la structure des sites sources change :

```bash
python3 inspect_html.py [--url URL] [--file fichier.html]
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
