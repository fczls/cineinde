# Comedia Scraper

Scrape automatiquement le programme de **Le Comedia** (Lyon) depuis  
`https://www.cinema-comedia.fr/programme-accessible/`  
chaque **mercredi à 1h00** et produit un `programme.json` consommé par le frontend.

---

## Fichiers

| Fichier | Rôle |
|---|---|
| `scraper.py` | Script principal — scraping + parsing + enrichissement OMDb |
| `inspect_html.py` | Outil d'inspection de la structure HTML (à lancer une fois) |
| `setup_cron.sh` | Installe la tâche cron automatiquement |
| `programme.json` | Fichier généré (gitignorer ou exposer via nginx) |

---

## Dépendances

**Zéro dépendance externe.** Le scraper utilise uniquement la stdlib Python :

- `urllib` — requêtes HTTP
- `html.parser` — parsing HTML
- `json`, `re`, `datetime`, `pathlib`, `logging` — divers

Python **≥ 3.10** requis (pour `str | None` dans les annotations).

---

## Mise en place

### 1. Inspecter la structure HTML (obligatoire au premier lancement)

La structure HTML de Comedia peut différer selon leur thème WordPress.  
Avant de lancer le scraper, inspectez la page pour valider les sélecteurs :

```bash
python3 inspect_html.py
```

Cela affiche les classes CSS, les IDs et des extraits du HTML pour guider
l'adaptation du parseur dans `scraper.py` si nécessaire.

### 2. Configurer la clé OMDb

Créez une clé gratuite sur https://www.omdbapi.com/apikey.aspx  
(1 000 requêtes/jour gratuites, largement suffisant).

Puis dans `scraper.py`, ligne 17 :

```python
OMDB_API_KEY = "votre_cle_ici"
```

### 3. Test manuel

```bash
# Mode dry-run (affiche le JSON sans écrire de fichier)
python3 scraper.py --dry-run

# Mode debug (logs détaillés)
python3 scraper.py --debug --dry-run

# Écriture effective
python3 scraper.py --output /var/www/comedia/programme.json

# Désactiver OMDb (parsing seul)
python3 scraper.py --dry-run --no-omdb
```

### 4. Installer le cron

```bash
chmod +x setup_cron.sh
bash setup_cron.sh /srv/comedia/scraper.py /var/www/comedia/programme.json
```

Ou manuellement via `crontab -e` :

```cron
# Comedia — scraping programme chaque mercredi à 1h00
0 1 * * 3 /usr/bin/python3 /srv/comedia/scraper.py --output /var/www/comedia/programme.json >> /var/log/comedia-scraper.log 2>&1
```

---

## Format de sortie `programme.json`

```json
{
  "generated_at": "2025-03-12T01:00:04.123456",
  "source": "https://www.cinema-comedia.fr/programme-accessible/",
  "films": [
    {
      "titre": "Perfect Days",
      "titreOriginal": "Perfect Days",
      "annee": 2023,
      "realisateur": "Wim Wenders",
      "duree": 124,
      "genres": ["Drame"],
      "synopsis": "À Tokyo, Hirayama…",
      "imdbId": "tt27503384",
      "poster": "https://m.media-amazon.com/images/…",
      "imdbRating": 7.6,
      "seances": [
        { "date": "2025-03-12", "heure": "11:00", "version": "VOSTFR" },
        { "date": "2025-03-12", "heure": "16:30", "version": "VOSTFR" },
        { "date": "2025-03-14", "heure": "19:15", "version": "VOSTFR" }
      ]
    }
  ]
}
```

---

## Adaptation du parseur

Si `inspect_html.py` révèle une structure différente, modifiez la fonction  
`parse_programme()` dans `scraper.py`. Les points d'adaptation principaux :

```python
# Nœuds films (ligne ~150) :
film_nodes = (
    find_nodes(root, tag="article", cls="film")      # ← ajuster selon le HTML réel
    or find_nodes(root, tag="div", cls="film-semaine")
    ...
)

# Nœuds séances (ligne ~220) :
seance_nodes = (
    find_nodes(node, tag="li", cls="seance")         # ← ajuster
    ...
)
```

---

## Intégration avec le frontend

Dans `comedia.html`, remplacez la constante `FILMS_DATA` par un fetch du JSON :

```javascript
async function loadAll() {
  // En prod : fetch du JSON généré par le scraper
  const res   = await fetch('/programme.json');
  const data  = await res.json();
  const today = new Date(); today.setHours(0,0,0,0);

  return data.films.map(f => ({
    ...f,
    note:    f.imdbRating || f.note,
    _poster: f.poster || null,
    color:   '#1a1a1a',
    _sd: f.seances.map(s => ({
      h: s.heure,
      v: s.version,
      date: new Date(s.date + 'T00:00:00'),
    })),
  }));
}
```

---

## Logs

```
2025-03-12 01:00:01 [INFO] ═══════════════════════════════════════════════════════
2025-03-12 01:00:01 [INFO] Comedia Scraper — mercredi 12 mars 2025 01:00
2025-03-12 01:00:01 [INFO] Fetch → https://www.cinema-comedia.fr/programme-accessible/
2025-03-12 01:00:02 [INFO] HTML reçu : 48,234 caractères
2025-03-12 01:00:02 [INFO] Stratégie 1 : 8 nœuds 'film' trouvés
2025-03-12 01:00:02 [INFO] 8 films extraits
2025-03-12 01:00:02 [INFO] Enrichissement OMDb pour 8 films…
2025-03-12 01:00:03 [INFO]   ✓ OMDb : Perfect Days → tt27503384
2025-03-12 01:00:03 [INFO]   ✓ OMDb : Oppenheimer → tt15398776
…
2025-03-12 01:00:05 [INFO] 8 films retenus pour la semaine
2025-03-12 01:00:05 [INFO] ✓ Écrit → /var/www/comedia/programme.json (12,840 octets)
2025-03-12 01:00:05 [INFO] Terminé.
```
