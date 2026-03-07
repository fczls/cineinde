#!/usr/bin/env bash
# setup_cron.sh — Installe le cron mercredi 1h00 pour le scraper Comedia
# Usage : bash setup_cron.sh [chemin_absolu_scraper] [chemin_absolu_json_sortie]
#
# Ex : bash setup_cron.sh /srv/comedia/scraper.py /var/www/comedia/programme.json

set -euo pipefail

SCRAPER="${1:-$(realpath scraper.py)}"
OUTPUT="${2:-$(realpath programme.json)}"
PYTHON="${3:-$(which python3)}"
LOGFILE="${4:-/var/log/comedia-scraper.log}"

# Vérifications
if [ ! -f "$SCRAPER" ]; then
  echo "❌  scraper.py introuvable : $SCRAPER"
  exit 1
fi

if ! command -v "$PYTHON" &>/dev/null; then
  echo "❌  python3 introuvable"
  exit 1
fi

# Dépendances Python (stdlib uniquement — pas d'install nécessaire)
# requests et beautifulsoup4 ne sont PAS requis : le scraper utilise
# uniquement urllib et html.parser de la stdlib Python ≥ 3.10

# Ligne cron : mercredi (3) à 1h00
CRON_LINE="0 1 * * 3 $PYTHON $SCRAPER --output $OUTPUT >> $LOGFILE 2>&1"

# Vérifie si la ligne existe déjà
CURRENT_CRONTAB=$(crontab -l 2>/dev/null || true)

if echo "$CURRENT_CRONTAB" | grep -qF "$SCRAPER"; then
  echo "ℹ️   La tâche cron existe déjà pour $SCRAPER"
  echo "     Supprimez-la manuellement avec 'crontab -e' si vous voulez la recréer."
else
  # Ajoute la ligne
  (echo "$CURRENT_CRONTAB"; echo "$CRON_LINE") | crontab -
  echo "✅  Cron installé :"
  echo "    $CRON_LINE"
fi

echo ""
echo "── Crontab actuel ──"
crontab -l

echo ""
echo "── Test manuel ──"
echo "  python3 $SCRAPER --dry-run --debug"
echo "  python3 $SCRAPER --output $OUTPUT"
echo ""
echo "── Inspecter la structure HTML d'abord ──"
echo "  python3 $(dirname $SCRAPER)/inspect_html.py"
