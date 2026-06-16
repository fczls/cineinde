#!/usr/bin/env bash
#
# sync_preview.sh — Copie le site statique vers /tmp pour la preview locale.
#
# Le serveur de preview de Claude tourne dans un bac à sable (sandbox) qui ne
# peut PAS lire ~/Documents (protection macOS TCC). On sert donc une copie
# depuis /tmp (non protégé). Lancer ce script PUIS démarrer la preview
# (preview_start — launch.json pointe sur /tmp/cineinde_preview/serve.py).
#
# Usage : bash scripts/sync_preview.sh   (ré-exécuter après chaque edit de index.html)
#
set -euo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="/tmp/cineinde_preview"

mkdir -p "$DEST"
cp "$SRC/index.html" "$SRC/programme.json" "$SRC/serve.py" "$DEST/"

echo "✓ Synchronisé → $DEST"
echo "  index.html, programme.json, serve.py"
echo "  (Les données restent live : le front lit Supabase en priorité.)"
echo "  Démarre/recharge ensuite la preview sur le port 4173."
