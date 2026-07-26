#!/usr/bin/env python3
"""Inline les données du design system dans design.html (galerie).

Deux blocs <script> inlinés, tenus à jour par ce script :
    #tokens-data           <- design/tokens.json           (vue « Tokens »)
    #tokens-changelog-data <- design/tokens-changelog.json (vue « Changelog »)

La galerie lit ses données depuis ces blocs — pas de fetch, donc elle marche
partout (file://, n'importe quel serveur, prod). Le changelog, lui, est dérivé
de l'historique git par design/build_tokens_changelog.py (à relancer après un
commit qui touche les tokens).

Usage :
    python3 design/build_gallery.py           # (ré)inline les données
    python3 design/build_gallery.py --check    # échoue si design.html n'est pas à jour
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GALLERY = ROOT / "design.html"
SOURCES = {
    "tokens-data": ROOT / "design" / "tokens.json",
    "tokens-changelog-data": ROOT / "design" / "tokens-changelog.json",
}


def _pattern(block_id: str) -> re.Pattern:
    return re.compile(
        rf'(<script id="{block_id}" type="application/json">)(.*?)(</script>)', re.S
    )


def _payload(path) -> str:
    data = json.loads(path.read_text(encoding="utf-8"))
    # compact, et < échappé pour ne jamais fermer le <script> par accident
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")


def render(html: str) -> str:
    for block_id, path in SOURCES.items():
        pattern = _pattern(block_id)
        if not pattern.search(html):
            sys.exit(f'ERREUR : bloc <script id="{block_id}"> introuvable dans design.html.')
        if not path.exists():
            sys.exit(f"ERREUR : {path.relative_to(ROOT)} manquant. "
                     "Lancer : python3 design/build_tokens_changelog.py")
        html = pattern.sub(lambda m: m.group(1) + _payload(path) + m.group(3), html, count=1)
    return html


def build() -> None:
    GALLERY.write_text(render(GALLERY.read_text(encoding="utf-8")), encoding="utf-8")


def main() -> None:
    html = GALLERY.read_text(encoding="utf-8")
    new = render(html)
    if "--check" in sys.argv:
        if new != html:
            sys.exit("ERREUR : design.html n'est pas à jour avec les données du DS. "
                     "Lancer : python3 design/build_gallery.py")
        print("OK : design.html est à jour avec les données du design system.")
        return
    if new != html:
        GALLERY.write_text(new, encoding="utf-8")
        print("design.html : données du design system ré-inlinées.")
    else:
        print("design.html déjà à jour.")


if __name__ == "__main__":
    main()
