#!/usr/bin/env python3
"""Inline design/tokens.json dans design.html (galerie du design system).

La galerie lit ses données depuis un bloc <script id="tokens-data"> inliné —
pas de fetch, donc elle marche partout (file://, n'importe quel serveur, prod).
Ce script garde ce bloc synchronisé avec tokens.json.

Usage :
    python3 design/build_gallery.py           # (ré)inline les données
    python3 design/build_gallery.py --check    # échoue si design.html n'est pas à jour
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TOKENS = ROOT / "design" / "tokens.json"
GALLERY = ROOT / "design.html"

PATTERN = re.compile(
    r'(<script id="tokens-data" type="application/json">)(.*?)(</script>)', re.S
)


def _payload() -> str:
    data = json.loads(TOKENS.read_text(encoding="utf-8"))
    # compact, et < échappé pour ne jamais fermer le <script> par accident
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")


def render(html: str) -> str:
    if not PATTERN.search(html):
        sys.exit('ERREUR : bloc <script id="tokens-data"> introuvable dans design.html.')
    return PATTERN.sub(lambda m: m.group(1) + _payload() + m.group(3), html, count=1)


def build() -> None:
    GALLERY.write_text(render(GALLERY.read_text(encoding="utf-8")), encoding="utf-8")


def main() -> None:
    html = GALLERY.read_text(encoding="utf-8")
    new = render(html)
    if "--check" in sys.argv:
        if new != html:
            sys.exit("ERREUR : design.html n'est pas à jour avec tokens.json. "
                     "Lancer : python3 design/build_gallery.py")
        print("OK : design.html est à jour avec tokens.json.")
        return
    if new != html:
        GALLERY.write_text(new, encoding="utf-8")
        print("design.html : données de tokens ré-inlinées.")
    else:
        print("design.html déjà à jour.")


if __name__ == "__main__":
    main()
