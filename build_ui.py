#!/usr/bin/env python3
"""Assemble index.html depuis les sources src/ (fichier GÉNÉRÉ — ne pas éditer à la main).

Chaîne de build :
    design/tokens.json ──(build_tokens.py)──▶ src/tokens.css ─┐
    src/components.css ─────────────────────────────────────┤
    src/template.html  ─────────────────────────────────────┴─▶ index.html

Le template contient deux emplacements, remplacés par le contenu des fichiers CSS :
    /*@@COWORK_TOKENS@@*/      -> src/tokens.css
    /*@@COWORK_COMPONENTS@@*/  -> src/components.css

Usage :
    python3 build_ui.py           # régénère src/tokens.css puis index.html
    python3 build_ui.py --check   # échoue si index.html n'est pas à jour (pour la CI)
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "design"))
import build_tokens  # noqa: E402

TEMPLATE = ROOT / "src" / "template.html"
TOKENS_CSS = ROOT / "src" / "tokens.css"
COMPONENTS_CSS = ROOT / "src" / "components.css"
INDEX = ROOT / "index.html"

P_TOKENS = "/*@@COWORK_TOKENS@@*/"
P_COMPONENTS = "/*@@COWORK_COMPONENTS@@*/"


def assemble() -> str:
    build_tokens.build()  # régénère src/tokens.css depuis tokens.json
    template = TEMPLATE.read_text(encoding="utf-8")
    for marker, path in ((P_TOKENS, TOKENS_CSS), (P_COMPONENTS, COMPONENTS_CSS)):
        if marker not in template:
            sys.exit(f"ERREUR : marqueur {marker} absent de src/template.html.")
        template = template.replace(marker, path.read_text(encoding="utf-8"), 1)
    return template


def main() -> None:
    result = assemble()
    if "--check" in sys.argv:
        current = INDEX.read_text(encoding="utf-8") if INDEX.exists() else ""
        if current != result:
            sys.exit("ERREUR : index.html n'est pas à jour avec src/. "
                     "Lancer : python3 build_ui.py")
        print("OK : index.html est à jour avec src/.")
        return
    INDEX.write_text(result, encoding="utf-8")
    print(f"index.html assemblé depuis src/ ({len(result)} octets).")


if __name__ == "__main__":
    main()
