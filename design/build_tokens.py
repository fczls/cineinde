#!/usr/bin/env python3
"""Génère src/tokens.css depuis design/tokens.json (source de vérité DTCG).

Sortie : le bloc :root, délimité par /* @tokens:start */ … /* @tokens:end */,
écrit dans src/tokens.css. C'est build_ui.py qui assemble ensuite index.html.

Règles de nommage des variables CSS générées :
    color.*      -> --{clé}         (ex. color.bg    -> --bg)
    fontFamily.* -> --font-{clé}    (ex. fontFamily.display-italic -> --font-display-italic)
    fontSize.*   -> --fs-{clé}      (ex. fontSize.xs -> --fs-xs)
    radius.*     -> --radius-{clé}  (ex. radius.pill -> --radius-pill)
    breakpoint.* -> --breakpoint-{clé} (utilisable en max-width/width ; ⚠️ PAS dans
                    @media, où var() est interdit par CSS — y écrire le littéral)

Usage :
    python3 design/build_tokens.py           # (ré)écrit src/tokens.css
    python3 design/build_tokens.py --check    # échoue si src/tokens.css n'est pas à jour
"""
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
TOKENS = SCRIPT_DIR / "tokens.json"
OUT = ROOT / "src" / "tokens.css"

START = "/* @tokens:start */"
END = "/* @tokens:end */"

# groupe DTCG -> préfixe de variable CSS (None = non généré en CSS)
CSS_PREFIX = {"color": "", "fontFamily": "font-", "fontSize": "fs-", "radius": "radius-",
              "breakpoint": "breakpoint-", "spacing": None}
GROUP_LABEL = {"color": "Couleurs", "fontFamily": "Familles de caractères",
               "fontSize": "Tailles de police", "radius": "Rayons",
               "breakpoint": "Seuils responsive (hors @media)"}


def _tokens_of(group: dict):
    for key, node in group.items():
        if key.startswith("$"):
            continue
        yield key, node["$value"]


def build_css(tokens: dict) -> str:
    """Bloc :root complet (marqueurs inclus), terminé par un saut de ligne."""
    lines = [
        START,
        "/* ⚙️  GÉNÉRÉ depuis design/tokens.json — ne pas éditer à la main.",
        "   Régénérer : python3 design/build_tokens.py */",
        ":root {",
    ]
    for group, prefix in CSS_PREFIX.items():
        if prefix is None or group not in tokens:
            continue
        lines.append(f"  /* {GROUP_LABEL[group]} */")
        for key, value in _tokens_of(tokens[group]):
            lines.append(f"  --{prefix}{key}: {value};")
    lines.append("}")
    lines.append(END)
    return "\n".join(lines) + "\n"


def build() -> str:
    """(Ré)écrit src/tokens.css. Rend le contenu généré."""
    css = build_css(json.loads(TOKENS.read_text(encoding="utf-8")))
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(css, encoding="utf-8")
    return css


def main() -> None:
    css = build_css(json.loads(TOKENS.read_text(encoding="utf-8")))
    if "--check" in sys.argv:
        current = OUT.read_text(encoding="utf-8") if OUT.exists() else ""
        if current != css:
            sys.exit("ERREUR : src/tokens.css n'est pas à jour avec tokens.json. "
                     "Lancer : python3 design/build_tokens.py")
        print("OK : src/tokens.css est à jour avec tokens.json.")
        return
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(css, encoding="utf-8")
    print(f"src/tokens.css régénéré ({css.count('--')} variables).")


if __name__ == "__main__":
    main()
