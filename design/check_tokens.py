#!/usr/bin/env python3
"""Lint du design system : refuse les valeurs brutes hors tokens dans src/components.css.

Contrôles :
  1. Couleur brute (#hex, rgba/rgb) hors allowlist → doit être un var(--…).
  2. font-size / border-radius en px brut → doit être var(--fs-*) / var(--radius-*).
  3. Espacement de rythme (padding / margin / gap) hors grille 2px (valeur impaire > 1).

Ne contrôle PAS : width/height, top/left/right/bottom (positions, dont les positions
précises d'animation), box-shadow, letter-spacing, etc. — le px brut y est légitime.

Usage :
    python3 design/check_tokens.py        # exit ≠ 0 s'il y a des violations

Une valeur légitimement unique (dégradé, teinte rare) s'ajoute à COLOR_ALLOWLIST
ci-dessous et se documente dans docs/design-system/color-mapping.md.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TARGET = ROOT / "src" / "components.css"

# One-offs couleur assumés (dégradés & teintes rares) — voir color-mapping.md.
COLOR_ALLOWLIST = {
    "#1c1608",
    "rgba(207,207,207,1)",
    "rgba(135,135,135,.77)",
    "rgba(102,102,102,0.20)", "rgba(102,102,102,0.40)",
    "rgba(12,11,11,0)", "rgba(12,11,11,.97)",
    "rgba(17,16,16,0)", "rgba(17,16,16,.32)", "rgba(17,16,16,.4)",
    "rgba(120,105,91,0)", "rgba(120,105,91,.30)",
}

COLOR_RE = re.compile(r'#[0-9a-fA-F]{3,6}\b|rgba?\([^)]*\)')
RHYTHM_RE = re.compile(r'\b(padding|margin|gap)[a-z-]*:\s*([^;{}]+)')


def _strip_comments(css: str) -> str:
    # Vide le contenu des /* */ mais préserve les sauts de ligne (n° de ligne exacts).
    return re.sub(r'/\*.*?\*/', lambda m: re.sub(r'[^\n]', ' ', m.group(0)), css, flags=re.S)


def lint(css: str):
    out = []
    for n, line in enumerate(_strip_comments(css).splitlines(), 1):
        for m in COLOR_RE.finditer(line):
            if m.group(0) not in COLOR_ALLOWLIST:
                out.append((n, f"couleur brute `{m.group(0)}` → var(--…) (ou allowlister si one-off)"))
        for prop, tok in (("font-size", "--fs-*"), ("border-radius", "--radius-*")):
            for m in re.finditer(rf'{prop}:\s*\d+px', line):
                out.append((n, f"`{m.group(0)}` en px brut → var({tok})"))
        for m in RHYTHM_RE.finditer(line):
            for px in re.findall(r'\b(\d+)px', m.group(2)):
                v = int(px)
                if v > 1 and v % 2 == 1:
                    out.append((n, f"espacement `{v}px` ({m.group(1)}) hors grille 2px → valeur paire"))
    return out


def main() -> None:
    violations = lint(TARGET.read_text(encoding="utf-8"))
    if violations:
        print(f"✗ check_tokens : {len(violations)} violation(s) dans {TARGET.relative_to(ROOT)} :",
              file=sys.stderr)
        for n, msg in violations:
            print(f"  {TARGET.name}:{n} : {msg}", file=sys.stderr)
        print("\nRègles : design/DSDS.md §2. Tokens : design/tokens.json.", file=sys.stderr)
        sys.exit(1)
    print("✓ check_tokens : aucune valeur brute hors tokens.")


if __name__ == "__main__":
    main()
