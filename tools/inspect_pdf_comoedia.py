#!/usr/bin/env python3
"""
Inspecteur du PDF Comoedia — à rejouer à chaque changement de maquette.

L'éditeur du cinéma refait son gabarit sans préavis (trois changements d'un coup
en septembre 2026, cf. CHANGELOG). Le scraper, lui, ne dit que « 0 film » : il
constate la panne sans montrer la structure. Cet outil montre la structure.

    python3 tools/inspect_pdf_comoedia.py <fichier.pdf | url>

Affiche : la géométrie des colonnes de la page éditoriale, la grille horaire
rubrique par rubrique, et l'inventaire des renvois de notes. Ne modifie rien.
"""
import re
import sys
import urllib.request
from collections import Counter

UA = {"User-Agent": "Mozilla/5.0", "Accept-Language": "fr-FR"}


def charger(source: str) -> "object":
    import io
    if source.startswith(("http://", "https://")):
        req = urllib.request.Request(source, headers=UA)
        return io.BytesIO(urllib.request.urlopen(req, timeout=60).read())
    return source


def gouttieres(page, seuil: int = 2) -> list:
    """Colonnes de la page éditoriale, repérées par les bandes verticales vides.

    Un PDF ne dit pas « ceci est une colonne » : il pose des mots à des abscisses.
    Une gouttière est une tranche de 20 pt que presque aucun mot ne commence.
    """
    mots = page.extract_words()
    if not mots:
        return []
    h = Counter(int(m["x0"] // 20) * 20 for m in mots)
    vides = [x for x in range(0, int(page.width), 20) if h.get(x, 0) <= seuil]
    # Ne garder qu'une borne par bande contiguë de tranches vides.
    bornes = [x for i, x in enumerate(vides) if i == 0 or x - vides[i - 1] > 20]
    return [b for b in bornes if 40 < b < page.width - 40]


def colonnes_texte(page, bornes: list) -> list:
    """Texte de la page regroupé colonne par colonne, dans l'ordre de lecture."""
    mots = page.extract_words(extra_attrs=["size"])
    for m in mots:
        m["col"] = sum(m["x0"] > b for b in bornes)
    mots.sort(key=lambda m: (m["col"], round(m["top"] / 3), m["x0"]))
    sorties, ligne, y = [], [], None
    for m in mots:
        rupture = y is None or abs(m["top"] - y) > 3 or (ligne and ligne[-1]["col"] != m["col"])
        if rupture:
            if ligne:
                sorties.append((ligne[0]["col"], max(x["size"] for x in ligne),
                                " ".join(x["text"] for x in ligne)))
            ligne, y = [], m["top"]
        ligne.append(m)
    if ligne:
        sorties.append((ligne[0]["col"], max(x["size"] for x in ligne),
                        " ".join(x["text"] for x in ligne)))
    return sorties


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__.strip())
        return 2
    try:
        import pdfplumber
    except ImportError:
        print("pdfplumber non installé — lancez : pip install pdfplumber")
        return 2

    with pdfplumber.open(charger(sys.argv[1])) as pdf:
        print(f"{len(pdf.pages)} page(s)")
        for i, page in enumerate(pdf.pages):
            tb = page.extract_table()
            print(f"\n{'═' * 70}\nPAGE {i + 1} — {round(page.width)}×{round(page.height)} pt, "
                  f"tableau : {len(tb) if tb else 0} ligne(s)")

            if tb:
                _grille(tb)
            else:
                bornes = gouttieres(page)
                print(f"  gouttières détectées à {bornes} pt "
                      f"→ {len(bornes) + 1} colonnes")
                tailles = Counter(round(t, 1) for _, t, _ in colonnes_texte(page, bornes))
                print(f"  tailles de police : {tailles.most_common(6)}")
                print("  — 12 premières lignes reconstruites —")
                for col, taille, txt in colonnes_texte(page, bornes)[:12]:
                    marque = "TITRE" if taille >= 14 else "     "
                    print(f"   c{col} {marque} {txt[:70]}")
    return 0


def _grille(tb: list) -> None:
    """Grille horaire : rubriques, entête, renvois de notes."""
    entete = " | ".join((c or "")[:8] for c in tb[0])
    print(f"  entête : {entete}")

    rubrique, notes, total = None, Counter(), 0
    for r in tb[1:]:
        if r[0]:
            # La rubrique est écrite à la verticale : pdfplumber la rend à l'envers.
            rubrique = r[0][::-1].replace("\n", " ")
            print(f"  ── rubrique « {rubrique} »")
        titre = (r[1] or "").split("\n")[0]
        renvois = []
        for cell in r[2:]:
            for m in re.finditer(r"\b\d{1,2}[h:]\d{2}(\d)?", cell or ""):
                total += 1
                if m.group(1):
                    notes[m.group(1)] += 1
                    renvois.append(m.group(1))
        suffixe = f"  ← renvois {sorted(set(renvois))}" if renvois else ""
        print(f"     {titre[:44]:44}{suffixe}")

    print(f"\n  {total} séances, {sum(notes.values())} porteuses d'un renvoi "
          f"{dict(sorted(notes.items()))}")
    if any(int(n) >= 10 for n in notes):
        print("  ⚠ renvoi à deux chiffres : « 20:3010 » est indécidable "
              "(20:30 + note 10, ou 20:301 + note 0)")


if __name__ == "__main__":
    sys.exit(main())
