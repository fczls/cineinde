"""
Repli Lumière : quand le site des Cinémas Lumière ne rend rien (maintenance),
le run lit le PDF hebdo de l'Institut Lumière, et à défaut reconduit les films
Lumière du programme.json précédent au lieu de les effacer du site de secours.
"""
import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("scraper_mod", RACINE / "scraper.py")
S = importlib.util.module_from_spec(_spec)
sys.modules["scraper_mod"] = S
_spec.loader.exec_module(S)


def _json(contenu) -> Path:
    fh = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
    fh.write(contenu if isinstance(contenu, str) else json.dumps(contenu))
    fh.close()
    return Path(fh.name)


class TestLumiereRepli(unittest.TestCase):

    def test_ne_garde_que_les_films_lumiere(self):
        chemin = _json({"films": [
            {"titre": "A", "cinema": "Lumière Fourmi", "source": "lumiere", "seances": []},
            {"titre": "B", "cinema": "Le Comoedia", "source": "comoedia", "seances": []},
            {"titre": "C", "cinema": "Le Zola", "source": "zola", "seances": []},
        ]})
        self.assertEqual([f["titre"] for f in S.lumiere_depuis_repli(chemin)], ["A"])

    def test_fichier_absent_ou_illisible(self):
        self.assertEqual(S.lumiere_depuis_repli("/nonexistent/programme.json"), [])
        self.assertEqual(S.lumiere_depuis_repli(_json("{pas du json")), [])

    def test_programme_json_reel_contient_des_films_lumiere(self):
        films = S.lumiere_depuis_repli(RACINE / "programme.json")
        self.assertTrue(films)
        self.assertTrue(all(f["cinema"].startswith("Lumière") for f in films))


# Extrait de la grille du numéro du 28 oct. 2026, tel que pdfplumber le rend.
GRILLE = [[
    ["", "", "", "Mercredi 28", "Jeudi 29", "Vendredi 30", "Samedi 31",
     "Dimanche 1er", "Lundi 2", "Mardi 3"],
    ["LUMIÈRE terreaux", "", "", "", "", "", "", "", "", ""],
    ["", "", "Le Corset", "10h20, 15h55 , 18h50", "", "", "", "", "13h15", ""],
    ["", "", "Wild Horse Nine", "", "", "", "v", "", "20h30", ""],
    ["LUMIÈRE fourmi", "", "", "", "", "", "", "", "", ""],
    ["", "", "La Bataille De Gaulle\npartie 1 - L'Âge de fer", "15h40", "", "", "", "", "", "21h"],
    ["", "", "Le Corset", "", "10h20", "", "", "", "", ""],
]]


class TestLumierePdf(unittest.TestCase):

    def setUp(self):
        films = S.lumiere_films_from_tables(GRILLE, ref=date(2026, 11, 2))
        self.films = {(f["cinema"], f["titre"]): f for f in films}

    def _heures(self, cinema, titre):
        return [(s["date"], s["heure"]) for s in self.films[(cinema, titre)]["seances"]]

    def test_salle_titre_et_dates(self):
        self.assertEqual(self._heures("Lumière Terreaux", "Le Corset"), [
            ("2026-10-28", "10:20"), ("2026-10-28", "15:55"), ("2026-10-28", "18:50"),
            ("2026-11-02", "13:15"),
        ])

    def test_meme_titre_dans_deux_salles(self):
        self.assertEqual(self._heures("Lumière Fourmi", "Le Corset"), [("2026-10-29", "10:20")])

    def test_titre_sur_deux_lignes_et_heure_ronde(self):
        self.assertEqual(
            self._heures("Lumière Fourmi", "La Bataille De Gaulle partie 1 - L'Âge de fer"),
            [("2026-10-28", "15:40"), ("2026-11-03", "21:00")])

    def test_cellule_parasite_ignoree(self):
        self.assertEqual(self._heures("Lumière Terreaux", "Wild Horse Nine"),
                         [("2026-11-02", "20:30")])

    def test_mercredi_en_fin_de_mois_precedent(self):
        self.assertEqual(S._lumiere_pdf_mercredi("Mercredi 28", date(2026, 11, 2)),
                         date(2026, 10, 28))


if __name__ == "__main__":
    unittest.main()
