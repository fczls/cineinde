"""
Parsing du tableau PDF Comoedia (§ clean_pdf_table).

Le PDF est refait par l'éditeur du cinéma sans préavis : en septembre 2026 il a
changé d'entête (« MER 16 » au lieu de « MERCREDI. 26 »), de séparateur d'heure
(« 20:30 » au lieu de « 20h00 ») et gagné une colonne de garde portant la
rubrique à la verticale. Chaque forme rencontrée est verrouillée ici, car une
seule d'entre elles qui casse fait tomber le garde-fou (sortie 4) sans que le
reste du pipeline bronche.
"""
import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("scraper_mod", RACINE / "scraper.py")
S = importlib.util.module_from_spec(_spec)
sys.modules["scraper_mod"] = S
_spec.loader.exec_module(S)


class TestFormatSeptembre2026(unittest.TestCase):
    """Entête abrégée, heures en « : », colonne de garde en tête de ligne."""

    ROWS = [
        [None, "", "MER 16", "JEU 17", "VEN 18", "SAM 19", "DIM 20", "LUN 21", "MAR 22"],
        ["STNEMENÈVÉ", "LES ROCHES ROUGES\nVF", "20:301", "-", "-", "-", "-", "-", "-"],
        [None, "L'INVITATION\nVOST", "16:15 / 18:30\n20:55", "-", "-", "-", "-", "-", "-"],
    ]

    def test_entete_abregee_reconnue(self):
        self.assertTrue(S._is_day_header_row(self.ROWS[0]))

    def test_dates_inferees_depuis_les_numeros(self):
        cols = S._infer_col_dates(self.ROWS[0], None)
        self.assertEqual(cols[2], date(2026, 9, 16))
        self.assertEqual(cols[8], date(2026, 9, 22))

    def test_titre_lu_malgre_la_colonne_de_garde(self):
        films = S.clean_pdf_table(self.ROWS, date(2026, 9, 16))
        self.assertEqual([f["titre"] for f in films],
                         ["Les Roches Rouges", "L'Invitation"])

    def test_heures_en_deux_points_et_note_de_bas_de_page(self):
        films = S.clean_pdf_table(self.ROWS, date(2026, 9, 16))
        # « 20:301 » = 20:30 + renvoi de note, pas 20:30 et 1 h du matin.
        self.assertEqual(films[0]["seances"],
                         [{"date": "2026-09-16", "heure": "20:30", "version": "VF"}])
        self.assertEqual([s["heure"] for s in films[1]["seances"]],
                         ["16:15", "18:30", "20:55"])
        self.assertEqual({s["version"] for s in films[1]["seances"]}, {"VOSTFR"})


class TestFormatMars2026(unittest.TestCase):
    """Ancien format : noms de jours complets, heures en « h », titre en col 0."""

    ROWS = [
        ["", "MERCREDI. 26", "JEUDI. 27", "VENDREDI. 28", "SAMEDI. 29",
         "DIMANCHE. 30", "LUNDI. 31", "MARDI. 1ER"],
        ["NOTRE SALUT\nVF", "-", "-", "AVP\n20h001", "-", "-", "-", "-"],
    ]

    def test_entete_complete_reconnue(self):
        self.assertTrue(S._is_day_header_row(self.ROWS[0]))

    def test_seance_en_h_toujours_lue(self):
        films = S.clean_pdf_table(self.ROWS, date(2026, 8, 26))
        self.assertEqual(films[0]["titre"], "Notre Salut")
        self.assertEqual(films[0]["seances"],
                         [{"date": "2026-08-28", "heure": "20:00", "version": "VF"}])


class TestLigneSansJour(unittest.TestCase):
    def test_tableau_sans_entete_ne_renvoie_rien(self):
        self.assertEqual(S.clean_pdf_table([["TITRE", "10:00"]], date(2026, 9, 16)), [])

    def test_ligne_de_prose_nest_pas_une_entete(self):
        # « mars » contient « mar » : les abréviations sont cherchées sur un mot
        # entier, sinon une page de texte passerait pour un planning.
        self.assertFalse(S._is_day_header_row(["du 11 au 17 mars 2026, jeudi soir"]))


if __name__ == "__main__":
    unittest.main()
