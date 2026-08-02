"""
Corrections manuelles de titres (§ data/corrections-films.csv).

Le tableur est édité à la main : ces tests verrouillent le contrat sur lequel
son auteur compte — ce qui est corrigé, ce qui ne l'est pas, et le fait qu'une
règle devenue caduque se signale plutôt que de pourrir en silence.
"""
import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("scraper_mod", RACINE / "scraper.py")
S = importlib.util.module_from_spec(_spec)
sys.modules["scraper_mod"] = S
_spec.loader.exec_module(S)

COLONNES = ["cinema", "titre_source", "titre", "imdb_id", "ignorer", "note"]


def _csv(lignes: list) -> Path:
    fh = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False,
                                     encoding="utf-8", newline="")
    w = csv.DictWriter(fh, fieldnames=COLONNES)
    w.writeheader()
    for l in lignes:
        w.writerow({c: l.get(c, "") for c in COLONNES})
    fh.close()
    return Path(fh.name)


def _film(titre, cinema="Le Comoedia", **kw):
    return dict({"titre": titre, "cinema": cinema}, **kw)


class TestCorrections(unittest.TestCase):

    def test_titre_corrige_et_recherche_relancee(self):
        """Corriger le titre suffit à refaire l'association IMDb.

        L'enrichissement cherche sur `titreOriginal or titre` : laisser
        l'original en place relancerait la requête sur le libellé fautif.
        """
        r = S.charge_corrections(_csv([
            {"cinema": "Le Comoedia", "titre_source": "Memento 2", "titre": "Memento"}]))
        out = S.applique_corrections(
            [_film("Memento 2", titreOriginal="Memento 2")], r)
        self.assertEqual(out[0]["titre"], "Memento")
        self.assertIsNone(out[0]["titreOriginal"])

    def test_entree_ignoree_disparait(self):
        """Ce qui n'est pas un film (nom de cycle capté par le PDF) est retiré."""
        r = S.charge_corrections(_csv([
            {"titre_source": "Tati au Cinéma ! 2", "ignorer": "oui"}]))
        out = S.applique_corrections([_film("Tati au Cinéma ! 2"), _film("Playtime")], r)
        self.assertEqual([f["titre"] for f in out], ["Playtime"])

    def test_imdb_force_quand_il_est_donne(self):
        r = S.charge_corrections(_csv([
            {"titre_source": "Memento 2", "titre": "Memento", "imdb_id": "tt0209144"}]))
        out = S.applique_corrections([_film("Memento 2")], r)
        self.assertEqual(out[0]["imdbId"], "tt0209144")

    def test_imdb_vide_laisse_l_association_decider(self):
        """Colonne vide = on ne fige rien, l'enrichissement tranche."""
        r = S.charge_corrections(_csv([
            {"titre_source": "Memento 2", "titre": "Memento"}]))
        out = S.applique_corrections([_film("Memento 2")], r)
        self.assertIsNone(out[0].get("imdbId"))

    def test_perimetre_par_salle(self):
        """Une règle nommant une salle ne touche pas les autres."""
        r = S.charge_corrections(_csv([
            {"cinema": "Le Comoedia", "titre_source": "Memento 2", "titre": "Memento"}]))
        out = S.applique_corrections(
            [_film("Memento 2", "Le Comoedia"), _film("Memento 2", "Lumière Terreaux")], r)
        self.assertEqual([f["titre"] for f in out], ["Memento", "Memento 2"])

    def test_salle_vide_vaut_toutes_les_salles(self):
        r = S.charge_corrections(_csv([
            {"titre_source": "Memento 2", "titre": "Memento"}]))
        out = S.applique_corrections(
            [_film("Memento 2", "Le Comoedia"), _film("Memento 2", "Le Zola")], r)
        self.assertEqual([f["titre"] for f in out], ["Memento", "Memento"])

    def test_appariement_insensible_casse_et_accents(self):
        """Le tableur est saisi à la main : il ne doit pas exiger l'exactitude."""
        r = S.charge_corrections(_csv([
            {"titre_source": "TATI AU CINEMA ! 2", "ignorer": "oui"}]))
        out = S.applique_corrections([_film("Tati au Cinéma ! 2")], r)
        self.assertEqual(out, [])

    def test_regle_caduque_signalee(self):
        """Une règle qui ne correspond plus à rien doit se voir.

        Sinon le tableur accumule des lignes mortes que personne n'ose retirer.
        """
        r = S.charge_corrections(_csv([
            {"titre_source": "Un titre qui n'existe plus", "titre": "X"}]))
        with self.assertLogs(S.log, level="WARNING") as journal:
            S.applique_corrections([_film("Playtime")], r)
        self.assertIn("Un titre qui n'existe plus", "\n".join(journal.output))

    def test_fichier_absent_est_sans_effet(self):
        """Une correction manquante ne doit jamais faire tomber le pipeline."""
        self.assertEqual(S.charge_corrections(Path("/introuvable/x.csv")), [])
        films = [_film("Playtime")]
        self.assertEqual(S.applique_corrections(list(films), []), films)

    def test_le_fichier_livre_est_lisible(self):
        """Le CSV du dépôt doit rester chargeable — il est édité à la main."""
        regles = S.charge_corrections(S.CORRECTIONS_PATH)
        self.assertTrue(regles, "data/corrections-films.csv illisible ou vide")
        for r in regles:
            self.assertTrue(r["cle"], "titre_source vide")


if __name__ == "__main__":
    unittest.main()
