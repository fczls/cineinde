#!/usr/bin/env python3
"""
Tests du pipeline Événements (scraper.py).

Aucune dépendance : stdlib `unittest`, aucun accès réseau (fixtures figées
depuis les vraies pages, sondées le 2026-07-27).

    python3 -m unittest discover -s tests

Ces cas viennent de la « definition of done » du brief : ce sont les règles que
l'on n'a pas le droit de casser en retouchant les parsers.
"""
from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import scraper as S  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────
# §4.1 — Rencontre vs séance spéciale
# ─────────────────────────────────────────────────────────────────────────
class TestClassification(unittest.TestCase):
    """Les quatre cas-tests du brief, textes réels."""

    def test_partenariat_organisation_reste_seance_speciale(self):
        texte = "SÉANCE SPÉCIALE - Jeudi 17 septembre à 20h30 au Lumière Terreaux. " \
                "Séance présentée en partenariat avec Écrans Mixtes"
        self.assertEqual(S.classify_event_type(texte), "seance_speciale")

    def test_presentee_par_personne_nommee_est_une_rencontre(self):
        texte = "SÉANCE SPÉCIALE - Mercredi 30 septembre au Lumière Terreaux. " \
                "Séance présentée par l'auteur du livre, Peter Heller. " \
                "En partenariat avec Quais du Polar"
        # Les deux marqueurs cohabitent : la personne présente l'emporte.
        self.assertEqual(S.classify_event_type(texte), "rencontre")

    def test_echange_est_une_rencontre(self):
        texte = "SÉANCE SPÉCIALE - Mardi 15 septembre à 20h30 au Lumière Bellecour. " \
                "Séance suivie d'un échange dans le cadre du festival On Off"
        self.assertEqual(S.classify_event_type(texte), "rencontre")

    def test_seance_presentee_seule_garde_le_type_primaire(self):
        """Piège fréquent chez Lumière : « Séance présentée » n'a pas d'agent."""
        texte = "L'AVANT-PREMIÈRE DU LUNDI - Lundi 17 août à 20h30 au Lumière Terreaux. " \
                "Séance présentée"
        self.assertEqual(S.classify_event_type(texte), "avant_premiere")

    def test_type_explicite_de_la_source_fait_foi(self):
        """Comoedia étiquette : « en présence de » ne dégrade pas une AVP."""
        texte = "En présence du réalisateur Arthur Harari et du comédien Niels Schneider."
        self.assertEqual(S.classify_event_type(texte, "Avant-première"), "avant_premiere")

    def test_forme_nulle_hors_festival(self):
        self.assertIsNone(S.classify_event_forme("Cycle machin", "avant_premiere"))
        self.assertEqual(S.classify_event_forme("Cycle Scary Fourmi", "festival"), "cycle")
        self.assertEqual(
            S.classify_event_forme("Rétrospective intégrale Jacques Tati", "festival"),
            "retrospective")


# ─────────────────────────────────────────────────────────────────────────
# §7.2 / §4.3 — Lecture des périodes
# ─────────────────────────────────────────────────────────────────────────
class TestPeriodes(unittest.TestCase):
    REF = date(2026, 7, 27)

    def test_periode_complete(self):
        self.assertEqual(
            S.parse_event_period("Du 4 juillet au 22 août 2026 au Lumière Fourmi", self.REF),
            ("2026-07-04", "2026-08-22", "exact"))

    def test_periode_a_cheval_sur_deux_annees(self):
        """L'année portée par la borne finale remonte sur une borne de décembre."""
        self.assertEqual(
            S.parse_event_period("Du 20 décembre au 5 janvier 2027", self.REF),
            ("2026-12-20", "2027-01-05", "exact"))

    def test_jusqu_au(self):
        self.assertEqual(
            S.parse_event_period("Jusqu'au 31 août 2026 au Lumière Fourmi", self.REF),
            (None, "2026-08-31", "exact"))

    def test_a_partir_du_est_en_cours(self):
        """Début annoncé, fin inconnue : surtout pas `exact`.

        Une fin déduite des séances scrapées glisserait de semaine en semaine
        (« 29 juillet → 4 août », puis « 5 → 11 août »…). `en_cours` est le
        marqueur que le front lit pour n'afficher que ce qui est su.
        """
        self.assertEqual(
            S.parse_event_period("À partir du 12 août au Lumière Bellecour", self.REF),
            ("2026-08-12", None, "en_cours"))
        self.assertEqual(
            S.parse_event_period("RESSORTIE NATIONALE - Dès le 29 juillet", self.REF),
            ("2026-07-29", None, "en_cours"))

    def test_date_unitaire(self):
        self.assertEqual(
            S.parse_event_period("Mardi 15 septembre à 20h30 au Lumière Bellecour", self.REF),
            ("2026-09-15", "2026-09-15", "exact"))

    def test_actuellement_est_en_cours(self):
        self.assertEqual(S.parse_event_period("Actuellement au Lumière Terreaux", self.REF),
                         (None, None, "en_cours"))

    def test_saison(self):
        self.assertEqual(S.parse_event_period("Cet été, retrouvez l'œuvre de Tati", self.REF),
                         (None, None, "saison"))

    def test_heure(self):
        self.assertEqual(S.parse_event_time("Dimanche 2 août à 11h00"), "11:00")
        self.assertEqual(S.parse_event_time("Lundi 10 août à 20h"), "20:00")
        self.assertIsNone(S.parse_event_time("Actuellement au Lumière Terreaux"))


# ─────────────────────────────────────────────────────────────────────────
# Parsers de source (fixtures réelles, sans réseau)
# ─────────────────────────────────────────────────────────────────────────
COMOEDIA_INCONNUE = {
    "type": "Avant-première",
    "title": "L'Inconnue",
    "startAt": "2026-08-20T20:00:00",
    "endAt": None,
    "shortDescription": "En présence du réalisateur Arthur Harari et du comédien Niels Schneider.",
    "poster": "https://all.web.img.acsta.net/img/d0/20/d020c09c557ed15cce1979d015e96ff6.jpg",
    "description": "<p><strong>AVANT-PREMI&Egrave;RE - JEUDI 20 AO&Ucirc;T &Agrave; 20H00</strong></p>"
                   "<p>En pr&eacute;sence du r&eacute;alisateur <strong>Arthur Harari</strong>.</p>",
}

COMOEDIA_MATOUT = {
    "type": "Avant-première",
    "title": "Ma T'Août Première Avant-Première",
    "startAt": "2026-08-02", "endAt": "2026-08-30",
    "shortDescription": "Chaque dimanche du mois d'août, un film jeune public en avant-première.",
    "poster": "https://cms-assets.webediamovies.pro/production/945/e5f4.jpg",
    "description": (
        "<p><strong>&bull; Dimanche 2 ao&ucirc;t &agrave; 11h00</strong><br>"
        "<a href=\"https://www.cinema-comoedia.com/films/1000045582-le-monde-a-lenvers/?date=2026-08-02\">"
        "<em>Le Monde &agrave; l'envers</em></a> (2026)</p>"
        "<p><strong>&bull; Dimanche 9 ao&ucirc;t &agrave; 11h00</strong><br>"
        "<a href=\"https://www.cinema-comoedia.com/films/1000041813-patouille-et-momo/?date=2026-08-09\">"
        "<em>Patouille et Momo</em></a></p>"
        "<p>Programmation propos&eacute;e avec <a href=\"https://carlottafilms.com/films/evenement/\">"
        "Carlotta Films</a>.</p>"),
}

LUMIERE_ROW_CYCLE = """<tr><td><a href="film/sudden-fear.html"><img class="affiche_film"
 src="https://www.cinemas-lumiere.com/media/sudden-fear-affiche.jpg"></a></td>
<td valign="top"><p><span><strong><span style="text-decoration: underline;"><strong><span>
<span style="color: #df071a;">CYCLE 3 FILMS NOIRS DES ANN&Eacute;ES 50</span> -&nbsp;</span></strong>
Actuellement au Lumi&egrave;re Fourmi</span></strong></span></p>
<p><strong><a href="film/sudden-fear.html"><span><em>Sudden Fear</em></span></a>&nbsp; de David Miller
<br></strong>&Eacute;tats-Unis | 1952 | 1h50 | VOSTF</p>
<p>Lester, un acteur dans le besoin se marie &agrave; une riche dramaturge et d&eacute;cide de la
supprimer avec l'aide de sa ma&icirc;tresse, afin d'h&eacute;riter de son argent.</p>
<p><a href="film/sudden-fear.html"><strong>&gt; Lire la suite</strong></a></p></td></tr>"""

LUMIERE_ROW_TATI = """<tr><td><a href="evenement/retrospective-jacques-tati.html">
<img src="https://www.cinemas-lumiere.com/media/affiche-tati.jpg"></a></td>
<td valign="top"><p><strong><span>Actuellement au</span></strong>&nbsp;Lumi&egrave;re Bellecour</p>
<p><a href="evenement/retrospective-jacques-tati.html"><strong><em>R&eacute;trospective
int&eacute;grale Jacques Tati</em></strong></a></p>
<p>Cet &eacute;t&eacute;, retrouvez l'&oelig;uvre intemporelle du g&eacute;nie de la com&eacute;die
dans de somptueuses versions restaur&eacute;es 4K !</p>
<ul><li><a href="film/jour-de-fete.html"><em>Jour de f&ecirc;te</em></a> (1949)</li>
<li><a href="film/playtime.html"><em>Playtime</em></a> (1967)</li></ul></td></tr>"""


class TestParsersSource(unittest.TestCase):
    REF = date(2026, 7, 27)

    def test_comoedia_evenement_film_unique(self):
        ev = S.comoedia_event_from_json(COMOEDIA_INCONNUE, "https://x/events/1/", self.REF)
        self.assertEqual(ev["type"], "avant_premiere")
        self.assertEqual((ev["date_debut"], ev["date_fin"]), ("2026-08-20", "2026-08-20"))
        self.assertEqual([f["titre"] for f in ev["films"]], ["L'Inconnue"])
        self.assertEqual(ev["creneaux"][0]["heure"], "20:00")
        self.assertEqual(ev["creneaux"][0]["cinema"], "Le Comoedia")

    def test_comoedia_liens_films_donnent_film_date_et_heure(self):
        ev = S.comoedia_event_from_json(COMOEDIA_MATOUT, "https://x/events/2/", self.REF)
        titres = [f["titre"] for f in ev["films"]]
        self.assertIn("Le Monde à l'envers", titres)
        self.assertIn("Patouille et Momo", titres)
        # Un lien /films/ hors domaine Comoedia (distributeur) n'est pas un film.
        self.assertNotIn("Carlotta Films", titres)
        self.assertEqual({c["heure"] for c in ev["creneaux"]}, {"11:00"})
        self.assertEqual((ev["date_debut"], ev["date_fin"]), ("2026-08-02", "2026-08-30"))

    def test_lumiere_ligne_de_cycle_prend_le_nom_du_cycle(self):
        ev = S.lumiere_event_from_row(LUMIERE_ROW_CYCLE, "FILMS CLASSIQUES",
                                      S.URL_LUMIERE_EVENTS, self.REF)
        self.assertEqual(ev["titre"], "Cycle 3 films noirs des années 50")
        self.assertEqual((ev["type"], ev["forme"]), ("festival", "cycle"))
        self.assertEqual([f["titre"] for f in ev["films"]], ["Sudden Fear"])
        self.assertEqual(ev["creneaux"][0]["cinema"], "Lumière Fourmi")
        self.assertEqual(ev["precision"], "en_cours")

    def test_lumiere_titre_porte_par_le_lien_evenement(self):
        ev = S.lumiere_event_from_row(LUMIERE_ROW_TATI, "FILMS CLASSIQUES",
                                      S.URL_LUMIERE_EVENTS, self.REF)
        self.assertEqual(ev["titre"], "Rétrospective intégrale Jacques Tati")
        self.assertEqual((ev["type"], ev["forme"]), ("festival", "retrospective"))
        # L'événement n'est pas son propre film, et ses films sont bien listés.
        self.assertEqual([f["titre"] for f in ev["films"]], ["Jour de fête", "Playtime"])


# ─────────────────────────────────────────────────────────────────────────
# §2 — Dédup inter-sources (film + type + fenêtre ±14 j)
# ─────────────────────────────────────────────────────────────────────────
def _ev(titre, source, debut, films, cinema, type_="avant_premiere", forme=None):
    return S._new_event(
        type=type_, forme=forme, titre=titre, source=source,
        date_debut=debut, date_fin=debut, precision="exact",
        films=[{"titre": f, "dates": [debut] if debut else []} for f in films],
        creneaux=[{"cinema": cinema, "date": debut, "heure": "20:00",
                   "titre_film": films[0] if films else None,
                   "invite": None, "description": None, "resa_url": None}],
    )


class TestDedup(unittest.TestCase):
    def test_notre_salut_reste_deux_evenements(self):
        """Même film, même type, mais 24 jours d'écart : deux événements."""
        events = S.merge_events([
            _ev("Notre salut", "comoedia", "2026-08-28", ["Notre salut"], "Le Comoedia"),
            _ev("Notre salut", "lumiere", "2026-09-21", ["Notre salut"], "Lumière Terreaux"),
        ])
        self.assertEqual(len(events), 2)

    def test_inconnue_devient_un_evenement_a_deux_creneaux(self):
        events = S.merge_events([
            _ev("L'INCONNUE", "comoedia", "2026-08-20", ["L'Inconnue"], "Le Comoedia"),
            _ev("L'Inconnue", "lumiere", "2026-08-24", ["L'Inconnue"], "Lumière Terreaux"),
        ])
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(len(ev["creneaux"]), 2)
        self.assertEqual({c["cinema"] for c in ev["creneaux"]},
                         {"Le Comoedia", "Lumière Terreaux"})
        # Titre canonique : priorité de source Lumière > Comoedia (§9.2), et
        # surtout pas la version en capitales du Comoedia.
        self.assertEqual(ev["titre"], "L'Inconnue")
        self.assertEqual((ev["date_debut"], ev["date_fin"]), ("2026-08-20", "2026-08-24"))

    def test_retrospective_partagee_fusionne_sur_les_films(self):
        tati = ["Jour de fête", "Playtime", "Mon Oncle"]
        events = S.merge_events([
            _ev("Tati au cinéma !", "comoedia", "2026-07-15", tati, "Le Comoedia",
                type_="festival", forme="retrospective"),
            _ev("Rétrospective intégrale Jacques Tati", "lumiere", "2026-07-20", tati,
                "Lumière Bellecour", type_="festival", forme="retrospective"),
        ])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["titre"], "Rétrospective intégrale Jacques Tati")

    def test_festival_englobant_ne_fusionne_pas_une_retrospective(self):
        """« Plein Soleil sur les Classiques » ⊃ les Tati, sans être la rétro."""
        tati = ["Jour de fête", "Playtime", "Mon Oncle"]
        events = S.merge_events([
            _ev("Plein Soleil sur les Classiques", "comoedia", "2026-07-15",
                tati + ["Le Guépard"], "Le Comoedia", type_="festival", forme="festival"),
            _ev("Rétrospective intégrale Jacques Tati", "lumiere", "2026-07-20", tati,
                "Lumière Bellecour", type_="festival", forme="retrospective"),
        ])
        self.assertEqual(len(events), 2)

    def test_cle_stable_et_discriminante(self):
        a, b = S.merge_events([
            _ev("Notre salut", "comoedia", "2026-08-28", ["Notre salut"], "Le Comoedia"),
            _ev("Notre salut", "lumiere", "2026-09-21", ["Notre salut"], "Lumière Terreaux"),
        ])
        self.assertNotEqual(S.event_dedup_key(a), S.event_dedup_key(b))
        self.assertEqual(S.event_dedup_key(a), "avant_premiere|notre salut|2026-08")

    def test_cle_ne_glisse_pas_quand_le_debut_est_rogne(self):
        """Un événement long dont le début est rogné garde la MÊME clé.

        C'est le cas qui créait une 2e ligne tous les mois : la source
        n'annonce plus que la partie à venir, `date_debut` avance, et une clé
        ancrée sur le mois de début change en franchissant un 1er du mois.
        """
        avant = {"type": "festival", "titre": "Cycle Scary Fourmi",
                 "films": [{"titre": "The Thing"}, {"titre": "La Mouche"}],
                 "date_debut": "2026-07-04", "date_fin": "2026-08-22"}
        apres = dict(avant, date_debut="2026-08-01")   # début rogné au run suivant
        self.assertEqual(S.event_dedup_key(avant), S.event_dedup_key(apres))
        self.assertTrue(S.event_dedup_key(avant).endswith("|2026-08"))

    def test_cle_ne_bascule_pas_quand_un_film_se_rattache(self):
        """Un film rattaché plus tard ne doit pas changer la clé.

        C'est le cas « Biennale » : une 1re ligne est créée alors qu'aucun film
        n'est encore rattaché (identité = titre de l'événement), puis un run
        suivant en rattache un — l'identité basculait sur le titre du FILM et
        une 2e ligne naissait. merge_events ne peut rien y faire : il ne
        déduplique qu'au sein d'un run, jamais contre ce qui est déjà en base.
        """
        sans = {"type": "festival", "titre": "Résonance - Biennale de Lyon",
                "films": [], "date_debut": "2026-09-14", "date_fin": "2026-09-14"}
        avec = dict(sans, films=[{"titre": "Les Glaneurs et la glaneuse"}])
        self.assertEqual(S.event_dedup_key(sans), S.event_dedup_key(avec))

    def test_cle_suit_le_titre_pas_le_film(self):
        """Deux événements distincts autour du même film restent distincts."""
        a = {"type": "festival", "titre": "Cycle Kurosawa", "films": [{"titre": "Ran"}],
             "date_debut": "2026-09-01", "date_fin": "2026-09-30"}
        b = dict(a, titre="Nuit du cinéma japonais")
        self.assertNotEqual(S.event_dedup_key(a), S.event_dedup_key(b))

    def test_cle_distingue_toujours_deux_editions(self):
        """Deux éditions qui se terminent des mois différents restent distinctes."""
        a = {"type": "festival", "titre": "Play It Again !", "films": [],
             "date_debut": "2026-09-01", "date_fin": "2026-09-07"}
        b = dict(a, date_debut="2027-09-01", date_fin="2027-09-07")
        self.assertNotEqual(S.event_dedup_key(a), S.event_dedup_key(b))


# ─────────────────────────────────────────────────────────────────────────
# §4.3 — Jointure avec les séances : dater sans jamais inférer
# ─────────────────────────────────────────────────────────────────────────
CYCLE_FILMS_NOIRS = ["Sudden Fear", "Another Man's Poison", "Cast a Dark Shadow"]


def _film_scrape(titre, cinema, dates):
    return {"titre": titre, "cinema": cinema,
            "seances": [{"date": d, "heure": "16:30", "version": "VOSTFR"} for d in dates]}


class TestJointure(unittest.TestCase):
    TODAY = date(2026, 8, 1)

    def _cycle(self):
        return S._new_event(
            type="festival", forme="cycle", titre="Cycle 3 films noirs des années 50",
            source="lumiere", precision="en_cours",
            films=[{"titre": t, "dates": []} for t in CYCLE_FILMS_NOIRS],
            creneaux=[{"cinema": "Lumière Fourmi", "date": None, "heure": None,
                       "titre_film": None, "invite": None, "description": None,
                       "resa_url": None}],
        )

    def test_cycle_remonte_ses_films_et_leurs_seances(self):
        films = [_film_scrape(t, "Lumière Fourmi", ["2026-08-01", "2026-08-04"])
                 for t in CYCLE_FILMS_NOIRS]
        events = S.resolve_dates_from_seances([self._cycle()], films, self.TODAY)
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(len(ev["films"]), 3)
        self.assertTrue(all(f["dates"] for f in ev["films"]))
        self.assertEqual((ev["date_debut"], ev["date_fin"]), ("2026-08-01", "2026-08-04"))
        self.assertEqual(len([c for c in ev["creneaux"] if c["date"]]), 6)
        # La jointure complète les dates (tri, mois couverts, filtrage) mais ne
        # promeut PAS la précision : ces bornes sont celles de la semaine
        # scrapée, pas celles annoncées par la salle. Le front affichera donc
        # « En cours », et non une période qui bougerait chaque semaine.
        self.assertEqual(ev["precision"], "en_cours")

    def test_sans_seance_connue_l_evenement_disparait(self):
        """Pas de fantôme : un événement indatable n'est pas affiché."""
        self.assertEqual(S.resolve_dates_from_seances([self._cycle()], [], self.TODAY), [])

    def test_les_seances_d_une_autre_salle_ne_comptent_pas(self):
        films = [_film_scrape(t, "Lumière Terreaux", ["2026-08-01"]) for t in CYCLE_FILMS_NOIRS]
        self.assertEqual(S.resolve_dates_from_seances([self._cycle()], films, self.TODAY), [])

    def test_une_avant_premiere_datee_n_aspire_pas_les_seances_du_film(self):
        """Une date unique exacte est un ordre du programmateur, pas une enveloppe."""
        avp = _ev("Fjord", "lumiere", "2026-08-17", ["Fjord"], "Lumière Terreaux")
        films = [_film_scrape("Fjord", "Lumière Terreaux",
                              ["2026-08-17", "2026-08-18", "2026-08-25"])]
        ev = S.resolve_dates_from_seances([avp], films, self.TODAY)[0]
        self.assertEqual((ev["date_debut"], ev["date_fin"]), ("2026-08-17", "2026-08-17"))
        self.assertEqual(len(ev["creneaux"]), 1)


class TestFiltrageIngestion(unittest.TestCase):
    TODAY = date(2026, 8, 1)

    def test_le_passe_est_ecarte_a_l_ingestion(self):
        """Aucune source ne purge : c'est à nous de le faire (§3.3)."""
        passe = _ev("Vieux", "comoedia", "2026-07-13", ["Vieux"], "Le Comoedia")
        futur = _ev("À venir", "comoedia", "2026-08-20", ["À venir"], "Le Comoedia")
        gardes = S.filter_events_current([passe, futur], self.TODAY)
        self.assertEqual([e["titre"] for e in gardes], ["À venir"])

    def test_un_festival_en_cours_est_garde(self):
        fest = S._new_event(titre="Little Films Festival", type="festival", forme="festival",
                            date_debut="2026-06-28", date_fin="2026-08-18", source="comoedia")
        self.assertEqual(len(S.filter_events_current([fest], self.TODAY)), 1)


# ─────────────────────────────────────────────────────────────────────────
# §5 — Résumé du mois : le fallback doit être un bloc ABSENT, pas un secours
# ─────────────────────────────────────────────────────────────────────────
class TestResume(unittest.TestCase):
    def test_segments_valides(self):
        self.assertEqual(
            S._valider_segments([{"t": "En ", "s": "mute"}, {"icon": "movie"},
                                 {"t": "Juillet", "s": "strong"}]),
            [{"t": "En ", "s": "mute"}, {"icon": "movie"}, {"t": "Juillet", "s": "strong"}])

    def test_icone_inconnue_est_ignoree_sans_perdre_le_resume(self):
        out = S._valider_segments([{"t": "En ", "s": "mute"}, {"icon": "licorne"}])
        self.assertEqual(out, [{"t": "En ", "s": "mute"}])

    def test_style_invalide_invalide_tout(self):
        self.assertIsNone(S._valider_segments([{"t": "x", "s": "italique"}]))

    def test_forme_non_conforme_invalide_tout(self):
        self.assertIsNone(S._valider_segments("En juillet, retrouvez…"))
        self.assertIsNone(S._valider_segments([]))
        self.assertIsNone(S._valider_segments([{"t": "x" * 500, "s": "mute"}]))

    def test_sans_cle_api_pas_de_resume(self):
        ancienne = S.ANTHROPIC_API_KEY
        S.ANTHROPIC_API_KEY = None
        try:
            self.assertIsNone(S.generate_month_summary([{"titre": "X", "type": "festival"}],
                                                       "2026-08"))
        finally:
            S.ANTHROPIC_API_KEY = ancienne


if __name__ == "__main__":
    unittest.main(verbosity=2)
