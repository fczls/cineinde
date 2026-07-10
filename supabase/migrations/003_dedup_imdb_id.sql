-- Dédup inter-sources par imdb_id (identifiant canonique TMDB/OMDb)
--
-- Contexte : la clé de dédup historique UNIQUE(titre, annee, realisateur)
-- (invariant I1) crée des lignes `films` en double dès que deux sources
-- décrivent le même film avec une casse/orthographe/année différente
-- (mesuré 2026-07-10 : 18 titres dédoublés sur ~46). L'`imdb_id` résout
-- chaque film à un identifiant unique et sert donc de clé de dédup primaire
-- (repli sur la clé brute quand l'id est absent). Cf. exploration Obsidian
-- « Dédup inter-sources (doublons films) ».
--
-- Cet index unique PARTIEL (WHERE imdb_id IS NOT NULL) est le filet de
-- sécurité au niveau base : il garantit qu'un même imdb_id ne peut plus
-- exister sur deux lignes `films`. Les lignes sans imdb_id (séances
-- spéciales hors TMDB, vrais homonymes non enrichis) restent régies par la
-- clé brute (titre, annee, realisateur) et ne sont pas contraintes ici.
--
-- ⚠️ ORDRE D'APPLICATION : cet index échoue si des doublons d'imdb_id
-- existent déjà en base. Exécuter D'ABORD `scripts/merge_duplicate_films.py
-- --apply` pour fusionner les doublons existants, PUIS cette migration.

CREATE UNIQUE INDEX IF NOT EXISTS films_imdb_id_key
  ON films (imdb_id)
  WHERE imdb_id IS NOT NULL;
