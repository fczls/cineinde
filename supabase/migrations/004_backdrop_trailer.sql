-- Image backdrop (paysage) + bande-annonce pour la fiche détail (frontend)
--
-- Contexte : la refonte desktop de la fiche détail ajoute un bloc visuel
-- à gauche (image du film) et un bouton bande-annonce. TMDB expose déjà, en
-- plus du poster, un `backdrop_path` (visuel paysage) et des vidéos
-- (`/movie/{id}/videos` → clé YouTube). Le scraper (`_apply_tmdb_movie` /
-- `_tmdb_trailer`) renseigne désormais ces deux champs ; ils sont servis via
-- Supabase (source primaire) et `programme.json` (repli), puis mappés côté
-- front en `_backdrop` / `_trailer`.
--
-- ⚠️ À appliquer AVANT le prochain run du scraper : sans ces colonnes,
-- l'upsert `films` échoue (clés inconnues). Idempotent (IF NOT EXISTS).

ALTER TABLE films ADD COLUMN IF NOT EXISTS backdrop TEXT;
ALTER TABLE films ADD COLUMN IF NOT EXISTS trailer  TEXT;
