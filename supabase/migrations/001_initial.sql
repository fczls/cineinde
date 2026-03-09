-- Comedia cinema programme schema
-- Films: deduplicated by titre+annee+realisateur
-- Seances: film_id + cinema_id + date + heure

DROP TABLE IF EXISTS seances;
DROP TABLE IF EXISTS films;
DROP TABLE IF EXISTS cinemas;

CREATE TABLE cinemas (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL UNIQUE,
  slug TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE films (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  titre TEXT NOT NULL,
  titre_original TEXT,
  annee INTEGER,
  realisateur TEXT,
  duree INTEGER,
  genres TEXT[] DEFAULT '{}',
  synopsis TEXT,
  imdb_id TEXT,
  poster TEXT,
  imdb_rating NUMERIC(3,1),
  "cast" TEXT,
  source TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(titre, annee, realisateur)
);

CREATE TABLE seances (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  film_id UUID NOT NULL REFERENCES films(id) ON DELETE CASCADE,
  cinema_id UUID NOT NULL REFERENCES cinemas(id) ON DELETE CASCADE,
  date DATE NOT NULL,
  heure TIME NOT NULL,
  version TEXT,
  resa_url TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(film_id, cinema_id, date, heure)
);

CREATE INDEX idx_seances_film ON seances(film_id);
CREATE INDEX idx_seances_cinema ON seances(cinema_id);
CREATE INDEX idx_seances_date ON seances(date);

-- RLS: public read, no client write
ALTER TABLE cinemas ENABLE ROW LEVEL SECURITY;
ALTER TABLE films ENABLE ROW LEVEL SECURITY;
ALTER TABLE seances ENABLE ROW LEVEL SECURITY;

CREATE POLICY "cinemas_read" ON cinemas FOR SELECT USING (true);
CREATE POLICY "films_read" ON films FOR SELECT USING (true);
CREATE POLICY "seances_read" ON seances FOR SELECT USING (true);

-- Seed cinemas
INSERT INTO cinemas (name, slug) VALUES
  ('Le Comoedia', 'comoedia'),
  ('Lumière Terreaux', 'lumiere-terreaux'),
  ('Lumière Bellecour', 'lumiere-bellecour'),
  ('Lumière Fourmi', 'lumiere-fourmi')
ON CONFLICT (name) DO NOTHING;
