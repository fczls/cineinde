-- Onglet Événements — modèle de données
--
-- Contexte : l'onglet « Évènements » était une maquette figée (EVENTS_DATA en
-- dur dans le front). Il devient une vraie feature alimentée par le scraper
-- (pages événement Comoedia + Cinémas Lumière). Genèse : « Brief - Onglet
-- Événements » (vault Obsidian, 2026-07-27).
--
-- ⚠️ `evenement_seances` est DÉNORMALISÉE, et ce n'est pas un raccourci.
-- Les scrapers ne ramènent qu'UNE semaine de séances (Lumière `?week=`,
-- PDF hebdo Comoedia) alors que les pages événement annoncent jusqu'à ~9
-- semaines. Une FK obligatoire vers `seances` serait donc impossible à
-- satisfaire pour la majorité des dates affichées — et un film sans séance de
-- la semaine n'existe même pas dans `films` (les films naissent des séances).
-- Conséquence : l'événement est AUTOPORTANT (il porte ses propres dates), et
-- `seance_id` / `film_id` sont remplis OPPORTUNISTEMENT, run après run, à
-- mesure que les séances apparaissent en base.
--
-- Idempotent (IF NOT EXISTS) : rejouable sans casse.

-- ── Événements ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS evenements (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- Clé de dédup stable (source-agnostique) : rend l'upsert idempotent d'un run
  -- à l'autre. Construite côté scraper (cf. `event_dedup_key`) à partir de
  -- l'identité du film ou du titre normalisé + type + bucket temporel ±14 j.
  cle           TEXT NOT NULL UNIQUE,
  type          TEXT NOT NULL,
  forme         TEXT,
  titre         TEXT NOT NULL,
  description   TEXT,
  date_debut    DATE,
  date_fin      DATE,
  "precision"   TEXT NOT NULL DEFAULT 'exact',
  affiche_url   TEXT,
  source        TEXT,
  source_url    TEXT,
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now(),
  CONSTRAINT evenements_type_chk CHECK (
    type IN ('avant_premiere', 'rencontre', 'seance_speciale', 'festival')),
  -- `forme` n'a de sens que sur un festival (sous-classe d'affichage), NULL sinon.
  CONSTRAINT evenements_forme_chk CHECK (
    (type = 'festival' AND forme IN ('festival', 'cycle', 'retrospective', 'jeune_public'))
    OR (type <> 'festival' AND forme IS NULL)),
  CONSTRAINT evenements_precision_chk CHECK (
    "precision" IN ('exact', 'jour', 'mois', 'saison', 'en_cours'))
);

CREATE INDEX IF NOT EXISTS idx_evenements_dates ON evenements(date_debut, date_fin);
CREATE INDEX IF NOT EXISTS idx_evenements_type  ON evenements(type);

-- ── Films liés ────────────────────────────────────────────────────────────
-- `film_id` est NULLABLE : au-delà de la semaine scrapée, le film n'existe pas
-- encore dans `films` (il naît de ses séances). On garde donc le titre de la
-- source pour pouvoir AFFICHER la ligne de film au niveau 2 (état « Séances non
-- encore annoncées ») et rattacher plus tard, quand la séance arrive.
CREATE TABLE IF NOT EXISTS evenement_films (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  evenement_id  UUID NOT NULL REFERENCES evenements(id) ON DELETE CASCADE,
  film_id       UUID REFERENCES films(id) ON DELETE SET NULL,
  titre         TEXT NOT NULL,
  titre_key     TEXT NOT NULL,   -- titre normalisé (miroir de _normalize_title_key)
  affiche_url   TEXT,
  ordre         INTEGER NOT NULL DEFAULT 0,
  created_at    TIMESTAMPTZ DEFAULT now(),
  UNIQUE(evenement_id, titre_key)
);

CREATE INDEX IF NOT EXISTS idx_evenement_films_ev   ON evenement_films(evenement_id);
CREATE INDEX IF NOT EXISTS idx_evenement_films_film ON evenement_films(film_id);

-- ── Créneaux ──────────────────────────────────────────────────────────────
-- Le DESCRIPTIF et l'INVITÉ vivent ici, pas sur l'événement : un même événement
-- joué dans deux salles a deux textes différents (« en présence du réalisateur »
-- vs « séance présentée »). Les fusionner en écraserait un.
CREATE TABLE IF NOT EXISTS evenement_seances (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  evenement_id  UUID NOT NULL REFERENCES evenements(id) ON DELETE CASCADE,
  cinema_id     UUID NOT NULL REFERENCES cinemas(id) ON DELETE CASCADE,
  date          DATE,            -- dénormalisée (cf. en-tête)
  heure         TIME,
  seance_id     UUID REFERENCES seances(id) ON DELETE SET NULL,   -- rempli opportunistement
  film_id       UUID REFERENCES films(id) ON DELETE SET NULL,     -- idem
  titre_film    TEXT,            -- quand le créneau porte sur un film identifié
  invite        TEXT,
  description   TEXT,
  resa_url      TEXT,
  created_at    TIMESTAMPTZ DEFAULT now(),
  -- NULLS NOT DISTINCT : sans ça, deux créneaux sans date (fréquent) ne
  -- dédupliqueraient jamais et l'upsert empilerait des doublons à chaque run.
  UNIQUE NULLS NOT DISTINCT (evenement_id, cinema_id, date, heure, titre_film)
);

CREATE INDEX IF NOT EXISTS idx_evenement_seances_ev     ON evenement_seances(evenement_id);
CREATE INDEX IF NOT EXISTS idx_evenement_seances_cinema ON evenement_seances(cinema_id, date);

-- ── Données mensuelles (graine de sélection + résumé) ──────────────────────
-- Une ligne par mois affiché. `selection_seed` est reposé à chaque run : le
-- tirage de la sélection est CALCULÉ à l'affichage à partir de (seed, mois,
-- filtres) — on ne stocke jamais un rang figé, sinon la règle de comptage
-- « n éligibles ⇒ n cartes » ne tiendrait plus sous filtre.
-- `resume_segments` = tableau de segments typés produit par l'API Claude
-- (cf. scraper.generate_month_summary). NULL = pas de résumé → le front
-- n'affiche simplement pas le bloc.
CREATE TABLE IF NOT EXISTS evenement_mois (
  mois                TEXT PRIMARY KEY,     -- 'YYYY-MM'
  selection_seed      TEXT NOT NULL,
  resume_segments     JSONB,
  resume_generated_at TIMESTAMPTZ,
  updated_at          TIMESTAMPTZ DEFAULT now()
);

-- ── RLS : lecture publique, écriture service-role only (invariant I7) ──────
ALTER TABLE evenements        ENABLE ROW LEVEL SECURITY;
ALTER TABLE evenement_films   ENABLE ROW LEVEL SECURITY;
ALTER TABLE evenement_seances ENABLE ROW LEVEL SECURITY;
ALTER TABLE evenement_mois    ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'evenements' AND policyname = 'evenements_read') THEN
    CREATE POLICY "evenements_read" ON evenements FOR SELECT USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'evenement_films' AND policyname = 'evenement_films_read') THEN
    CREATE POLICY "evenement_films_read" ON evenement_films FOR SELECT USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'evenement_seances' AND policyname = 'evenement_seances_read') THEN
    CREATE POLICY "evenement_seances_read" ON evenement_seances FOR SELECT USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'evenement_mois' AND policyname = 'evenement_mois_read') THEN
    CREATE POLICY "evenement_mois_read" ON evenement_mois FOR SELECT USING (true);
  END IF;
END $$;

-- ── Bucket des affiches d'événement ────────────────────────────────────────
-- Les affiches maison (festivals, rétrospectives) vivent sur des CDN qui
-- renvoient des 403 en hotlink et dont les URLs tournent : on les RAPATRIE.
-- Même motif que 002_storage_pdfs.sql, mais SANS upload anonyme — seul le
-- scraper (clé service) écrit.
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'affiches',
  'affiches',
  true,
  5242880, -- 5 MB max
  ARRAY['image/jpeg', 'image/png', 'image/webp']
)
ON CONFLICT (id) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies
                 WHERE tablename = 'objects' AND policyname = 'affiches_public_read') THEN
    CREATE POLICY "affiches_public_read"
      ON storage.objects FOR SELECT
      USING (bucket_id = 'affiches');
  END IF;
END $$;
