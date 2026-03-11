-- Storage bucket for Comoedia PDF uploads
-- Allows anonymous upload (anon key), public read

INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'pdfs',
  'pdfs',
  true,
  10485760, -- 10 MB max
  ARRAY['application/pdf']
)
ON CONFLICT (id) DO NOTHING;

-- Public read (anyone can GET the file URL)
CREATE POLICY "pdfs_public_read"
  ON storage.objects FOR SELECT
  USING (bucket_id = 'pdfs');

-- Anon upload (anyone with anon key can INSERT)
CREATE POLICY "pdfs_anon_upload"
  ON storage.objects FOR INSERT
  WITH CHECK (bucket_id = 'pdfs');
