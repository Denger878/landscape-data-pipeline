CREATE TABLE IF NOT EXISTS images (
  id TEXT PRIMARY KEY,
  image_url TEXT NOT NULL,
  download_url TEXT NOT NULL,
  page_url TEXT,
  location_name TEXT,
  country TEXT,
  description TEXT,
  photographer_name TEXT NOT NULL,
  photographer_username TEXT,
  width INTEGER NOT NULL,
  height INTEGER NOT NULL,
  color TEXT,
  source TEXT NOT NULL DEFAULT 'unsplash',
  query TEXT,
  downloaded INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_location ON images(location_name, country);
CREATE INDEX IF NOT EXISTS idx_dimensions ON images(width, height);
