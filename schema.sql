CREATE TABLE IF NOT EXISTS images (
  id TEXT PRIMARY KEY,
  
  -- Image URLs
  image_url TEXT NOT NULL,
  download_url TEXT NOT NULL,  -- Full resolution download link
  page_url TEXT,               -- Link to Unsplash page
  
  -- Display info (what your website needs)
  location_name TEXT,           -- e.g., "Yosemite Valley"
  country TEXT,                 -- e.g., "United States"
  description TEXT,             -- Alt text / description
  
  -- Photographer credit (legally required by Unsplash)
  photographer_name TEXT NOT NULL,
  photographer_username TEXT,
  
  -- Image metadata
  width INTEGER NOT NULL,
  height INTEGER NOT NULL,
  color TEXT,                   -- Dominant color (for loading placeholders)
  
  -- Pipeline metadata
  source TEXT NOT NULL DEFAULT 'unsplash',
  query TEXT,                   -- Search term used
  downloaded INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Index for quick filtering by location
CREATE INDEX IF NOT EXISTS idx_location ON images(location_name, country);

-- Index for landscape filtering (width > height)
CREATE INDEX IF NOT EXISTS idx_dimensions ON images(width, height);