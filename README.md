# Landscape Data Pipeline

ETL pipeline for collecting and curating landscape images with geographic metadata from Unsplash API. Built to demonstrate data engineering fundamentals: API integration, data validation, and database design.

## Features

- Automated image collection from Unsplash API with rate limiting
- Smart location extraction from image descriptions
- Data validation and quality control (aspect ratio, resolution, orientation)
- SQLite database with optimized indexes
- Configurable search queries and validation rules

## Tech Stack

- **Python 3.8+** - Core pipeline logic
- **SQLite** - Relational database
- **Unsplash API** - Image source
- **Logging** - Professional output formatting

## Project Structure

```
landscape-data-pipeline/
├── config.py           # Centralized configuration
├── ingest.py          # API data collection
├── clean.py           # Data validation and cleaning
├── load_sqlite.py     # Database loading
├── schema.sql         # Database schema
├── requirements.txt   # Python dependencies
├── .env.example       # Environment template
└── .gitignore
```

## Setup

### Prerequisites

- Python 3.8 or higher
- Unsplash API access key ([Get one here](https://unsplash.com/developers))

### Installation

1. Clone the repository:
```bash
git clone https://github.com/Denger878/landscape-data-pipeline.git
cd landscape-data-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment:
```bash
cp .env.example .env
# Edit .env and add your UNSPLASH_ACCESS_KEY
```

## Usage

Run the complete pipeline:

```bash
# Step 1: Fetch images from Unsplash
python3 ingest.py

# Step 2: Clean and validate data
python3 clean.py

# Step 3: Load into SQLite database
python3 load_sqlite.py
```

### Configuration

Modify `config.py` to adjust:
- `TARGET_IMAGE_COUNT` - Number of images to collect
- `MIN_WIDTH` - Minimum image resolution
- `MIN_ASPECT_RATIO` - Minimum width/height ratio
- `SEARCH_QUERIES` - Landscape types to search for

## Database Schema

```sql
CREATE TABLE images (
  id TEXT PRIMARY KEY,
  image_url TEXT NOT NULL,
  location_name TEXT,
  country TEXT,
  photographer_name TEXT NOT NULL,
  width INTEGER NOT NULL,
  height INTEGER NOT NULL,
  -- Additional fields...
);

-- Indexes for optimized queries
CREATE INDEX idx_location ON images(location_name, country);
CREATE INDEX idx_dimensions ON images(width, height);
```

## Example Queries

```sql
-- Get random landscape image
SELECT * FROM images ORDER BY RANDOM() LIMIT 1;

-- Find images from specific country
SELECT * FROM images WHERE country = 'Iceland';

-- High-resolution images only
SELECT * FROM images WHERE width >= 4000;
```

## Pipeline Results

Typical execution produces:
- **300 images** collected from Unsplash
- **~95% data quality** after validation
- **~10-15% location coverage** (varies by search terms)

### Validation Rules

Images are filtered based on:
1. Successful download
2. Landscape orientation (width > height)
3. Aspect ratio ≥ 1.3 (prevents near-square images)
4. Resolution ≥ 1920px width
5. Required metadata fields present

## Development

### Project Timeline

- **Day 1:** API integration and data collection
- **Day 2:** Data cleaning and validation
- **Day 3:** Database design and loading

### Future Enhancements

- [ ] PostgreSQL support for production deployment
- [ ] REST API layer for data access
- [ ] Automated testing suite
- [ ] Docker containerization
- [ ] Additional image sources (Pexels, Pixabay)

## License

Images are sourced from Unsplash and subject to the [Unsplash License](https://unsplash.com/license).

## Acknowledgments

Built as a data engineering portfolio project demonstrating ETL pipeline development, API integration, and database design.

---

**Author:** Max Deng  
**GitHub:** [Denger878](https://github.com/Denger878)
