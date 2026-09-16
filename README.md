# Landscape Data Pipeline

[![tests](https://github.com/Denger878/landscape-data-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/Denger878/landscape-data-pipeline/actions/workflows/tests.yml)

An ETL pipeline and serverless REST API that serves curated, high-resolution landscape photos with photographer credit and location metadata.

**[Live API](https://landscape-data-pipeline.vercel.app/api/random)** · Built to power [Study Tour](https://github.com/Denger878/study-tour)

## Highlights

- **Ingests** photos from the Unsplash API across 30 search queries, plus hand-picked photos fetched by ID
- **Cleans** 308 raw records down to **297** (96.4% pass rate) by removing duplicates and enforcing landscape orientation, aspect ratio ≥ 1.3 and width ≥ 1920px
- **Extracts locations** from Unsplash metadata, falling back to whole-word keyword matching against 55 landmarks. Matches that conflict with the country named in the text are rejected
- **Loads** the cleaned data into SQLite with an atomic rebuild, so the database always matches the pipeline output
- **Serves** it through a read-only Flask API deployed as a Vercel serverless function
- **Tested** with pytest (34 tests) on GitHub Actions

## Architecture

```
Unsplash API
     │
     ▼
ingest.py ──► data/raw_metadata.json        fetch search results + manual picks
     │
     ▼
clean.py ───► data/cleaned_metadata.json    dedupe, validate, refine locations
     │        data/cleaning_report.txt
     ▼
load_sqlite.py ──► db/images.db             rebuild SQLite database
     │
     ▼
api.py ─────► Vercel                        read-only Flask API
```

`config.py` holds all paths, thresholds and search queries. `location.py` holds the location extraction shared by ingest and clean.

## Dataset

| Metric | Value |
| --- | --- |
| Images | 297 |
| Photographers | 211 |
| Average resolution | 5466 × 3586 px |
| Minimum width | 2092 px |
| Images with a country | 49 (16.5%) |
| Countries represented | 10 |

## API

Base URL: `https://landscape-data-pipeline.vercel.app`

| Endpoint | Description |
| --- | --- |
| `GET /api/random` | Random image |
| `GET /api/random/location` | Random image with location data |
| `GET /api/stats` | Collection statistics |
| `GET /api/health` | Health check |

CORS is enabled for all origins.

### Example response

```json
{
  "success": true,
  "data": {
    "id": "BbHH4PES5E8",
    "imageUrl": "https://images.unsplash.com/photo-...",
    "caption": "Bryce Canyon, United States",
    "photographer": {
      "name": "...",
      "username": "...",
      "profile": "https://unsplash.com/@..."
    },
    "unsplashLink": "https://unsplash.com/photos/..."
  }
}
```

`caption` is `null` when an image has no location data. Errors return `{"success": false, "error": "..."}` with a 404 or 500 status.

## Local development

```bash
git clone https://github.com/Denger878/landscape-data-pipeline.git
cd landscape-data-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

python api.py      # http://localhost:5001
pytest             # run the test suite
```

### Rebuilding the dataset

Requires an [Unsplash access key](https://unsplash.com/developers) in `.env`:

```bash
echo "UNSPLASH_ACCESS_KEY=your_key" > .env

python ingest.py                 # full ingest (downloads images to data/images/)
python ingest.py --manual-only   # only re-fetch MANUAL_PHOTO_IDS into the raw data
python clean.py
python load_sqlite.py
```

## Deployment

The API is deployed on Vercel. `pyproject.toml` points Vercel at the Flask app (`api:app`), and `.vercelignore` keeps the raw data and tests out of the function bundle. Pushing to `main` triggers a new deployment.

## Tech stack

Python · Flask · SQLite · Unsplash API · Vercel · pytest · GitHub Actions

## License

MIT. Photos are served through Unsplash's CDN, and every response credits the photographer as the [Unsplash API guidelines](https://help.unsplash.com/en/articles/2511245-unsplash-api-guidelines) require.
