# Landscape Data Pipeline

A data pipeline and REST API serving high-quality landscape photographs with location metadata.

**[Live API](https://landscape-data-pipeline-production.up.railway.app/api/random)**

## About

This project collects, cleans, and serves landscape images from Unsplash. Originally built to power [Study Tour](https://github.com/Denger878/study-tour), the API is open for anyone who needs beautiful landscape imagery with location data.

## API Endpoints

### Get Random Image
```
GET /api/random
```
Returns a random landscape image with photographer credit and location.

### Get Random Image with Location
```
GET /api/random/location
```
Returns a random image guaranteed to have location data.

### Database Statistics
```
GET /api/stats
```
Returns collection statistics including total images and location coverage.

### Health Check
```
GET /api/health
```
Returns API status.

## Response Example

```json
{
  "success": true,
  "data": {
    "id": "abc123",
    "imageUrl": "https://images.unsplash.com/...",
    "caption": "Lofoten Islands, Norway",
    "photographer": {
      "name": "John Doe",
      "username": "johndoe",
      "profile": "https://unsplash.com/@johndoe"
    },
    "unsplashLink": "https://unsplash.com/photos/abc123"
  }
}
```

## Tech Stack

- **API:** Flask, Flask-CORS
- **Database:** SQLite
- **Data Source:** Unsplash API
- **Deployment:** Railway

## Pipeline Architecture

```
ingest.py → clean.py → load_sqlite.py → app.py
```

1. **Ingest** — Fetches images from Unsplash API with location extraction
2. **Clean** — Removes duplicates, validates dimensions, filters landscape orientation
3. **Load** — Stores cleaned metadata in SQLite database
4. **Serve** — Flask API serves random images from database

## Local Development

```bash
# Clone the repo
git clone https://github.com/Denger878/landscape-data-pipeline.git
cd landscape-data-pipeline

# Install dependencies
pip install -r requirements.txt

# Run the API
python app.py
```

API runs at `http://localhost:5001`

## Data Quality

- 297 curated landscape images
- 96.4% pass rate from raw data
- Minimum 1920px width
- Landscape orientation only (aspect ratio > 1.3)

## License

MIT

Images served via Unsplash API — photographer credit included in all responses per Unsplash guidelines.
