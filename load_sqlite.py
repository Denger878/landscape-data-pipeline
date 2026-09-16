"""Database loading module.

Rebuilds the SQLite database from the cleaned metadata on every run, so the
database always mirrors the pipeline output.
"""
import json
import sqlite3
import logging
from contextlib import closing

import config

config.setup_logging()
logger = logging.getLogger(__name__)

COLUMNS = [
    'id', 'image_url', 'download_url', 'page_url',
    'location_name', 'country', 'description',
    'photographer_name', 'photographer_username',
    'width', 'height', 'color', 'source', 'query', 'downloaded'
]
DEFAULTS = {'source': 'unsplash', 'downloaded': 0}


def load_cleaned_data():
    with open(config.CLEANED_METADATA_FILE, 'r') as f:
        data = json.load(f)
    logger.info(f"Loaded {len(data)} records")
    return data


def create_tables(conn):
    with open(config.SCHEMA_FILE, 'r') as f:
        conn.executescript(f.read())
    logger.info("Schema created")


def insert_images(conn, data):
    placeholders = ', '.join('?' for _ in COLUMNS)
    rows = [
        tuple(item.get(col, DEFAULTS.get(col)) for col in COLUMNS)
        for item in data
    ]
    conn.executemany(
        f"INSERT INTO images ({', '.join(COLUMNS)}) VALUES ({placeholders})",
        rows
    )
    logger.info(f"Inserted {len(rows)} images")


def verify_database(conn):
    total, with_location, with_country = conn.execute("""
        SELECT COUNT(*), COUNT(location_name), COUNT(country) FROM images
    """).fetchone()
    avg_width, avg_height = conn.execute(
        "SELECT AVG(width), AVG(height) FROM images"
    ).fetchone()
    countries = conn.execute("""
        SELECT country, COUNT(*) AS count
        FROM images WHERE country IS NOT NULL
        GROUP BY country ORDER BY count DESC, country LIMIT 5
    """).fetchall()

    logger.info(f"Verified: {total} images | {with_location} with location | {with_country} with country")
    if total:
        logger.info(f"Avg dimensions: {avg_width:.0f}x{avg_height:.0f}px")
    if countries:
        logger.info(f"Top countries: {', '.join(f'{c} ({n})' for c, n in countries)}")


def main():
    logger.info("Starting database load")
    config.DB_DIR.mkdir(exist_ok=True)
    data = load_cleaned_data()

    # Build into a temp file and swap it in, so a failed load never leaves a
    # half-written database behind
    tmp_file = config.DATABASE_FILE.with_suffix('.db.tmp')
    tmp_file.unlink(missing_ok=True)
    try:
        with closing(sqlite3.connect(tmp_file)) as conn:
            create_tables(conn)
            insert_images(conn, data)
            conn.commit()
            verify_database(conn)
        tmp_file.replace(config.DATABASE_FILE)
    finally:
        tmp_file.unlink(missing_ok=True)

    logger.info("Database load complete")


if __name__ == '__main__':
    main()
