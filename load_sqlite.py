"""Database loading module."""
import json
import sqlite3
import logging

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def setup_database():
    config.DB_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(config.DATABASE_FILE)
    return conn, conn.cursor()


def create_tables(cursor):
    with open(config.SCHEMA_FILE, 'r') as f:
        cursor.executescript(f.read())
    logger.info("Schema created")


def load_cleaned_data():
    with open(config.CLEANED_METADATA_FILE, 'r') as f:
        data = json.load(f)
    logger.info(f"Loaded {len(data)} records")
    return data


def insert_images(cursor, data):
    insert_sql = """
    INSERT INTO images (
        id, image_url, download_url, page_url,
        location_name, country, description,
        photographer_name, photographer_username,
        width, height, color, source, query, downloaded
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    inserted, skipped = 0, 0
    
    for item in data:
        try:
            cursor.execute(insert_sql, (
                item.get('id'), item.get('image_url'), item.get('download_url'),
                item.get('page_url'), item.get('location_name'), item.get('country'),
                item.get('description'), item.get('photographer_name'),
                item.get('photographer_username'), item.get('width'),
                item.get('height'), item.get('color'),
                item.get('source', 'unsplash'), item.get('query'),
                item.get('downloaded', 0)
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1
    
    if skipped:
        logger.warning(f"Skipped {skipped} duplicates")
    logger.info(f"Inserted {inserted} images")
    return inserted


def verify_database(cursor):
    cursor.execute("SELECT COUNT(*) FROM images")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM images WHERE location_name IS NOT NULL")
    with_location = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM images WHERE country IS NOT NULL")
    with_country = cursor.fetchone()[0]
    
    cursor.execute("SELECT AVG(width), AVG(height) FROM images")
    avg_width, avg_height = cursor.fetchone()
    
    cursor.execute("""
        SELECT country, COUNT(*) as count 
        FROM images WHERE country IS NOT NULL 
        GROUP BY country ORDER BY count DESC LIMIT 5
    """)
    countries = cursor.fetchall()
    
    logger.info(f"Verified: {total} images | {with_location} with location | {with_country} with country")
    logger.info(f"Avg dimensions: {avg_width:.0f}x{avg_height:.0f}px")
    if countries:
        logger.info(f"Top countries: {', '.join([f'{c[0]} ({c[1]})' for c in countries])}")


def main():
    logger.info("Starting database load")
    
    conn, cursor = setup_database()
    create_tables(cursor)
    data = load_cleaned_data()
    insert_images(cursor, data)
    conn.commit()
    verify_database(cursor)
    conn.close()
    
    logger.info("Database load complete")


if __name__ == '__main__':
    main()
