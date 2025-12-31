"""
Database loading module for landscape image pipeline.
Loads cleaned metadata into SQLite database.
"""
import json
import sqlite3
import logging

import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def setup_database():
    """Create database directory and initialize database."""
    config.DB_DIR.mkdir(exist_ok=True)
    
    conn = sqlite3.connect(config.DATABASE_FILE)
    cursor = conn.cursor()
    
    logger.info(f"Database initialized: {config.DATABASE_FILE}")
    return conn, cursor


def create_tables(cursor):
    """Create tables using schema.sql."""
    with open(config.SCHEMA_FILE, 'r') as f:
        schema = f.read()
    
    cursor.executescript(schema)
    logger.info("Database schema created")


def load_cleaned_data():
    """Load cleaned metadata from cleaning phase."""
    with open(config.CLEANED_METADATA_FILE, 'r') as f:
        data = json.load(f)
    logger.info(f"Loaded {len(data)} cleaned records")
    return data


def insert_images(cursor, data):
    """Insert image metadata into database."""
    insert_sql = """
    INSERT INTO images (
        id, image_url, download_url, page_url,
        location_name, country, description,
        photographer_name, photographer_username,
        width, height, color,
        source, query, downloaded
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    inserted = 0
    skipped = 0
    
    for item in data:
        try:
            cursor.execute(insert_sql, (
                item.get('id'),
                item.get('image_url'),
                item.get('download_url'),
                item.get('page_url'),
                item.get('location_name'),
                item.get('country'),
                item.get('description'),
                item.get('photographer_name'),
                item.get('photographer_username'),
                item.get('width'),
                item.get('height'),
                item.get('color'),
                item.get('source', 'unsplash'),
                item.get('query'),
                item.get('downloaded', 0)
            ))
            inserted += 1
                
        except sqlite3.IntegrityError:
            skipped += 1
    
    if skipped > 0:
        logger.warning(f"Skipped {skipped} duplicate IDs")
    
    logger.info(f"Inserted {inserted} images into database")
    return inserted


def verify_database(cursor):
    """Run verification queries on database."""
    # Count total
    cursor.execute("SELECT COUNT(*) FROM images")
    total = cursor.fetchone()[0]
    
    # Count with location
    cursor.execute("SELECT COUNT(*) FROM images WHERE location_name IS NOT NULL")
    with_location = cursor.fetchone()[0]
    
    # Count with country
    cursor.execute("SELECT COUNT(*) FROM images WHERE country IS NOT NULL")
    with_country = cursor.fetchone()[0]
    
    # Average dimensions
    cursor.execute("SELECT AVG(width), AVG(height) FROM images")
    avg_width, avg_height = cursor.fetchone()
    
    logger.info(f"Verification: {total} images, {with_location} with location, "
                f"{with_country} with country")
    logger.info(f"Average dimensions: {avg_width:.0f}x{avg_height:.0f}px")
    
    # Top countries
    cursor.execute("""
        SELECT country, COUNT(*) as count 
        FROM images 
        WHERE country IS NOT NULL 
        GROUP BY country 
        ORDER BY count DESC 
        LIMIT 5
    """)
    
    countries = cursor.fetchall()
    if countries:
        logger.info(f"Top countries: {', '.join([f'{c[0]} ({c[1]})' for c in countries])}")


def main():
    """Main database loading pipeline."""
    logger.info("Starting database loading pipeline")
    
    conn, cursor = setup_database()
    create_tables(cursor)
    data = load_cleaned_data()
    inserted = insert_images(cursor, data)
    
    conn.commit()
    logger.info("Changes committed to database")
    
    verify_database(cursor)
    conn.close()
    
    logger.info("Database loading complete")
    return inserted


if __name__ == '__main__':
    main()
