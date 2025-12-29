import json
import sqlite3
from pathlib import Path
from datetime import datetime

# Paths
CLEANED_METADATA = Path('data/cleaned_metadata.json')
DB_DIR = Path('db')
DB_FILE = DB_DIR / 'images.db'
SCHEMA_FILE = Path('schema.sql')


def setup_database():
    """Create database directory and initialize database"""
    print("🗄️  Setting up database...\n")
    
    # Create db directory
    DB_DIR.mkdir(exist_ok=True)
    
    # Connect to database (creates file if doesn't exist)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    print(f"✓ Database created at: {DB_FILE}\n")
    
    return conn, cursor


def create_tables(cursor):
    """Create the images table using schema.sql"""
    print("📊 Creating tables...\n")
    
    # Read schema from file
    with open(SCHEMA_FILE, 'r') as f:
        schema = f.read()
    
    # Execute schema (create table + indexes)
    cursor.executescript(schema)
    
    print("✓ Tables and indexes created\n")


def load_cleaned_data():
    """Load cleaned metadata from Day 2"""
    print("📂 Loading cleaned metadata...\n")
    
    with open(CLEANED_METADATA, 'r') as f:
        data = json.load(f)
    
    print(f"✓ Loaded {len(data)} cleaned records\n")
    return data


def insert_images(cursor, data):
    """Insert image metadata into database"""
    print("💾 Inserting images into database...\n")
    
    # SQL insert statement
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
            
            if inserted % 50 == 0:
                print(f"  • Inserted {inserted} images...")
                
        except sqlite3.IntegrityError:
            # Duplicate ID (shouldn't happen after cleaning, but just in case)
            skipped += 1
    
    print(f"\n✓ Inserted {inserted} images")
    if skipped > 0:
        print(f"✗ Skipped {skipped} duplicates\n")
    else:
        print()
    
    return inserted


def verify_database(cursor):
    """Run test queries to verify database"""
    print("✅ Verifying database...\n")
    
    # Count total images
    cursor.execute("SELECT COUNT(*) FROM images")
    total = cursor.fetchone()[0]
    print(f"  • Total images in database: {total}")
    
    # Count images with location
    cursor.execute("SELECT COUNT(*) FROM images WHERE location_name IS NOT NULL")
    with_location = cursor.fetchone()[0]
    print(f"  • Images with location: {with_location}")
    
    # Count images with country
    cursor.execute("SELECT COUNT(*) FROM images WHERE country IS NOT NULL")
    with_country = cursor.fetchone()[0]
    print(f"  • Images with country: {with_country}")
    
    # Get average dimensions
    cursor.execute("SELECT AVG(width), AVG(height) FROM images")
    avg_width, avg_height = cursor.fetchone()
    print(f"  • Average dimensions: {avg_width:.0f} x {avg_height:.0f}px")
    
    # Count by source
    cursor.execute("SELECT source, COUNT(*) FROM images GROUP BY source")
    sources = cursor.fetchall()
    for source, count in sources:
        print(f"  • Source '{source}': {count} images")
    
    # Top 5 countries
    cursor.execute("""
        SELECT country, COUNT(*) as count 
        FROM images 
        WHERE country IS NOT NULL 
        GROUP BY country 
        ORDER BY count DESC 
        LIMIT 5
    """)
    print(f"\n  📍 Top 5 Countries:")
    for country, count in cursor.fetchall():
        print(f"     • {country}: {count} images")
    
    print()


def main():
    """Main database loading pipeline"""
    print("🗄️  DAY 3: Loading Data into SQLite Database\n")
    print("="*60 + "\n")
    
    # Step 1: Setup database
    conn, cursor = setup_database()
    
    # Step 2: Create tables
    create_tables(cursor)
    
    # Step 3: Load cleaned data
    data = load_cleaned_data()
    
    # Step 4: Insert into database
    inserted = insert_images(cursor, data)
    
    # Step 5: Commit changes
    print("💾 Committing changes to database...\n")
    conn.commit()
    print("✓ Changes committed\n")
    
    # Step 6: Verify database
    verify_database(cursor)
    
    # Step 7: Create example queries file
    create_example_queries()
    
    # Close connection
    conn.close()
    
    # Final summary
    print("="*60)
    print("\n🎉 DAY 3 COMPLETE!\n")
    print(f"Summary:")
    print(f"  • Database created: {DB_FILE}")
    print(f"  • Images loaded: {inserted}")
    print(f"  • Example queries: query_examples.sql")
    
    print("\n📊 Next Steps:")
    print("  1. Test queries: sqlite3 db/images.db")
    print("  2. Run example query: sqlite3 db/images.db < query_examples.sql")
    print("  3. Integrate with your study timer website!")
    
    print("\n✅ Your data pipeline is complete! 🎊\n")


if __name__ == '__main__':
    main()