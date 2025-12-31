"""
REST API for landscape image data.
Serves random images with location metadata from SQLite database.
"""
import sqlite3
import logging
from flask import Flask, jsonify
from flask_cors import CORS

import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend access


def get_db_connection():
    """Create database connection."""
    conn = sqlite3.connect(config.DATABASE_FILE)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    return conn


@app.route('/api/random', methods=['GET'])
def get_random_image():
    """
    Get a random landscape image.
    
    Returns:
        JSON with image data including URL, location, and photographer credit
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, image_url, location_name, country, 
                   photographer_name, photographer_username, page_url
            FROM images
            ORDER BY RANDOM()
            LIMIT 1
        """)
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return jsonify({'error': 'No images found'}), 404
        
        # Build caption
        caption = None
        if row['location_name'] and row['country']:
            caption = f"{row['location_name']}, {row['country']}"
        elif row['country']:
            caption = row['country']
        elif row['location_name']:
            caption = row['location_name']
        
        return jsonify({
            'success': True,
            'data': {
                'id': row['id'],
                'imageUrl': row['image_url'],
                'caption': caption,
                'photographer': {
                    'name': row['photographer_name'],
                    'username': row['photographer_username'],
                    'profile': f"https://unsplash.com/@{row['photographer_username']}"
                },
                'unsplashLink': row['page_url']
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching random image: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/random/location', methods=['GET'])
def get_random_with_location():
    """
    Get a random landscape image that has location data.
    
    Returns:
        JSON with image data - only images with location_name or country
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, image_url, location_name, country, 
                   photographer_name, photographer_username, page_url
            FROM images
            WHERE location_name IS NOT NULL OR country IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 1
        """)
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return jsonify({'error': 'No images with location found'}), 404
        
        # Build caption
        if row['location_name'] and row['country']:
            caption = f"{row['location_name']}, {row['country']}"
        elif row['country']:
            caption = row['country']
        else:
            caption = row['location_name']
        
        return jsonify({
            'success': True,
            'data': {
                'id': row['id'],
                'imageUrl': row['image_url'],
                'caption': caption,
                'photographer': {
                    'name': row['photographer_name'],
                    'username': row['photographer_username'],
                    'profile': f"https://unsplash.com/@{row['photographer_username']}"
                },
                'unsplashLink': row['page_url']
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching image with location: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """
    Get database statistics.
    
    Returns:
        JSON with total images, location coverage, and top countries
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Total images
        cursor.execute("SELECT COUNT(*) FROM images")
        total = cursor.fetchone()[0]
        
        # With location
        cursor.execute("SELECT COUNT(*) FROM images WHERE location_name IS NOT NULL")
        with_location = cursor.fetchone()[0]
        
        # With country
        cursor.execute("SELECT COUNT(*) FROM images WHERE country IS NOT NULL")
        with_country = cursor.fetchone()[0]
        
        # Top countries
        cursor.execute("""
            SELECT country, COUNT(*) as count
            FROM images
            WHERE country IS NOT NULL
            GROUP BY country
            ORDER BY count DESC
            LIMIT 5
        """)
        top_countries = [{'country': row['country'], 'count': row['count']} 
                        for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'total': total,
                'withLocation': with_location,
                'withCountry': with_country,
                'locationCoverage': round((with_location / total * 100), 1) if total > 0 else 0,
                'topCountries': top_countries
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'service': 'landscape-api'})


if __name__ == '__main__':
    logger.info("Starting Landscape API server")
    logger.info(f"Database: {config.DATABASE_FILE}")
    app.run(debug=True, host='0.0.0.0', port=5001)
