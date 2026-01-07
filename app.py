"""REST API for landscape image data."""
import sqlite3
import logging
from flask import Flask, jsonify
from flask_cors import CORS

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)


def get_db_connection():
    conn = sqlite3.connect(config.DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def build_caption(row):
    if row['location_name'] and row['country']:
        return f"{row['location_name']}, {row['country']}"
    return row['country'] or row['location_name']


def format_image_response(row):
    return {
        'id': row['id'],
        'imageUrl': row['image_url'],
        'caption': build_caption(row),
        'photographer': {
            'name': row['photographer_name'],
            'username': row['photographer_username'],
            'profile': f"https://unsplash.com/@{row['photographer_username']}"
        },
        'unsplashLink': row['page_url']
    }


@app.route('/api/random', methods=['GET'])
def get_random_image():
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
        
        return jsonify({'success': True, 'data': format_image_response(row)})
        
    except Exception as e:
        logger.error(f"Error fetching random image: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/random/location', methods=['GET'])
def get_random_with_location():
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
        
        return jsonify({'success': True, 'data': format_image_response(row)})
        
    except Exception as e:
        logger.error(f"Error fetching image with location: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM images")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM images WHERE location_name IS NOT NULL")
        with_location = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM images WHERE country IS NOT NULL")
        with_country = cursor.fetchone()[0]
        
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
    return jsonify({'status': 'healthy', 'service': 'landscape-api'})


if __name__ == '__main__':
    logger.info(f"Starting API server | Database: {config.DATABASE_FILE}")
    app.run(debug=True, host='0.0.0.0', port=5001)
