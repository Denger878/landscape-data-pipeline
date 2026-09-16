"""REST API serving landscape images from the SQLite database."""
import os
import sqlite3
import logging
from contextlib import closing

from flask import Flask, jsonify
from flask_cors import CORS

import config

config.setup_logging()
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

IMAGE_COLUMNS = """
    id, image_url, location_name, country,
    photographer_name, photographer_username, page_url
"""


def get_db_connection():
    # Read-only: the database is static and serverless filesystems are read-only
    conn = sqlite3.connect(f"file:{config.DATABASE_FILE}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def query_one(sql):
    with closing(get_db_connection()) as conn:
        return conn.execute(sql).fetchone()


def query_all(sql):
    with closing(get_db_connection()) as conn:
        return conn.execute(sql).fetchall()


COUNTRY_ALIASES = {'United States': ('United States', 'USA')}


def build_caption(row):
    location, country = row['location_name'], row['country']
    if location and country:
        # Unsplash location names often already end with the country
        aliases = COUNTRY_ALIASES.get(country, (country,))
        if any(location.lower().endswith(alias.lower()) for alias in aliases):
            return location
        return f"{location}, {country}"
    return location or country


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


def random_image_response(where_clause, not_found_message):
    try:
        row = query_one(f"""
            SELECT {IMAGE_COLUMNS}
            FROM images
            {where_clause}
            ORDER BY RANDOM()
            LIMIT 1
        """)
    except sqlite3.Error as e:
        logger.error(f"Error fetching random image: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500

    if not row:
        return jsonify({'success': False, 'error': not_found_message}), 404

    return jsonify({'success': True, 'data': format_image_response(row)})


@app.route('/', methods=['GET'])
def index():
    return jsonify({
        'service': 'landscape-api',
        'endpoints': ['/api/random', '/api/random/location', '/api/stats', '/api/health']
    })


@app.route('/api/random', methods=['GET'])
def get_random_image():
    return random_image_response('', 'No images found')


@app.route('/api/random/location', methods=['GET'])
def get_random_with_location():
    return random_image_response(
        'WHERE location_name IS NOT NULL OR country IS NOT NULL',
        'No images with location found'
    )


@app.route('/api/stats', methods=['GET'])
def get_stats():
    try:
        totals = query_one("""
            SELECT
                COUNT(*) AS total,
                COUNT(location_name) AS with_location,
                COUNT(country) AS with_country,
                SUM(location_name IS NOT NULL OR country IS NOT NULL) AS with_any_location
            FROM images
        """)
        top_countries = query_all("""
            SELECT country, COUNT(*) AS count
            FROM images
            WHERE country IS NOT NULL
            GROUP BY country
            ORDER BY count DESC, country
            LIMIT 5
        """)
    except sqlite3.Error as e:
        logger.error(f"Error fetching stats: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500

    total = totals['total']
    return jsonify({
        'success': True,
        'data': {
            'total': total,
            'withLocation': totals['with_location'],
            'withCountry': totals['with_country'],
            'locationCoverage': round((totals['with_any_location'] or 0) / total * 100, 1) if total else 0,
            'topCountries': [{'country': r['country'], 'count': r['count']} for r in top_countries]
        }
    })


@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'landscape-api'})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    logger.info(f"Starting API server on port {port} | Database: {config.DATABASE_FILE}")
    app.run(debug=os.environ.get('FLASK_DEBUG') == '1', host='0.0.0.0', port=port)
