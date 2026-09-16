"""Image ingestion from Unsplash API."""
import os
import sys
import json
import time
import logging
import requests
from dotenv import load_dotenv

import config
from location import extract_location

config.setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()
UNSPLASH_ACCESS_KEY = os.getenv('UNSPLASH_ACCESS_KEY')


def setup_directories():
    config.IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)


def fetch_images(query, per_page=30, page=1):
    url = f'{config.UNSPLASH_BASE_URL}/search/photos'
    params = {
        'query': query,
        'per_page': per_page,
        'page': page,
        'orientation': config.IMAGE_ORIENTATION,
        'client_id': UNSPLASH_ACCESS_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()['results']
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch '{query}': {e}")
        return []


def fetch_photo(photo_id):
    url = f'{config.UNSPLASH_BASE_URL}/photos/{photo_id}'
    try:
        response = requests.get(url, params={'client_id': UNSPLASH_ACCESS_KEY}, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch photo {photo_id}: {e}")
        return None


def parse_image_metadata(image_data, query):
    location_name, country = extract_location(image_data)
    
    return {
        'id': image_data['id'],
        'image_url': image_data['urls']['regular'],
        'download_url': image_data['urls']['full'],
        'page_url': image_data['links']['html'],
        'location_name': location_name,
        'country': country,
        'description': image_data.get('description') or image_data.get('alt_description'),
        'photographer_name': image_data['user']['name'],
        'photographer_username': image_data['user']['username'],
        'width': image_data['width'],
        'height': image_data['height'],
        'color': image_data.get('color'),
        'source': 'unsplash',
        'query': query,
        'downloaded': 0
    }


def download_image(image_url, image_id):
    try:
        response = requests.get(image_url, timeout=15)
        response.raise_for_status()
        filepath = config.IMAGES_DIR / f"{image_id}.jpg"
        filepath.write_bytes(response.content)
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {image_id}: {e}")
        return False


def ingest_manual_photos(seen_ids):
    """Fetch the hand-picked photos in config.MANUAL_PHOTO_IDS."""
    metadata_list = []
    for photo_id in config.MANUAL_PHOTO_IDS:
        if photo_id in seen_ids:
            continue
        time.sleep(config.RATE_LIMIT_DELAY)
        img = fetch_photo(photo_id)
        if not img:
            continue
        seen_ids.add(photo_id)

        metadata = parse_image_metadata(img, config.MANUAL_QUERY)
        already_on_disk = (config.IMAGES_DIR / f"{photo_id}.jpg").exists()
        if already_on_disk or download_image(metadata['download_url'], photo_id):
            metadata['downloaded'] = 1
        metadata_list.append(metadata)

    logger.info(f"Manual photos: {len(metadata_list)}/{len(config.MANUAL_PHOTO_IDS)} fetched")
    return metadata_list


def refresh_manual_photos():
    """Re-fetch manual photos into the existing raw metadata without re-running searches."""
    with open(config.RAW_METADATA_FILE, 'r') as f:
        all_metadata = json.load(f)

    manual_ids = set(config.MANUAL_PHOTO_IDS)
    all_metadata = [
        m for m in all_metadata
        if m['id'] not in manual_ids and m.get('query') != config.MANUAL_QUERY
    ]
    all_metadata.extend(ingest_manual_photos({m['id'] for m in all_metadata}))

    with open(config.RAW_METADATA_FILE, 'w') as f:
        json.dump(all_metadata, f, indent=2)
    logger.info(f"Saved {len(all_metadata)} raw records")


def main():
    if not UNSPLASH_ACCESS_KEY:
        logger.error("UNSPLASH_ACCESS_KEY not found")
        return

    setup_directories()
    if '--manual-only' in sys.argv:
        refresh_manual_photos()
        return
    
    logger.info(f"Starting ingestion | Target: {config.TARGET_IMAGE_COUNT} images")
    
    all_metadata = []
    seen_ids = set()
    images_downloaded = 0
    
    for query in config.SEARCH_QUERIES:
        if images_downloaded >= config.TARGET_IMAGE_COUNT:
            break
        
        logger.info(f"Searching: '{query}'")
        time.sleep(config.RATE_LIMIT_DELAY)
        
        images = fetch_images(query, per_page=config.IMAGES_PER_QUERY, page=1)
        
        for img in images:
            # The same photo can match several queries; only download it once
            if img['id'] in seen_ids:
                continue
            seen_ids.add(img['id'])

            metadata = parse_image_metadata(img, query)
            
            if download_image(metadata['download_url'], metadata['id']):
                metadata['downloaded'] = 1
                images_downloaded += 1
                
                if images_downloaded % 20 == 0:
                    logger.info(f"Progress: {images_downloaded}/{config.TARGET_IMAGE_COUNT}")
            
            all_metadata.append(metadata)
            
            if images_downloaded >= config.TARGET_IMAGE_COUNT:
                break
    
    all_metadata.extend(ingest_manual_photos(seen_ids))

    with open(config.RAW_METADATA_FILE, 'w') as f:
        json.dump(all_metadata, f, indent=2)
    
    with_location = sum(1 for m in all_metadata if m['location_name'] or m['country'])
    logger.info(f"Complete: {images_downloaded} images | Location coverage: {with_location}/{len(all_metadata)}")
    
    return all_metadata


if __name__ == '__main__':
    main()
