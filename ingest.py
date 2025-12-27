import os
import json
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
UNSPLASH_ACCESS_KEY = os.getenv('UNSPLASH_ACCESS_KEY')
BASE_URL = 'https://api.unsplash.com'
IMAGES_DIR = Path('data/images')
METADATA_FILE = Path('data/raw_metadata.json')

SEARCH_QUERIES = [
    'landscape mountains',
    'landscape ocean',
    'landscape forest',
    'landscape desert',
    'landscape valley',
    'landscape sunset'
]

# API rate limit: 50 requests per hour
RATE_LIMIT_DELAY = 2 


def setup_directories():
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    print(f"✓ Created directories")

# Fetch images from Unsplash API
def fetch_images(query, per_page=30, page=1):
    url = f'{BASE_URL}/search/photos'
    params = {
        'query': query,
        'per_page': per_page,
        'page': page,
        'orientation': 'landscape',
        'client_id': UNSPLASH_ACCESS_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        print(f"  ✓ Fetched {len(data['results'])} images for '{query}' (page {page})")
        return data['results']
    
    except requests.exceptions.RequestException as e:
        print(f"  ✗ Error fetching '{query}': {e}")
        return []

# Extract location and country
def extract_location(image_data):
    location = image_data.get('location', {})
    
    location_name = None
    country = None
    
    if location.get('name'):
        location_name = location['name']
    elif location.get('city'):
        location_name = location['city']
    if location.get('country'):
        country = location['country']
    
    return location_name, country

# Extract relevant metadat from API call
def parse_image_metadata(image_data, query):
    location_name, country = extract_location(image_data)
    
    metadata = {
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
    
    return metadata

# Download images
def download_image(image_url, image_id):
    try:
        response = requests.get(image_url, timeout=15)
        response.raise_for_status()
        
        # Save as JPG
        filepath = IMAGES_DIR / f"{image_id}.jpg"
        filepath.write_bytes(response.content)
        
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"    ✗ Failed to download {image_id}: {e}")
        return False

# Main ingestion pipeline
def main():
    if not UNSPLASH_ACCESS_KEY:
        print("❌ Error: UNSPLASH_ACCESS_KEY not found in .env file")
        return
    
    print("Starting image ingestion pipeline...\n")
    
    setup_directories()
    all_metadata = []
    images_downloaded = 0
    target_images = 300
    
    for query in SEARCH_QUERIES:
        print(f"\nSearching: '{query}'")
        
        # Fetch 2 pages per query (60 images)
        for page in range(1, 3):

            # Respect rate limits
            time.sleep(RATE_LIMIT_DELAY)
            images = fetch_images(query, per_page=30, page=page)
            
            # Parse metadata
            for img in images:
                metadata = parse_image_metadata(img, query)
                
                # Download image
                success = download_image(metadata['download_url'], metadata['id'])
                if success:
                    metadata['downloaded'] = 1
                    images_downloaded += 1
                    print(f"    ✓ Downloaded {metadata['id']} ({images_downloaded}/{target_images})")
                
                all_metadata.append(metadata)

                if images_downloaded >= target_images:
                    break
            
            if images_downloaded >= target_images:
                break
        
        if images_downloaded >= target_images:
            print(f"\n✅ Target reached: {images_downloaded} images")
            break
    
    # Save metadata to JSON
    with open(METADATA_FILE, 'w') as f:
        json.dump(all_metadata, f, indent=2)
    
    print(f"\n📊 Summary:")
    print(f"  • Total metadata records: {len(all_metadata)}")
    print(f"  • Images downloaded: {images_downloaded}")
    print(f"  • Metadata saved to: {METADATA_FILE}")
    print(f"  • Images saved to: {IMAGES_DIR}")
    
    # Quick stats
    with_location = sum(1 for m in all_metadata if m['location_name'])
    print(f"\nLocation data:")
    print(f"  • Images with location: {with_location}")
    print(f"  • Images without location: {len(all_metadata) - with_location}")

if __name__ == '__main__':
    main()