import os
import json
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
UNSPLASH_ACCESS_KEY = os.getenv('UNSPLASH_ACCESS_KEY')
BASE_URL = 'https://api.unsplash.com'
IMAGES_DIR = Path('data/images')
METADATA_FILE = Path('data/raw_metadata.json')

# Search queries for landscape images
SEARCH_QUERIES = [
    'landscape mountains',
    'landscape ocean',
    'landscape forest',
    'landscape desert',
    'landscape valley',
    'landscape sunset'
]

# API rate limit: 50 requests per hour
RATE_LIMIT_DELAY = 2  # seconds between requests


def setup_directories():
    """Create necessary directories if they don't exist"""
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    print(f"✓ Created directories")


def fetch_images(query, per_page=30, page=1):
    """
    Fetch images from Unsplash API
    
    Args:
        query: Search term
        per_page: Number of results (max 30)
        page: Page number
    
    Returns:
        List of image metadata dictionaries
    """
    url = f'{BASE_URL}/search/photos'
    params = {
        'query': query,
        'per_page': per_page,
        'page': page,
        'orientation': 'landscape',  # Only landscape orientation
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


def extract_location(image_data):
    """
    Extract location name and country from Unsplash metadata
    
    Note: Not all images have location data
    """
    location = image_data.get('location', {})
    
    location_name = None
    country = None
    
    # Try to get location name (city, landmark, etc.)
    if location.get('name'):
        location_name = location['name']
    elif location.get('city'):
        location_name = location['city']
    
    # Get country
    if location.get('country'):
        country = location['country']
    
    return location_name, country


def parse_image_metadata(image_data, query):
    """
    Extract relevant metadata from Unsplash API response
    
    Args:
        image_data: Raw JSON from Unsplash API
        query: Search query used
    
    Returns:
        Dictionary with cleaned metadata
    """
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


def download_image(image_url, image_id):
    """
    Download image to local storage
    
    Args:
        image_url: URL to download from
        image_id: Unique ID for filename
    
    Returns:
        True if successful, False otherwise
    """
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


def main():
    """Main ingestion pipeline"""
    
    # Check API key
    if not UNSPLASH_ACCESS_KEY:
        print("❌ Error: UNSPLASH_ACCESS_KEY not found in .env file")
        return
    
    print("🚀 Starting image ingestion pipeline...\n")
    
    # Setup
    setup_directories()
    
    all_metadata = []
    images_downloaded = 0
    target_images = 300
    
    # Fetch images for each query
    for query in SEARCH_QUERIES:
        print(f"\n📸 Searching: '{query}'")
        
        # Fetch 2 pages per query (60 images)
        for page in range(1, 3):
            
            # Respect rate limits
            time.sleep(RATE_LIMIT_DELAY)
            
            images = fetch_images(query, per_page=30, page=page)
            
            for img in images:
                # Parse metadata
                metadata = parse_image_metadata(img, query)
                
                # Download image
                success = download_image(metadata['download_url'], metadata['id'])
                
                if success:
                    metadata['downloaded'] = 1
                    images_downloaded += 1
                    print(f"    ✓ Downloaded {metadata['id']} ({images_downloaded}/{target_images})")
                
                all_metadata.append(metadata)
                
                # Stop if we hit target
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
    print(f"\n📍 Location data:")
    print(f"  • Images with location: {with_location}")
    print(f"  • Images without location: {len(all_metadata) - with_location}")
    
    print("\n✅ Day 1 complete!")


if __name__ == '__main__':
    main()