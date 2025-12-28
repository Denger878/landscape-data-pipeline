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

# Diverse, unique landscape descriptors
SEARCH_QUERIES = [
    # Water features
    'turquoise waterfall',
    'cascade waterfall',
    'natural hot springs',
    'geyser eruption',
    'thermal pool',
    'crystal clear lake',
    'glacier lake',
    'alpine lake reflection',
    
    # Mountains & peaks
    'jagged mountain peaks',
    'snow capped mountains',
    'volcanic crater',
    'volcanic landscape',
    'alpine meadow',
    'mountain summit',
    
    # Canyons & valleys
    'slot canyon',
    'red rock canyon',
    'canyon walls',
    'valley vista',
    'gorge landscape',
    
    # Caves & formations
    'sea cave',
    'limestone cave',
    'rock formations',
    'natural arch',
    'hoodoos rock',
    
    # Deserts
    'sand dunes sunset',
    'desert oasis',
    'salt flats',
    'badlands landscape',
    'sandstone formations',
    
    # Coastal
    'sea cliffs',
    'rocky coastline',
    'fjord landscape',
    'island aerial view',
    'lagoon tropical',
    
    # Ice & snow
    'glacier panorama',
    'ice cave blue',
    'frozen waterfall',
    'aurora landscape',
    'tundra landscape',
    
    # Unique features
    'terraced rice fields',
    'lava field',
    'karst mountains',
    'bioluminescent bay',
    'rainbow eucalyptus',
    'lavender fields',
    'tulip fields',
    'cherry blossom mountain'
]

# API rate limit: 50 requests per hour
RATE_LIMIT_DELAY = 2 


def setup_directories():
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    print(f"✓ Created directories")


def extract_location_from_text(text):
    """
    Parse description for location keywords.
    Looks for country names, famous landmarks, and geographic features.
    """
    if not text:
        return None, None
    
    text_lower = text.lower()
    
    # Famous landmarks (expanded list)
    landmarks = {
        # Iceland
        'jokulsarlon': 'Jökulsárlón Glacier Lagoon',
        'skogafoss': 'Skógafoss',
        'seljalandsfoss': 'Seljalandsfoss',
        'gulfoss': 'Gullfoss',
        'reynisfjara': 'Reynisfjara Black Beach',
        'kirkjufell': 'Kirkjufell',
        
        # USA - National Parks
        'yosemite': 'Yosemite Valley',
        'grand canyon': 'Grand Canyon',
        'yellowstone': 'Yellowstone',
        'zion': 'Zion National Park',
        'bryce canyon': 'Bryce Canyon',
        'arches': 'Arches National Park',
        'antelope canyon': 'Antelope Canyon',
        'crater lake': 'Crater Lake',
        'death valley': 'Death Valley',
        'monument valley': 'Monument Valley',
        'sedona': 'Sedona',
        'havasu falls': 'Havasu Falls',
        
        # Canada
        'banff': 'Banff National Park',
        'moraine lake': 'Moraine Lake',
        'lake louise': 'Lake Louise',
        'peyto lake': 'Peyto Lake',
        'jasper': 'Jasper National Park',
        
        # South America
        'patagonia': 'Patagonia',
        'torres del paine': 'Torres del Paine',
        'iguazu': 'Iguazu Falls',
        'salar de uyuni': 'Salar de Uyuni',
        'machu picchu': 'Machu Picchu',
        'atacama': 'Atacama Desert',
        'perito moreno': 'Perito Moreno Glacier',
        
        # Europe
        'dolomites': 'Dolomites',
        'matterhorn': 'Matterhorn',
        'lofoten': 'Lofoten Islands',
        'faroe': 'Faroe Islands',
        'plitvice': 'Plitvice Lakes',
        'lake bled': 'Lake Bled',
        'swiss alps': 'Swiss Alps',
        'scottish highlands': 'Scottish Highlands',
        'amalfi': 'Amalfi Coast',
        'cinque terre': 'Cinque Terre',
        'santorini': 'Santorini',
        'meteora': 'Meteora',
        'cappadocia': 'Cappadocia',
        'pamukkale': 'Pamukkale',
        
        # Asia
        'mount fuji': 'Mount Fuji',
        'zhangjiajie': 'Zhangjiajie',
        'guilin': 'Guilin',
        'halong bay': 'Halong Bay',
        'phi phi': 'Phi Phi Islands',
        'bali': 'Bali',
        'milford sound': 'Milford Sound',
        'mount cook': 'Mount Cook',
        'lake tekapo': 'Lake Tekapo',
        
        # Other
        'uluru': 'Uluru',
        'twelve apostles': 'Twelve Apostles',
        'milford sound': 'Milford Sound',
        'fiordland': 'Fiordland'
    }
    
    # Countries (for when landmark isn't found but country is mentioned)
    countries = {
        'iceland': 'Iceland',
        'norway': 'Norway',
        'switzerland': 'Switzerland',
        'italy': 'Italy',
        'canada': 'Canada',
        'new zealand': 'New Zealand',
        'patagonia': 'Argentina/Chile',
        'chile': 'Chile',
        'bolivia': 'Bolivia',
        'peru': 'Peru',
        'greece': 'Greece',
        'turkey': 'Turkey',
        'slovenia': 'Slovenia',
        'croatia': 'Croatia',
        'scotland': 'Scotland',
        'japan': 'Japan',
        'china': 'China',
        'vietnam': 'Vietnam',
        'thailand': 'Thailand',
        'indonesia': 'Indonesia',
        'australia': 'Australia',
        'usa': 'United States',
        'united states': 'United States',
        'california': 'United States',
        'arizona': 'United States',
        'utah': 'United States',
        'colorado': 'United States',
        'montana': 'United States',
        'oregon': 'United States',
        'washington': 'United States'
    }
    
    # Check for landmarks first (more specific)
    for keyword, location in landmarks.items():
        if keyword in text_lower:
            # Try to extract country too
            country = None
            for country_key, country_val in countries.items():
                if country_key in text_lower:
                    country = country_val
                    break
            return location, country
    
    # If no landmark, check for country
    for keyword, country in countries.items():
        if keyword in text_lower:
            return None, country
    
    return None, None


def extract_location(image_data):
    """
    Extract location using multiple methods:
    1. Official Unsplash location data
    2. Parse description for landmarks and countries
    """
    location = image_data.get('location', {})
    
    location_name = None
    country = None
    
    # Method 1: Official location fields
    if location.get('name'):
        location_name = location['name']
    elif location.get('city'):
        location_name = location['city']
    
    if location.get('country'):
        country = location['country']
    
    # Method 2: Parse description
    if not location_name or not country:
        description = image_data.get('description') or image_data.get('alt_description') or ''
        parsed_location, parsed_country = extract_location_from_text(description)
        
        if not location_name and parsed_location:
            location_name = parsed_location
        if not country and parsed_country:
            country = parsed_country
    
    return location_name, country


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


# Extract relevant metadata from API call
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
    
    print("🚀 Starting image ingestion pipeline...\n")
    print(f"🎯 Using {len(SEARCH_QUERIES)} unique landscape descriptors\n")
    
    setup_directories()
    all_metadata = []
    images_downloaded = 0
    target_images = 300
    
    for query in SEARCH_QUERIES:
        print(f"\n📸 Searching: '{query}'")
        
        # Fetch only 10-15 images per query for maximum variety
        time.sleep(RATE_LIMIT_DELAY)
        images = fetch_images(query, per_page=10, page=1)
        
        # Parse metadata
        for img in images:
            metadata = parse_image_metadata(img, query)
            
            # Download image
            success = download_image(metadata['download_url'], metadata['id'])
            if success:
                metadata['downloaded'] = 1
                images_downloaded += 1
                
                # Show location if found
                loc_info = ""
                if metadata['location_name']:
                    loc_info = f" - {metadata['location_name']}"
                    if metadata['country']:
                        loc_info += f", {metadata['country']}"
                elif metadata['country']:
                    loc_info = f" - {metadata['country']}"
                
                print(f"    ✓ Downloaded {metadata['id']} ({images_downloaded}/{target_images}){loc_info}")
            
            all_metadata.append(metadata)

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
    
    # Enhanced location stats
    with_location = sum(1 for m in all_metadata if m['location_name'])
    with_country = sum(1 for m in all_metadata if m['country'])
    with_either = sum(1 for m in all_metadata if m['location_name'] or m['country'])
    
    print(f"\n📍 Location data:")
    print(f"  • Images with location name: {with_location}")
    print(f"  • Images with country: {with_country}")
    print(f"  • Images with location OR country: {with_either}")
    print(f"  • Coverage: {(with_either/len(all_metadata)*100):.1f}%")
    
    # Show some example locations found
    unique_locations = [m['location_name'] for m in all_metadata if m['location_name']]
    if unique_locations:
        print(f"\n🌍 Sample locations found:")
        for loc in list(set(unique_locations))[:10]:
            print(f"     • {loc}")
    
    print("\n✅ Day 1 complete!")


if __name__ == '__main__':
    main()