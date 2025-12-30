"""
Configuration settings for landscape image data pipeline
"""
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'
IMAGES_DIR = DATA_DIR / 'images'
DB_DIR = PROJECT_ROOT / 'db'

# File paths
RAW_METADATA_FILE = DATA_DIR / 'raw_metadata.json'
CLEANED_METADATA_FILE = DATA_DIR / 'cleaned_metadata.json'
CLEANING_REPORT_FILE = DATA_DIR / 'cleaning_report.txt'
DATABASE_FILE = DB_DIR / 'images.db'
SCHEMA_FILE = PROJECT_ROOT / 'schema.sql'

# API settings
UNSPLASH_BASE_URL = 'https://api.unsplash.com'
RATE_LIMIT_DELAY = 2  # seconds between requests

# Image collection settings
TARGET_IMAGE_COUNT = 300
IMAGES_PER_QUERY = 10
IMAGE_ORIENTATION = 'landscape'

# Validation rules
MIN_WIDTH = 1920  # pixels
MIN_ASPECT_RATIO = 1.3  # width/height ratio (prevents near-square images)

# Required fields for valid images
REQUIRED_FIELDS = ['id', 'image_url', 'photographer_name', 'width', 'height']

# Search queries for diverse landscape types
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

# Location extraction keywords
LANDMARK_KEYWORDS = {
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
    'fiordland': 'Fiordland'
}

COUNTRY_KEYWORDS = {
    'iceland': 'Iceland',
    'norway': 'Norway',
    'switzerland': 'Switzerland',
    'italy': 'Italy',
    'canada': 'Canada',
    'new zealand': 'New Zealand',
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
