"""Pipeline configuration."""
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'
IMAGES_DIR = DATA_DIR / 'images'
DB_DIR = PROJECT_ROOT / 'db'

RAW_METADATA_FILE = DATA_DIR / 'raw_metadata.json'
CLEANED_METADATA_FILE = DATA_DIR / 'cleaned_metadata.json'
CLEANING_REPORT_FILE = DATA_DIR / 'cleaning_report.txt'
DATABASE_FILE = DB_DIR / 'images.db'
SCHEMA_FILE = PROJECT_ROOT / 'schema.sql'

UNSPLASH_BASE_URL = 'https://api.unsplash.com'
RATE_LIMIT_DELAY = 2

TARGET_IMAGE_COUNT = 300
IMAGES_PER_QUERY = 10
IMAGE_ORIENTATION = 'landscape'

MIN_WIDTH = 1920
MIN_ASPECT_RATIO = 1.3

REQUIRED_FIELDS = ['id', 'image_url', 'photographer_name', 'width', 'height']

# Hand-picked Unsplash photo IDs added on top of the search results
MANUAL_PHOTO_IDS = [
    'wCmmOL6K_Qk', 'mwefJd0HdHk', '4uo97dAkX5Q', 'Mou_j-PpY2U',
    '_oWq2fKkT90', 'zaXHqMItpcc', 'rauWKt-AVSo', 'BcdrybyRkxc'
]
MANUAL_QUERY = 'manual_addition'

SEARCH_QUERIES = [
    'turquoise waterfall', 'cascade waterfall', 'natural hot springs',
    'geyser eruption', 'thermal pool', 'crystal clear lake',
    'glacier lake', 'alpine lake reflection', 'jagged mountain peaks',
    'snow capped mountains', 'volcanic crater', 'volcanic landscape',
    'alpine meadow', 'mountain summit', 'slot canyon', 'red rock canyon',
    'canyon walls', 'valley vista', 'gorge landscape', 'sea cave',
    'limestone cave', 'rock formations', 'natural arch', 'hoodoos rock',
    'sand dunes sunset', 'desert oasis', 'salt flats', 'badlands landscape',
    'sandstone formations', 'sea cliffs', 'rocky coastline', 'fjord landscape',
    'island aerial view', 'lagoon tropical', 'glacier panorama', 'ice cave blue',
    'frozen waterfall', 'aurora landscape', 'tundra landscape',
    'terraced rice fields', 'lava field', 'karst mountains',
    'bioluminescent bay', 'rainbow eucalyptus', 'lavender fields',
    'tulip fields', 'cherry blossom mountain'
]

# keyword -> (landmark name, country)
LANDMARK_KEYWORDS = {
    'jokulsarlon': ('Jökulsárlón Glacier Lagoon', 'Iceland'),
    'skogafoss': ('Skógafoss', 'Iceland'),
    'seljalandsfoss': ('Seljalandsfoss', 'Iceland'),
    'gullfoss': ('Gullfoss', 'Iceland'),
    'reynisfjara': ('Reynisfjara Black Beach', 'Iceland'),
    'kirkjufell': ('Kirkjufell', 'Iceland'),
    'yosemite': ('Yosemite Valley', 'United States'),
    'grand canyon': ('Grand Canyon', 'United States'),
    'yellowstone': ('Yellowstone', 'United States'),
    'zion': ('Zion National Park', 'United States'),
    'bryce canyon': ('Bryce Canyon', 'United States'),
    'arches national park': ('Arches National Park', 'United States'),
    'antelope canyon': ('Antelope Canyon', 'United States'),
    'crater lake': ('Crater Lake', 'United States'),
    'death valley': ('Death Valley', 'United States'),
    'monument valley': ('Monument Valley', 'United States'),
    'sedona': ('Sedona', 'United States'),
    'havasu falls': ('Havasu Falls', 'United States'),
    'banff': ('Banff National Park', 'Canada'),
    'moraine lake': ('Moraine Lake', 'Canada'),
    'lake louise': ('Lake Louise', 'Canada'),
    'peyto lake': ('Peyto Lake', 'Canada'),
    'jasper national park': ('Jasper National Park', 'Canada'),
    'torres del paine': ('Torres del Paine', 'Chile'),
    'iguazu': ('Iguazu Falls', 'Argentina'),
    'salar de uyuni': ('Salar de Uyuni', 'Bolivia'),
    'machu picchu': ('Machu Picchu', 'Peru'),
    'atacama': ('Atacama Desert', 'Chile'),
    'perito moreno': ('Perito Moreno Glacier', 'Argentina'),
    'dolomites': ('Dolomites', 'Italy'),
    'matterhorn': ('Matterhorn', 'Switzerland'),
    'lofoten': ('Lofoten Islands', 'Norway'),
    'faroe': ('Faroe Islands', 'Faroe Islands'),
    'plitvice': ('Plitvice Lakes', 'Croatia'),
    'lake bled': ('Lake Bled', 'Slovenia'),
    'swiss alps': ('Swiss Alps', 'Switzerland'),
    'scottish highlands': ('Scottish Highlands', 'Scotland'),
    'amalfi': ('Amalfi Coast', 'Italy'),
    'cinque terre': ('Cinque Terre', 'Italy'),
    'santorini': ('Santorini', 'Greece'),
    'meteora': ('Meteora', 'Greece'),
    'cappadocia': ('Cappadocia', 'Turkey'),
    'pamukkale': ('Pamukkale', 'Turkey'),
    'mount fuji': ('Mount Fuji', 'Japan'),
    'zhangjiajie': ('Zhangjiajie', 'China'),
    'guilin': ('Guilin', 'China'),
    'halong bay': ('Halong Bay', 'Vietnam'),
    'phi phi': ('Phi Phi Islands', 'Thailand'),
    'bali': ('Bali', 'Indonesia'),
    'milford sound': ('Milford Sound', 'New Zealand'),
    'mount cook': ('Mount Cook', 'New Zealand'),
    'lake tekapo': ('Lake Tekapo', 'New Zealand'),
    'uluru': ('Uluru', 'Australia'),
    'twelve apostles': ('Twelve Apostles', 'Australia'),
    'fiordland': ('Fiordland', 'New Zealand'),
}

COUNTRY_KEYWORDS = {
    'iceland': 'Iceland', 'norway': 'Norway', 'switzerland': 'Switzerland',
    'italy': 'Italy', 'canada': 'Canada', 'new zealand': 'New Zealand',
    'chile': 'Chile', 'bolivia': 'Bolivia', 'peru': 'Peru',
    'greece': 'Greece', 'turkey': 'Turkey', 'slovenia': 'Slovenia',
    'croatia': 'Croatia', 'scotland': 'Scotland', 'japan': 'Japan',
    'china': 'China', 'vietnam': 'Vietnam', 'thailand': 'Thailand',
    'indonesia': 'Indonesia', 'australia': 'Australia',
    'usa': 'United States', 'united states': 'United States',
    'california': 'United States', 'arizona': 'United States',
    'utah': 'United States', 'colorado': 'United States',
    'montana': 'United States', 'oregon': 'United States',
    'washington': 'United States', 'hawaii': 'United States',
    'wyoming': 'United States', 'nevada': 'United States',
    'alaska': 'United States', 'argentina': 'Argentina'
}


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
