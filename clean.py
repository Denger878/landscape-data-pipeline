"""Data cleaning and validation."""
import json
import logging
from collections import Counter

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_raw_data():
    with open(config.RAW_METADATA_FILE, 'r') as f:
        data = json.load(f)
    logger.info(f"Loaded {len(data)} raw records")
    return data


def analyze_data(data):
    ids = [d['id'] for d in data]
    duplicate_ids = [id for id, count in Counter(ids).items() if count > 1]
    
    widths = [d['width'] for d in data if 'width' in d]
    heights = [d['height'] for d in data if 'height' in d]
    
    return {
        'total': len(data),
        'downloaded': sum(1 for d in data if d.get('downloaded') == 1),
        'with_location': sum(1 for d in data if d.get('location_name')),
        'with_country': sum(1 for d in data if d.get('country')),
        'landscape_count': sum(1 for w, h in zip(widths, heights) if w > h),
        'portrait_count': sum(1 for w, h in zip(widths, heights) if w < h),
        'duplicates': len(duplicate_ids)
    }


def remove_duplicates(data):
    seen_ids = set()
    unique_data = []
    
    for item in data:
        if item['id'] not in seen_ids:
            seen_ids.add(item['id'])
            unique_data.append(item)
    
    removed = len(data) - len(unique_data)
    if removed > 0:
        logger.info(f"Removed {removed} duplicates")
    
    return unique_data


def validate_images(data):
    valid_data = []
    failed_counts = {
        'download': 0, 'orientation': 0, 'aspect_ratio': 0,
        'resolution': 0, 'missing_fields': 0
    }
    
    for item in data:
        if item.get('downloaded') != 1:
            failed_counts['download'] += 1
            continue
        
        width, height = item.get('width', 0), item.get('height', 0)
        
        if width <= height:
            failed_counts['orientation'] += 1
            continue
        
        if height > 0 and (width / height) < config.MIN_ASPECT_RATIO:
            failed_counts['aspect_ratio'] += 1
            continue
        
        if width < config.MIN_WIDTH:
            failed_counts['resolution'] += 1
            continue
        
        if not all(item.get(field) for field in config.REQUIRED_FIELDS):
            failed_counts['missing_fields'] += 1
            continue
        
        valid_data.append(item)
    
    logger.info(f"Validation: {len(valid_data)}/{len(data)} passed")
    return valid_data, failed_counts


def enhance_metadata(data):
    for item in data:
        if item.get('width') and item.get('height'):
            item['aspect_ratio'] = round(item['width'] / item['height'], 2)
            item['megapixels'] = round((item['width'] * item['height']) / 1_000_000, 1)
        
        if item.get('description'):
            item['description'] = ' '.join(item['description'].split())
    
    return data


def save_cleaned_data(data):
    with open(config.CLEANED_METADATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved {len(data)} cleaned records")


def generate_report(stats, cleaned_count, failed_counts):
    report = f"""DATA CLEANING REPORT
{'='*60}

INITIAL DATA
  Total records: {stats['total']}
  Successfully downloaded: {stats['downloaded']}
  With location: {stats['with_location']}
  With country: {stats['with_country']}

ISSUES FOUND
  Duplicates: {stats['duplicates']}
  Portrait orientation: {stats['portrait_count']}

VALIDATION FAILURES
  Failed download: {failed_counts['download']}
  Wrong orientation: {failed_counts['orientation']}
  Aspect ratio < {config.MIN_ASPECT_RATIO}: {failed_counts['aspect_ratio']}
  Resolution < {config.MIN_WIDTH}px: {failed_counts['resolution']}
  Missing fields: {failed_counts['missing_fields']}

FINAL CLEAN DATASET
  Valid images: {cleaned_count}
  Data quality: {cleaned_count/stats['total']*100:.1f}%

{'='*60}
"""
    with open(config.CLEANING_REPORT_FILE, 'w') as f:
        f.write(report)


def main():
    logger.info("Starting data cleaning")
    
    raw_data = load_raw_data()
    stats = analyze_data(raw_data)
    unique_data = remove_duplicates(raw_data)
    valid_data, failed_counts = validate_images(unique_data)
    cleaned_data = enhance_metadata(valid_data)
    save_cleaned_data(cleaned_data)
    generate_report(stats, len(cleaned_data), failed_counts)
    
    logger.info(f"Complete: {len(cleaned_data)}/{len(raw_data)} images ({len(cleaned_data)/len(raw_data)*100:.1f}% quality)")
    
    return cleaned_data


if __name__ == '__main__':
    main()
