"""
Data cleaning and validation module for landscape image pipeline.
Removes duplicates, validates image quality, and enhances metadata.
"""
import json
import logging
from pathlib import Path
from collections import Counter

import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_raw_data():
    """Load raw metadata from ingestion phase."""
    with open(config.RAW_METADATA_FILE, 'r') as f:
        data = json.load(f)
    logger.info(f"Loaded {len(data)} raw records")
    return data


def analyze_data(data):
    """Analyze raw data and return statistics."""
    total = len(data)
    
    with_location = sum(1 for d in data if d.get('location_name'))
    with_country = sum(1 for d in data if d.get('country'))
    downloaded = sum(1 for d in data if d.get('downloaded') == 1)
    
    widths = [d['width'] for d in data if 'width' in d]
    heights = [d['height'] for d in data if 'height' in d]
    
    landscape_count = sum(1 for w, h in zip(widths, heights) if w > h)
    portrait_count = sum(1 for w, h in zip(widths, heights) if w < h)
    
    ids = [d['id'] for d in data]
    duplicate_ids = [id for id, count in Counter(ids).items() if count > 1]
    
    stats = {
        'total': total,
        'downloaded': downloaded,
        'with_location': with_location,
        'with_country': with_country,
        'landscape_count': landscape_count,
        'portrait_count': portrait_count,
        'duplicates': len(duplicate_ids)
    }
    
    logger.info(f"Analysis: {total} total, {downloaded} downloaded, "
                f"{with_location} with location, {len(duplicate_ids)} duplicates")
    
    return stats


def remove_duplicates(data):
    """Remove duplicate images based on ID."""
    seen_ids = set()
    unique_data = []
    duplicates_removed = 0
    
    for item in data:
        if item['id'] not in seen_ids:
            seen_ids.add(item['id'])
            unique_data.append(item)
        else:
            duplicates_removed += 1
    
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicates")
    
    return unique_data


def validate_images(data):
    """
    Apply validation rules to filter images.
    
    Rules:
    1. Must be downloaded successfully
    2. Must be landscape orientation (width > height)
    3. Minimum aspect ratio (prevents near-square images)
    4. Minimum resolution (width >= MIN_WIDTH)
    5. Must have required fields
    """
    valid_data = []
    
    failed_counts = {
        'download': 0,
        'orientation': 0,
        'aspect_ratio': 0,
        'resolution': 0,
        'missing_fields': 0
    }
    
    for item in data:
        # Rule 1: Downloaded
        if item.get('downloaded') != 1:
            failed_counts['download'] += 1
            continue
        
        # Rule 2: Landscape orientation
        width = item.get('width', 0)
        height = item.get('height', 0)
        
        if width <= height:
            failed_counts['orientation'] += 1
            continue
        
        # Rule 3: Aspect ratio
        aspect_ratio = width / height if height > 0 else 0
        if aspect_ratio < config.MIN_ASPECT_RATIO:
            failed_counts['aspect_ratio'] += 1
            continue
        
        # Rule 4: Resolution
        if width < config.MIN_WIDTH:
            failed_counts['resolution'] += 1
            continue
        
        # Rule 5: Required fields
        if not all(item.get(field) for field in config.REQUIRED_FIELDS):
            failed_counts['missing_fields'] += 1
            continue
        
        valid_data.append(item)
    
    logger.info(f"Validation: {len(valid_data)}/{len(data)} passed "
                f"(failed: {failed_counts['orientation']} orientation, "
                f"{failed_counts['aspect_ratio']} aspect ratio, "
                f"{failed_counts['resolution']} resolution)")
    
    return valid_data, failed_counts


def enhance_metadata(data):
    """Add computed fields to metadata."""
    for item in data:
        if item.get('width') and item.get('height'):
            item['aspect_ratio'] = round(item['width'] / item['height'], 2)
            item['megapixels'] = round((item['width'] * item['height']) / 1_000_000, 1)
        
        if item.get('description'):
            item['description'] = ' '.join(item['description'].split())
    
    return data


def save_cleaned_data(data):
    """Save cleaned metadata to JSON."""
    with open(config.CLEANED_METADATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved {len(data)} cleaned records to {config.CLEANED_METADATA_FILE}")


def generate_report(stats, cleaned_count, failed_counts):
    """Generate cleaning report."""
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
    
    logger.info(f"Report saved to {config.CLEANING_REPORT_FILE}")


def main():
    """Main cleaning pipeline."""
    logger.info("Starting data cleaning pipeline")
    
    raw_data = load_raw_data()
    stats = analyze_data(raw_data)
    unique_data = remove_duplicates(raw_data)
    valid_data, failed_counts = validate_images(unique_data)
    cleaned_data = enhance_metadata(valid_data)
    save_cleaned_data(cleaned_data)
    generate_report(stats, len(cleaned_data), failed_counts)
    
    quality_pct = len(cleaned_data) / len(raw_data) * 100
    logger.info(f"Cleaning complete: {len(cleaned_data)}/{len(raw_data)} images ({quality_pct:.1f}% quality)")
    
    with_location = sum(1 for d in cleaned_data if d.get('location_name') or d.get('country'))
    logger.info(f"Location coverage: {with_location}/{len(cleaned_data)} ({with_location/len(cleaned_data)*100:.1f}%)")
    
    return cleaned_data


if __name__ == '__main__':
    main()