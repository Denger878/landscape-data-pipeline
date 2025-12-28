import json
from pathlib import Path
from collections import Counter

# Paths
RAW_METADATA = Path('data/raw_metadata.json')
CLEANED_METADATA = Path('data/cleaned_metadata.json')
REPORT_FILE = Path('data/cleaning_report.txt')


def load_raw_data():
    """Load raw metadata from Day 1"""
    print("📂 Loading raw metadata...\n")
    
    with open(RAW_METADATA, 'r') as f:
        data = json.load(f)
    
    print(f"✓ Loaded {len(data)} records\n")
    return data


def analyze_data(data):
    """Analyze the raw data to understand what we're working with"""
    print("🔍 Analyzing raw data...\n")
    
    total = len(data)
    
    # Basic counts
    with_location = sum(1 for d in data if d.get('location_name'))
    with_country = sum(1 for d in data if d.get('country'))
    with_description = sum(1 for d in data if d.get('description'))
    downloaded = sum(1 for d in data if d.get('downloaded') == 1)
    
    # Dimension analysis
    widths = [d['width'] for d in data if 'width' in d]
    heights = [d['height'] for d in data if 'height' in d]
    aspect_ratios = [w/h for w, h in zip(widths, heights) if h > 0]
    
    landscape_count = sum(1 for w, h in zip(widths, heights) if w > h)
    portrait_count = sum(1 for w, h in zip(widths, heights) if w < h)
    square_count = sum(1 for w, h in zip(widths, heights) if w == h)
    
    # Resolution analysis
    resolutions = [w * h for w, h in zip(widths, heights)]
    avg_resolution = sum(resolutions) / len(resolutions) if resolutions else 0
    
    print("📊 Data Quality Report:")
    print(f"  • Total records: {total}")
    print(f"  • Successfully downloaded: {downloaded}")
    print(f"  • With location name: {with_location} ({with_location/total*100:.1f}%)")
    print(f"  • With country: {with_country} ({with_country/total*100:.1f}%)")
    print(f"  • With description: {with_description} ({with_description/total*100:.1f}%)")
    
    print(f"\n📐 Dimensions:")
    print(f"  • Landscape orientation (w>h): {landscape_count} ({landscape_count/total*100:.1f}%)")
    print(f"  • Portrait orientation (w<h): {portrait_count} ({portrait_count/total*100:.1f}%)")
    print(f"  • Square (w=h): {square_count}")
    
    print(f"\n🖼️  Resolution:")
    print(f"  • Average resolution: {avg_resolution/1_000_000:.1f} megapixels")
    print(f"  • Average width: {sum(widths)/len(widths):.0f}px")
    print(f"  • Average height: {sum(heights)/len(heights):.0f}px")
    
    # Find duplicates
    ids = [d['id'] for d in data]
    duplicate_ids = [id for id, count in Counter(ids).items() if count > 1]
    
    print(f"\n🔄 Duplicates:")
    print(f"  • Duplicate IDs found: {len(duplicate_ids)}")
    
    # Query distribution
    queries = [d['query'] for d in data if 'query' in d]
    query_counts = Counter(queries)
    
    print(f"\n🔎 Query Distribution:")
    print(f"  • Unique queries used: {len(query_counts)}")
    print(f"  • Top 5 queries:")
    for query, count in query_counts.most_common(5):
        print(f"     • '{query}': {count} images")
    
    # Location distribution
    countries = [d['country'] for d in data if d.get('country')]
    country_counts = Counter(countries)
    
    if country_counts:
        print(f"\n🌍 Top Countries:")
        for country, count in country_counts.most_common(5):
            print(f"     • {country}: {count} images")
    
    return {
        'total': total,
        'downloaded': downloaded,
        'duplicates': len(duplicate_ids),
        'duplicate_ids': duplicate_ids,
        'landscape_count': landscape_count,
        'portrait_count': portrait_count
    }


def remove_duplicates(data):
    """Remove duplicate images based on ID"""
    print("\n🔄 Removing duplicates...\n")
    
    seen_ids = set()
    unique_data = []
    duplicates_removed = 0
    
    for item in data:
        if item['id'] not in seen_ids:
            seen_ids.add(item['id'])
            unique_data.append(item)
        else:
            duplicates_removed += 1
    
    print(f"✓ Removed {duplicates_removed} duplicate(s)")
    print(f"✓ {len(unique_data)} unique records remaining\n")
    
    return unique_data


def validate_images(data):
    """
    Apply validation rules:
    1. Must have been downloaded successfully
    2. Must be landscape orientation (width > height)
    3. Must have minimum aspect ratio (not too square - min 1.3:1)
    4. Must have minimum resolution (width >= 1920)
    5. Must have required fields
    """
    print("✅ Validating images...\n")
    
    valid_data = []
    
    failed_download = 0
    failed_orientation = 0
    failed_aspect_ratio = 0
    failed_resolution = 0
    failed_missing_fields = 0
    
    for item in data:
        # Rule 1: Must be downloaded
        if item.get('downloaded') != 1:
            failed_download += 1
            continue
        
        # Rule 2: Must be landscape orientation
        width = item.get('width', 0)
        height = item.get('height', 0)
        
        if width <= height:
            failed_orientation += 1
            continue
        
        # Rule 3: Minimum aspect ratio (1.3:1 to avoid near-square images)
        # This filters out images like 2000x1800 (1.11:1) that look stretched
        aspect_ratio = width / height if height > 0 else 0
        if aspect_ratio < 1.3:
            failed_aspect_ratio += 1
            continue
        
        # Rule 4: Minimum resolution (1920px wide for quality)
        if width < 1920:
            failed_resolution += 1
            continue
        
        # Rule 5: Must have required fields
        required_fields = ['id', 'image_url', 'photographer_name', 'width', 'height']
        if not all(item.get(field) for field in required_fields):
            failed_missing_fields += 1
            continue
        
        # Passed all validations
        valid_data.append(item)
    
    print(f"  ✗ Failed download: {failed_download}")
    print(f"  ✗ Wrong orientation (portrait): {failed_orientation}")
    print(f"  ✗ Too close to square (ratio < 1.3): {failed_aspect_ratio}")
    print(f"  ✗ Low resolution (< 1920px): {failed_resolution}")
    print(f"  ✗ Missing required fields: {failed_missing_fields}")
    print(f"  ✓ Passed validation: {len(valid_data)}\n")
    
    return valid_data, {
        'failed_download': failed_download,
        'failed_orientation': failed_orientation,
        'failed_aspect_ratio': failed_aspect_ratio,
        'failed_resolution': failed_resolution,
        'failed_missing_fields': failed_missing_fields
    }


def enhance_metadata(data):
    """
    Add computed fields and clean up data
    """
    print("✨ Enhancing metadata...\n")
    
    for item in data:
        # Add aspect ratio
        if item.get('width') and item.get('height'):
            item['aspect_ratio'] = round(item['width'] / item['height'], 2)
        
        # Add megapixels
        if item.get('width') and item.get('height'):
            item['megapixels'] = round((item['width'] * item['height']) / 1_000_000, 1)
        
        # Clean up description (remove extra whitespace)
        if item.get('description'):
            item['description'] = ' '.join(item['description'].split())
    
    print(f"✓ Enhanced {len(data)} records\n")
    return data


def save_cleaned_data(data):
    """Save cleaned metadata to JSON"""
    print("💾 Saving cleaned data...\n")
    
    with open(CLEANED_METADATA, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"✓ Saved to {CLEANED_METADATA}\n")


def generate_report(stats, cleaned_count):
    """Generate a detailed cleaning report"""
    print("📄 Generating cleaning report...\n")
    
    report = f"""
DATA CLEANING REPORT
{'='*60}

INITIAL DATA
  • Total records: {stats['total']}
  • Successfully downloaded: {stats['downloaded']}

ISSUES FOUND
  • Duplicate images: {stats['duplicates']}
  • Portrait orientation: {stats['portrait_count']}
  • Low resolution images: (calculated during validation)

CLEANING ACTIONS
  • Duplicates removed: {stats['duplicates']}
  • Invalid images filtered: {stats['total'] - cleaned_count}

FINAL CLEAN DATASET
  • Valid images: {cleaned_count}
  • Data quality: {cleaned_count/stats['total']*100:.1f}%

{'='*60}
Generated on Day 2 - Data Cleaning
"""
    
    with open(REPORT_FILE, 'w') as f:
        f.write(report)
    
    print(f"✓ Report saved to {REPORT_FILE}\n")


def main():
    """Main cleaning pipeline"""
    print("🧹 DAY 2: Data Cleaning & Validation\n")
    print("="*60 + "\n")
    
    # Step 1: Load raw data
    raw_data = load_raw_data()
    
    # Step 2: Analyze raw data
    stats = analyze_data(raw_data)
    
    # Step 3: Remove duplicates
    unique_data = remove_duplicates(raw_data)
    
    # Step 4: Validate images
    valid_data, validation_stats = validate_images(unique_data)
    
    # Step 5: Enhance metadata
    cleaned_data = enhance_metadata(valid_data)
    
    # Step 6: Save cleaned data
    save_cleaned_data(cleaned_data)
    
    # Step 7: Generate report
    generate_report(stats, len(cleaned_data))
    
    # Final summary
    print("="*60)
    print("\n🎉 DAY 2 COMPLETE!\n")
    print(f"Summary:")
    print(f"  • Started with: {len(raw_data)} images")
    print(f"  • Removed duplicates: {stats['duplicates']}")
    print(f"  • Filtered invalid: {len(unique_data) - len(cleaned_data)}")
    print(f"  • Final dataset: {len(cleaned_data)} images")
    print(f"  • Data quality: {len(cleaned_data)/len(raw_data)*100:.1f}%")
    
    # Location coverage in final dataset
    with_location = sum(1 for d in cleaned_data if d.get('location_name') or d.get('country'))
    print(f"\n📍 Location Coverage (Clean Data):")
    print(f"  • Images with location data: {with_location} ({with_location/len(cleaned_data)*100:.1f}%)")
    
    print("\n✅ Next step: Day 3 - Load data into SQLite database")
    print("   Run: python load_sqlite.py\n")


if __name__ == '__main__':
    main()