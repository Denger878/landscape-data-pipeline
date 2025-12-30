import json
from pathlib import Path

# IDs to remove (embedded in script)
IDS_TO_REMOVE = {
    '00cUiHcmGG4', '4_45OOu0F5s', '5fJPWeGPR7o', '5RZLouoQRgc', '6SHaj7PqETA',
    '7dOVO_ZLy7E', '8poggJLLYYA', '8sD_hPVSmg8', '9AMya5zS5Ac', '73p44Sd5MvY',
    'a0dSmST0veE', 'AdNNOv_leW4', 'ag6XjPWgpPY', 'AR0O6G6vfr0', 'As-cWXbIrMs',
    'BLIlHJRH3ws', 'bpSWz6ikP3I', 'bvTcBb3Gy6Q', 'CyQY_g0aKEU', 'd06yNEOkNaQ',
    'dM3hYpeUmBc', 'e1dQF4O86xw', 'Egqtj1bp5oU', 'EHpcSQiZ6qE', 'eulDlmyOF4c',
    'ExR4JIbpo2U', 'EZdimiAoWDs', 'fF6pX9zxj2I', 'Fv9EMIzhmUw', 'g7OJqiDeN7Y',
    'go1kCa8tgoE', 'gT7yZXCTZI8', 'Gxtc-Zkyuww', 'gZc-w1ljxYY', 'gZxGRJfq1Ko',
    'hjDRTEeryNY', 'HkoTlXpQ8bI', 'HVWvzE4RC90', 'Hy08CLM6ya0', 'I3O4Fo52-J8',
    'Igg3M1BxFss', 'ihJDv9Ogxh4', 'im_sKc-k2Ok', 'IrXlYn56lGg', 'jctQLwTWHUY',
    'JDs91vNrSeY', 'jIeN3aYwzLQ', 'JmpK6HCbyNU', 'kjg5sEuovHM', 'Kjz2bz9AWsk',
    'kuagiGRix3w', 'L5Of5S5yP2Q', 'L8atGRUUlt4', 'lA_hJpVt75Q', 'LBMvueaBKT8',
    'LbnA9a7OGOY', 'lImNhE_qgU4', 'LPXPPuFVSGo', 'lZ-mC4tFxT8', 'M1PSS2R20hc',
    'MeMbExzoyF8', 'mLg0SFQQ9mw', 'mO6ssD5-KLQ', 'MOggaXKjWao', 'MTCy6Z6uaLM',
    'mxLlkPZaSQY', 'MzeSloMrFTY', 'n_8p6Bt0CTE', 'naOAg4lOo04', 'niuLjIe7678',
    'nvKdCEew3r8', 'OeZyMS5byLA', 'OiRTwOIDRek', 'OmFuGKuXik8', 'oUGDsqTc-fE',
    'PeWJ9Wc9JeA', 'pLso-JIwGjw', 'pMwTiDYTpWE', 'q4NEv-GhYjY', 'qBEUMUWhZcQ',
    'QKmbBAcQulw', 'QNPw2OaBL9c', 'qprFpgPuL7k', 'qtbjq9xqCr8', 'qu_YD-alREs',
    'rjMi3M_-NXE', 'RKishWnWiT8', 'rOdr0Mu3hUs', 'S0uWDf2qATo', 's8tJheipWQQ',
    'sGYdFzbQkDw', 'skOO2HrZ-QU', 'SOOZlZ5fnnY', 'teKg8KhetAw', 'tI7y2FvqMeg',
    'tV_8XZwPz98', 'TWl8LjMt19Y', 'TxY-7FBUGzY', 'uuA-9Ui63-w', 'UHcwyq05_Gk',
    'veWIhKTIwX0', 'Vyz8sxWO_As', 'W_ieciuHrxQ', 'w5GhMvdnRtg', 'wfa3pUYdHww',
    'wYvCM5ghxfM', 'XQi5lyIC6wI', 'XS0LsQAcLg0', 'xZ07O0-yG1g', 'YaZmCKWQcnI',
    'yBQuVoL1BVQ', 'YeE-dWk0MXs', 'Z1aTc4CHBIM', 'zDyg7wfqjpE', 'ZPSpTnHWnJA',
    'ZtRCBsF2Ick', 'ZTwAfMJOf6M', '_7vkHga1HRc', '_w395pqB7PY', '-v7DCnh-XQo',
    '0DReGVDnkOI', '0JFL9__xLqs', '0JY3gGiP4Nc', '1SVw6E6MsFw', '2ko_RKjmQ2s',
    '2zm00PNYg0M', '3oi8O5otnyQ', '3M5YA8ZUhPA', '5SJx0vjfhpA', '6LGsSlQcbVA',
    '7j3Eb8GbSlg', '8oYPewvmhnY', '8qpxkZ8epIA', 'bHWa-OBGKWg', 'bUweOlXUso4',
    '8TPOh8NoYok', '9PBKILaBgCM', '9wDesv9hKTA', '88dHrVRUIuQ', '90WWdxLbiME',
    '97JW321dFGE', '327wv8qPC6I', '795Xt_8UGds', 'ACOUzUNOzRc', 'AdSoy5Mke3E',
    'AkA1L9sKg8g', 'aLbEe5uMSb4', 'AM53LSIBnRo', 'B9DzB2OC8BI', 'cMKA66OtK9U',
    'CV7KPRM6fHc', 'DfW3HibwnHs', 'ELmcNBsy3VI', 'ELz0kdh3xYs', 'Fq2ByVF3yI0',
    'Fu6ykBPVhOc', 'gzo3y_1RewI', 'hMC9l2Ks4sg', 'hyMS36l2trE',
    'InUfN98On50', 'IZnso9zXrP8', 'j3zNzWQH5Gg', 'J7zNuZkHXKE', 'jURd5THOjTA',
    'k153fE-uWkA', 'KH7EOKNM-MY', 'Kwx3Hj81ZtA', 'LO4v5E04Otk', 'LRhn96xjg5U',
    'mWKDKQ6Wmr4', 'NvhxKCtfnHA', 'Q5EaIN0OT00', 'qsfBFeSCjvQ', 'rQ1O8t9HWpQ',
    'SFdYZPe0QYc', 'T1CLUM6TIWw', 'tlVbaOnPC0c', 'uWcBtIws3eM', 'XXLB3jZ3CWQ',
    'y1PPumgtxhs', 'yI49CSC97EI', 'zTLTJxYS_0g', 'XoUzCBxuLyo', 'XjR7NFj0QnM',
    'XF8xNibEr84', '-TJGAtPA7GQ', '4KCVIRGfq24', '7L9QtwXOhQU', '8jwjnBmD4Y4',
    'bAB0Qptcz34', 'ehGkWN0z3Gs', 'JaRqC7IXxVI', 'nDi1FnvsiRk', 'TWFBILyxaQA',
    'uxOzaTN3SYM', 'V0ktfkerWmg', 'vC1NHxycp2I'
}

# File paths
CLEANED_METADATA = Path('data/cleaned_metadata.json')
BACKUP_FILE = Path('data/cleaned_metadata_backup.json')

def main():
    print("\nImage Cleanup Script")
    print("=" * 50)
    print(f"\nIDs to remove: {len(IDS_TO_REMOVE)}\n")
    
    # Load cleaned metadata
    if not CLEANED_METADATA.exists():
        print("ERROR: cleaned_metadata.json not found!")
        print("Make sure you're in the landscape-data-pipeline directory")
        return
    
    with open(CLEANED_METADATA, 'r') as f:
        data = json.load(f)
    
    print(f"Loaded {len(data)} images from metadata\n")
    
    # Create backup
    with open(BACKUP_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Backup created: {BACKUP_FILE}\n")
    
    # Filter out unwanted images
    original_count = len(data)
    filtered_data = [img for img in data if img['id'] not in IDS_TO_REMOVE]
    removed_count = original_count - len(filtered_data)
    
    print("Results:")
    print(f"  Original images: {original_count}")
    print(f"  Removed: {removed_count}")
    print(f"  Remaining: {len(filtered_data)}")
    
    # Check if any IDs weren't found
    found_ids = {img['id'] for img in data}
    not_found = IDS_TO_REMOVE - found_ids
    if not_found:
        print(f"\nWarning: {len(not_found)} IDs not found in metadata")
        print("(These images may have already been removed)")
    
    # Save cleaned data
    with open(CLEANED_METADATA, 'w') as f:
        json.dump(filtered_data, f, indent=2)
    
    print(f"\nCleaned metadata saved: {CLEANED_METADATA}")
    print(f"Final count: {len(filtered_data)} images\n")
    
    print("Next steps:")
    print("  1. rm db/images.db")
    print("  2. python3 load_sqlite.py")
    print()

if __name__ == '__main__':
    main()
