"""Tests for the cleaning, location extraction and loading steps."""
import json
import sqlite3

import pytest

import clean
import config
import load_sqlite
import location


def make_record(image_id, width=4000, height=2500, **overrides):
    record = {
        'id': image_id,
        'image_url': f'https://images.unsplash.com/photo-{image_id}',
        'download_url': f'https://images.unsplash.com/photo-{image_id}?full',
        'page_url': f'https://unsplash.com/photos/{image_id}',
        'location_name': None,
        'country': None,
        'description': 'A  lake\nat dawn',
        'photographer_name': 'Jane Doe',
        'photographer_username': 'janedoe',
        'width': width,
        'height': height,
        'color': '#ffffff',
        'source': 'unsplash',
        'query': 'glacier lake',
        'downloaded': 1,
    }
    record.update(overrides)
    return record


def test_remove_duplicates_keeps_first():
    data = [make_record('a'), make_record('a', query='other'), make_record('b')]
    result = clean.remove_duplicates(data)
    assert [r['id'] for r in result] == ['a', 'b']
    assert result[0]['query'] == 'glacier lake'


@pytest.mark.parametrize('record, failure', [
    (make_record('x', downloaded=0), 'download'),
    (make_record('x', width=2000, height=3000), 'orientation'),
    (make_record('x', width=2400, height=2000), 'aspect_ratio'),
    (make_record('x', width=1600, height=900), 'resolution'),
    (make_record('x', photographer_name=None), 'missing_fields'),
])
def test_validate_images_rejects(record, failure):
    valid, failed = clean.validate_images([record])
    assert valid == []
    assert failed[failure] == 1


def test_validate_images_accepts_good_record():
    valid, failed = clean.validate_images([make_record('ok')])
    assert len(valid) == 1
    assert sum(failed.values()) == 0


def test_enhance_metadata():
    [record] = clean.enhance_metadata([make_record('a', width=6000, height=4000)])
    assert record['aspect_ratio'] == 1.5
    assert record['megapixels'] == 24.0
    assert record['description'] == 'A lake at dawn'


@pytest.mark.parametrize('text, expected', [
    ('Sunrise over Moraine Lake in Banff, Canada', ('Moraine Lake', 'Canada')),
    ('Kirkjufell mountain', ('Kirkjufell', 'Iceland')),
    ('Black sand beach, Iceland', (None, 'Iceland')),
    ('Ijen crater lake in Indonesia', (None, 'Indonesia')),
    ('Antelope Canyon, a short drive from the Grand Canyon', ('Antelope Canyon', 'United States')),
    ('A trip to Jerusalem', (None, None)),
    ('A quiet forest', (None, None)),
    (None, (None, None)),
])
def test_extract_location_from_text(text, expected):
    assert location.extract_location_from_text(text) == expected


def test_extract_location_prefers_api_location():
    image = {'location': {'name': 'Reine', 'country': 'Norway'}, 'description': 'Iceland'}
    assert location.extract_location(image) == ('Reine', 'Norway')


def test_extract_location_handles_missing_location():
    image = {'location': None, 'description': None, 'alt_description': 'Yellowstone geyser'}
    assert location.extract_location(image) == ('Yellowstone', 'United States')


def test_refine_locations():
    data = [
        make_record('a', location_name='Yellowstone', description='Yellowstone geyser'),
        make_record('b', location_name='Crater Lake', country='Indonesia',
                    description='Ijen crater lake in Indonesia'),
        make_record('c', location_name='Reine', country='Norway', description='Iceland'),
    ]
    result = clean.refine_locations(data)
    assert [(r['location_name'], r['country']) for r in result] == [
        ('Yellowstone', 'United States'),
        (None, 'Indonesia'),
        ('Reine', 'Norway'),
    ]


def test_load_rebuilds_database(tmp_path, monkeypatch):
    cleaned_file = tmp_path / 'cleaned.json'
    monkeypatch.setattr(config, 'CLEANED_METADATA_FILE', cleaned_file)
    monkeypatch.setattr(config, 'DB_DIR', tmp_path)
    monkeypatch.setattr(config, 'DATABASE_FILE', tmp_path / 'images.db')

    cleaned_file.write_text(json.dumps([make_record('a'), make_record('b')]))
    load_sqlite.main()

    # A second run must replace the old contents, not append to them
    cleaned_file.write_text(json.dumps([make_record('c', country='Iceland')]))
    load_sqlite.main()

    conn = sqlite3.connect(config.DATABASE_FILE)
    rows = conn.execute('SELECT id, country, source FROM images').fetchall()
    conn.close()
    assert rows == [('c', 'Iceland', 'unsplash')]
    assert not (tmp_path / 'images.db.tmp').exists()


def test_committed_database_matches_cleaned_metadata():
    with open(config.CLEANED_METADATA_FILE) as f:
        cleaned_ids = {record['id'] for record in json.load(f)}
    conn = sqlite3.connect(f'file:{config.DATABASE_FILE}?mode=ro', uri=True)
    db_ids = {row[0] for row in conn.execute('SELECT id FROM images')}
    conn.close()
    assert db_ids == cleaned_ids
