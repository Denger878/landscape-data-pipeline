"""API tests against the committed database."""
import pytest

import api


@pytest.fixture
def client():
    api.app.config['TESTING'] = True
    return api.app.test_client()


def assert_valid_image(data):
    assert data['id']
    assert data['imageUrl'].startswith('https://images.unsplash.com/')
    assert data['unsplashLink'].startswith('https://unsplash.com/photos/')
    assert data['photographer']['name']
    assert data['photographer']['profile'] == (
        f"https://unsplash.com/@{data['photographer']['username']}"
    )


def test_health(client):
    response = client.get('/api/health')
    assert response.status_code == 200
    assert response.get_json()['status'] == 'healthy'


def test_index_lists_endpoints(client):
    response = client.get('/')
    assert response.status_code == 200
    assert '/api/random' in response.get_json()['endpoints']


def test_random_image(client):
    response = client.get('/api/random')
    assert response.status_code == 200
    body = response.get_json()
    assert body['success'] is True
    assert_valid_image(body['data'])


def test_random_image_with_location_has_caption(client):
    for _ in range(10):
        body = client.get('/api/random/location').get_json()
        assert body['success'] is True
        assert_valid_image(body['data'])
        assert body['data']['caption']


def test_stats(client):
    body = client.get('/api/stats').get_json()
    stats = body['data']
    assert body['success'] is True
    assert stats['total'] > 0
    assert 0 <= stats['locationCoverage'] <= 100
    assert len(stats['topCountries']) <= 5


def test_cors_header(client):
    response = client.get('/api/random', headers={'Origin': 'https://example.com'})
    assert response.headers['Access-Control-Allow-Origin'] in ('*', 'https://example.com')


def test_missing_database_returns_500(client, monkeypatch, tmp_path):
    monkeypatch.setattr(api.config, 'DATABASE_FILE', tmp_path / 'missing.db')
    response = client.get('/api/random')
    assert response.status_code == 500
    assert response.get_json()['success'] is False


@pytest.mark.parametrize('row, expected', [
    ({'location_name': 'Lofoten Islands', 'country': 'Norway'}, 'Lofoten Islands, Norway'),
    ({'location_name': 'Bryce Canyon', 'country': None}, 'Bryce Canyon'),
    ({'location_name': None, 'country': 'Iceland'}, 'Iceland'),
    ({'location_name': 'Maui, Hawaii, USA', 'country': 'United States'}, 'Maui, Hawaii, USA'),
    ({'location_name': 'Brúarfoss, Iceland', 'country': 'Iceland'}, 'Brúarfoss, Iceland'),
    ({'location_name': None, 'country': None}, None),
])
def test_build_caption(row, expected):
    assert api.build_caption(row) == expected
