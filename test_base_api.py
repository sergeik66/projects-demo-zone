import pytest
import requests
from requests_mock import Mocker
from base_api import BaseAPI  # Assume the class is in base_api.py

@pytest.fixture
def base_api():
    """Fixture to create a BaseAPI instance."""
    return BaseAPI(base_url="https://api.example.com", user_agent="TestClient/1.0")

def test_init(base_api):
    """Test BaseAPI initialization."""
    assert base_api.base_url == "https://api.example.com"
    assert base_api.user_agent == "TestClient/1.0"
    assert base_api.access_token is None

def test_init_trailing_slash():
    """Test BaseAPI initialization with trailing slash in base_url."""
    api = BaseAPI(base_url="https://api.example.com/", user_agent="TestClient/1.0")
    assert api.base_url == "https://api.example.com"

def test_set_access_token(base_api):
    """Test setting the access token."""
    base_api.set_access_token("test_token")
    assert base_api.access_token == "test_token"

def test_make_request_get_json(base_api, requests_mock: Mocker):
    """Test GET request returning JSON."""
    requests_mock.get(
        "https://api.example.com/test",
        json={"key": "value"},
        headers={"Content-Type": "application/json"},
        status_code=200
    )
    base_api.set_access_token("test_token")
    response = base_api._make_request(endpoint="/test", method="GET")
    
    assert response == {"key": "value"}
    assert requests_mock.last_request.headers["User-Agent"] == "TestClient/1.0"
    assert requests_mock.last_request.headers["Authorization"] == "Bearer test_token"

def test_make_request_post_form_data(base_api, requests_mock: Mocker):
    """Test POST request with form data."""
    requests_mock.post(
        "https://api.example.com/test",
        json={"success": True},
        headers={"Content-Type": "application/json"},
        status_code=200
    )
    form_data = {"field": "value"}
    response = base_api._make_request(
        endpoint="/test",
        method="POST",
        form_data=form_data
    )
    
    assert response == {"success": True}
    assert requests_mock.last_request.headers["User-Agent"] == "TestClient/1.0"
    assert requests_mock.last_request.urlencoded_post_data == "field=value"

def test_make_request_no_content(base_api, requests_mock: Mocker):
    """Test request with 204 No Content response."""
    requests_mock.get(
        "https://api.example.com/test",
        status_code=204
    )
    response = base_api._make_request(endpoint="/test", method="GET")
    
    assert response is None

def test_make_request_binary_content(base_api, requests_mock: Mocker):
    """Test request returning binary content."""
    requests_mock.get(
        "https://api.example.com/test",
        content=b"binary_data",
        headers={"Content-Type": "application/octet-stream"},
        status_code=200
    )
    response = base_api._make_request(endpoint="/test", method="GET")
    
    assert response == b"binary_data"

def test_make_request_custom_headers(base_api, requests_mock: Mocker):
    """Test request with custom headers."""
    requests_mock.get(
        "https://api.example.com/test",
        json={"key": "value"},
        status_code=200
    )
    custom_headers = {"X-Custom-Header": "custom_value"}
    base_api._make_request(
        endpoint="/test",
        method="GET",
        headers=custom_headers
    )
    
    assert requests_mock.last_request.headers["X-Custom-Header"] == "custom_value"
    assert requests_mock.last_request.headers["User-Agent"] == "TestClient/1.0"

def test_make_request_http_error(base_api, requests_mock: Mocker, capsys):
    """Test handling of HTTP error."""
    requests_mock.get(
        "https://api.example.com/test",
        status_code=400,
        text="Bad Request",
    )
    response = base_api._make_request(endpoint="/test", method="GET")
    
    assert response is None
    captured = capsys.readouterr()
    assert "HTTP error: 400 Client Error" in captured.out
    assert "Response: Bad Request" in captured.out

def test_make_request_network_error(base_api, requests_mock: Mocker, capsys):
    """Test handling of network error."""
    requests_mock.get(
        "https://api.example.com/test",
        exc=requests.exceptions.ConnectionError("Connection failed")
    )
    response = base_api._make_request(endpoint="/test", method="GET")
    
    assert response is None
    captured = capsys.readouterr()
    assert "Request error: Connection failed" in captured.out

def test_make_request_endpoint_slashes(base_api, requests_mock: Mocker):
    """Test endpoint with leading/trailing slashes."""
    requests_mock.get(
        "https://api.example.com/test",
        json={"key": "value"},
        status_code=200
    )
    response = base_api._make_request(endpoint="//test//", method="GET")
    
    assert response == {"key": "value"}
    assert requests_mock.last_request.url == "https://api.example.com/test"
