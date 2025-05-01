import pytest
import requests
from unittest.mock import patch, Mock
from api_client.base_api import BaseAPI

class TestBaseAPI(BaseAPI):
    def fetch_data(self, *args, **kwargs):
        pass

@pytest.fixture
def base_api():
    return TestBaseAPI(base_url="https://api.example.com", user_agent="TestApp/1.0")

def test_make_request_get_success(base_api):
    with patch("requests.request") as mock_request:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_request.return_value = mock_response
        
        result = base_api._make_request("/test", method="GET")
        assert result == {"data": "test"}
        mock_request.assert_called_once_with(
            "GET",
            "https://api.example.com/test",
            headers={"User-Agent": "TestApp/1.0"},
            params=None,
            timeout=10
        )

def test_make_request_post_json_success(base_api):
    with patch("requests.post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "post_test"}
        mock_post.return_value = mock_response
        
        result = base_api._make_request(
            "/test",
            method="POST",
            json_data={"key": "value"}
        )
        assert result == {"data": "post_test"}
        mock_post.assert_called_once_with(
            "https://api.example.com/test",
            headers={"User-Agent": "TestApp/1.0", "Content-Type": "application/json"},
            params=None,
            json={"key": "value"},
            timeout=10
        )

def test_make_request_post_form_success(base_api):
    with patch("requests.post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "form_test"}
        mock_post.return_value = mock_response
        
        result = base_api._make_request(
            "/test",
            method="POST",
            form_data={"key": "value"}
        )
        assert result == {"data": "form_test"}
        mock_post.assert_called_once_with(
            "https://api.example.com/test",
            headers={"User-Agent": "TestApp/1.0", "Content-Type": "application/x-www-form-urlencoded"},
            params=None,
            data={"key": "value"},
            timeout=10
        )

def test_make_request_http_error(base_api):
    with patch("requests.request") as mock_request:
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_request.return_value = mock_response
        
        result = base_api._make_request("/test", method="GET")
        assert result is None

def test_make_request_invalid_json(base_api):
    with patch("requests.request") as mock_request:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_request.return_value = mock_response
        
        result = base_api._make_request("/test", method="GET")
        assert result is None

def test_make_request_json_and_form_error(base_api):
    with pytest.raises(ValueError, match="Cannot use both json_data and form_data simultaneously"):
        base_api._make_request("/test", method="POST", json_data={"a": 1}, form_data={"b": 2})

def test_set_access_token(base_api):
    base_api.set_access_token("test_token")
    assert base_api.access_token == "test_token"
    assert base_api.headers["Authorization"] == "Bearer test_token"
