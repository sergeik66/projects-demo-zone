from typing import Optional, Dict, Any
import requests
from urllib.parse import urljoin

class BaseAPI:
    """Base class for HTTP API interactions with OAuth 2.0 authentication."""
    
    def __init__(self, base_url: str, user_agent: str = "APIClient/1.0"):
        """
        Initialize BaseAPI.
        
        Args:
            base_url: Base URL of the API (e.g., https://api.example.com).
            user_agent: User-Agent header for requests.
        """
        self.base_url = base_url.rstrip('/')
        self.user_agent = user_agent
        self.access_token: Optional[str] = None
    
    def set_access_token(self, token: str) -> None:
        """Set the OAuth 2.0 access token."""
        self.access_token = token
    
    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        form_data: Optional[Dict] = None
    ) -> Optional[Any]:
        """
        Make an HTTP request to the API.
        
        Args:
            endpoint: API endpoint (e.g., '/resource').
            method: HTTP method (GET, POST, etc.).
            params: Query parameters.
            headers: Custom headers (merged with defaults).
            json_data: JSON payload for POST/PUT requests.
            form_data: Form data for POST requests.
        
        Returns:
            Response data (JSON, bytes, or None if failed).
        """
        try:
            # Construct full URL
            url = urljoin(self.base_url, endpoint.lstrip('/'))
            
            # Default headers
            default_headers = {
                "User-Agent": self.user_agent,
                "Authorization": f"Bearer {self.access_token}" if self.access_token else None
            }
            # Remove None values from default headers
            default_headers = {k: v for k, v in default_headers.items() if v is not None}
            
            # Merge custom headers with defaults
            final_headers = default_headers.copy()
            if headers:
                final_headers.update(headers)
            
            # Make request
            response = requests.request(
                method=method,
                url=url,
                params=params,
                headers=final_headers,
                json=json_data,
                data=form_data,
                timeout=30
            )
            response.raise_for_status()
            
            # Handle response
            if response.status_code == 204:
                return None
            if "application/json" in response.headers.get("Content-Type", ""):
                return response.json()
            return response.content  # Return bytes for binary content (e.g., file downloads)
        
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error: {e}, Response: {e.response.text if e.response else 'No response'}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
