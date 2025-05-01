from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import requests
from urllib.parse import urljoin

class BaseAPI(ABC):
    """Base class for API interactions with enhanced HTTP request handling."""
    
    def __init__(self, base_url: str, user_agent: str, default_headers: Optional[Dict] = None):
        self.base_url = base_url.rstrip('/')
        self.headers = {"User-Agent": user_agent}
        if default_headers:
            self.headers.update(default_headers)
        self.timeout = 10
        self.access_token = None
    
    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        form_data: Optional[Dict] = None,
        headers: Optional[Dict] = None
    ) -> Optional[Dict]:
        url = urljoin(self.base_url, endpoint.lstrip('/'))
        request_headers = self.headers.copy()
        if headers:
            request_headers.update(headers)
        
        try:
            if method.upper() == "POST":
                if json_data and form_data:
                    raise ValueError("Cannot use both json_data and form_data simultaneously.")
                
                if json_data:
                    request_headers["Content-Type"] = "application/json"
                    response = requests.post(
                        url,
                        headers=request_headers,
                        params=params,
                        json=json_data,
                        timeout=self.timeout
                    )
                elif form_data:
                    request_headers["Content-Type"] = "application/x-www-form-urlencoded"
                    response = requests.post(
                        url,
                        headers=request_headers,
                        params=params,
                        data=form_data,
                        timeout=self.timeout
                    )
                else:
                    response = requests.post(
                        url,
                        headers=request_headers,
                        params=params,
                        timeout=self.timeout
                    )
            else:
                response = requests.request(
                    method.upper(),
                    url,
                    headers=request_headers,
                    params=params,
                    timeout=self.timeout
                )
            
            response.raise_for_status()
            return response.json() if response.content else {}
            
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error for {method} {url}: {http_err}")
        except requests.exceptions.RequestException as err:
            print(f"Request error for {method} {url}: {err}")
        except ValueError as e:
            print(f"Error for {method} {url}: {e}")
        return None
    
    def set_access_token(self, token: str) -> None:
        self.access_token = token
        self.headers["Authorization"] = f"Bearer {self.access_token}"
    
    @abstractmethod
    def fetch_data(self, *args, **kwargs) -> Optional[Any]:
        pass
