from typing import Optional, List, Dict, Any
from ..core.base_api import BaseAPI

class SharePointAPI(BaseAPI):
    """Class for interacting with SharePoint REST API using OAuth 2.0."""
    
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, site_url: str, user_agent: str = "SharePointApp/1.0 (your.email@example.com)"):
        """
        Initialize SharePointAPI.
        
        Args:
            tenant_id: Azure AD tenant ID.
            client_id: Azure AD application client ID.
            client_secret: Azure AD application client secret.
            site_url: SharePoint site URL (e.g., https://yourtenant.sharepoint.com/sites/yoursite).
            user_agent: User-Agent header (default: SharePointApp/1.0).
        """
        super().__init__(base_url=f"{site_url}/_api", user_agent=user_agent)
        self.token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        self.client_id = client_id
        self.client_secret = client_secret
        self.site_url = site_url.rstrip('/')
    
    def _get_access_token(self) -> Optional[str]:
        """Obtain access token using client credentials flow."""
        form_data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": f"https://{self.site_url.split('//')[1].split('/')[0]}/.default",
            "grant_type": "client_credentials"
        }
        data = self._make_request(
            endpoint=self.token_url,
            method="POST",
            form_data=form_data
        )
        if data:
            token = data.get("access_token")
            if token:
                self.set_access_token(token)
                return token
        print("Failed to obtain SharePoint access token.")
        return None
    
    def fetch_data(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Fetch data from SharePoint REST API.
        
        Args:
            endpoint: API endpoint (e.g., 'web/lists').
            params: Query parameters (e.g., {'$select': 'Title,Id'}).
        
        Returns:
            Dictionary with API response or None if the request fails.
        """
        if not self.access_token and not self._get_access_token():
            return None
        
        headers = {"Accept": "application/json;odata=verbose"}
        data = self._make_request(endpoint, params=params, headers=headers)
        if data:
            return data.get("d", {}).get("results", data.get("d", {}))
        return None
    
    def get_lists(self) -> Optional[List[Dict]]:
        """Get all lists in the SharePoint site."""
        return self.fetch_data("web/lists")
    
    def get_list_items(self, list_title: str, select: Optional[str] = None) -> Optional[List[Dict]]:
        """
        Get items from a SharePoint list.
        
        Args:
            list_title: Title of the list.
            select: OData $select query (e.g., 'Title,Id').
        
        Returns:
            List of items or None if the request fails.
        """
        endpoint = f"web/lists/getbytitle('{list_title}')/items"
        params = {"$select": select} if select else None
        return self.fetch_data(endpoint, params)
    
    def create_list_item(self, list_title: str, item_data: Dict) -> Optional[Dict]:
        """
        Create an item in a SharePoint list.
        
        Args:
            list_title: Title of the list.
            item_data: Dictionary of item fields (e.g., {'Title': 'New Item'}).
        
        Returns:
            Dictionary with created item data or None.
        """
        if not self.access_token and not self._get_access_token():
            return None
        
        endpoint = f"web/lists/getbytitle('{list_title}')/items"
        headers = {
            "Accept": "application/json;odata=verbose",
            "Content-Type": "application/json;odata=verbose"
        }
        json_data = {"__metadata": {"type": f"SP.Data.{list_title.replace(' ', '')}ListItem"}}
        json_data.update(item_data)
        
        return self._make_request(endpoint, method="POST", json_data=json_data, headers=headers)
    
    def get_file_by_server_relative_url(self, server_relative_url: str, select: Optional[str] = None, get_content: bool = False) -> Optional[Dict]:
        """
        Retrieve file metadata or content using GetFileByServerRelativeUrl.
        
        Args:
            server_relative_url: Server-relative URL of the file (e.g., '/sites/yoursite/Shared Documents/myfile.docx').
            select: OData $select query (e.g., 'Name,Length').
            get_content: If True, return file content as bytes; otherwise, return metadata.
        
        Returns:
            Dictionary with file metadata or bytes if get_content=True, or None if the request fails.
        """
        if not self.access_token and not self._get_access_token():
            return None
        
        endpoint = f"web/GetFileByServerRelativeUrl('{server_relative_url}')"
        if not get_content:
            endpoint += "/$value" if not select else ""
            params = {"$select": select} if select else None
            headers = {"Accept": "application/json;odata=verbose"}
            data = self._make_request(endpoint, params=params, headers=headers)
            return data.get("d", {}) if data else None
        else:
            headers = {"Accept": "*/*"}
            try:
                response = self._make_request(endpoint + "/$value", headers=headers, method="GET")
                if response and isinstance(response, bytes):
                    return response
                return None
            except Exception as e:
                print(f"Failed to retrieve file content: {e}")
                return None
