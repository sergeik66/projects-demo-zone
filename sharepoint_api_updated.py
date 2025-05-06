from typing import Optional, List, Dict, Any
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from ..core.base_api import BaseAPI

class SharePointAPI(BaseAPI):
    """Class for interacting with SharePoint REST API using OAuth 2.0 with SPN and delegated permissions."""
    
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        site_url: str,
        key_vault_url: str,
        refresh_token_secret_name: str = "spn-refresh-token",
        user_agent: str = "SharePointApp/1.0 (your.email@example.com)"
    ):
        """
        Initialize SharePointAPI with Service Principal and delegated permissions.
        
        Args:
            tenant_id: Azure AD tenant ID.
            client_id: Azure AD Service Principal client ID.
            client_secret: Azure AD Service Principal client secret.
            site_url: SharePoint site URL (e.g., https://yourtenant.sharepoint.com/sites/yoursite).
            key_vault_url: Azure Key Vault URL (e.g., https://yourvault.vault.azure.net).
            refresh_token_secret_name: Name of the refresh token secret in Key Vault.
            user_agent: User-Agent header (default: SharePointApp/1.0).
        """
        super().__init__(base_url=f"{site_url}/_api", user_agent=user_agent)
        self.token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        self.client_id = client_id
        self.client_secret = client_secret
        self.site_url = site_url.rstrip('/')
        self.tenant_domain = site_url.split('//')[1].split('/')[0]  # e.g., yourtenant.sharepoint.com
        self.key_vault_url = key_vault_url
        self.refresh_token_secret_name = refresh_token_secret_name
        
        # Initialize Key Vault client
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        self.secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    
    def _get_access_token(self) -> Optional[str]:
        """
        Obtain access token using a stored refresh token for delegated permissions.
        
        Returns:
            Access token or None if the request fails.
        """
        try:
            # Retrieve refresh token from Key Vault
            refresh_token = self.secret_client.get_secret(self.refresh_token_secret_name).value
            if not refresh_token:
                print("No refresh token found in Key Vault.")
                return None
            
            # Request new access token using refresh token
            scope = f"https://{self.tenant_domain}/Sites.Read.All openid profile"
            form_data = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": scope,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            }
            data = self._make_request(
                endpoint=self.token_url,
                method="POST",
                form_data=form_data
            )
            if not data:
                print("Token request failed: No response data.")
                return None
            if "error" in data:
                print(f"Token error: {data.get('error_description', 'Unknown error')}")
                return None
            token = data.get("access_token")
            if token:
                self.set_access_token(token)
                # Update refresh token in Key Vault if a new one is provided
                new_refresh_token = data.get("refresh_token")
                if new_refresh_token:
                    self.secret_client.set_secret(self.refresh_token_secret_name, new_refresh_token)
                return token
            print("Failed to obtain SharePoint access token: No token in response.")
            return None
        except Exception as e:
            print(f"Error obtaining access token: {e}")
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
            print("No access token available for GetFileByServerRelativeUrl.")
            return None
        
        endpoint = f"web/GetFileByServerRelativeUrl('{server_relative_url}')"
        headers = {
            "Accept": "application/json;odata=verbose" if not get_content else "*/*",
            "Authorization": f"Bearer {self.access_token}"  # Explicitly ensure Authorization header
        }
        print(f"Request headers for GetFileByServerRelativeUrl: {headers}")  # Debugging
        
        if not get_content:
            params = {"$select": select} if select else None
            data = self._make_request(endpoint, params=params, headers=headers)
            return data.get("d", {}) if data else None
        else:
            try:
                response = self._make_request(endpoint + "/$value", headers=headers, method="GET")
                if response and isinstance(response, bytes):
                    return response
                print("No file content returned.")
                return None
            except Exception as e:
                print(f"Failed to retrieve file content: {e}")
                return None
