import requests
from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder.appName("GraphSharePointFileDownload").getOrCreate()

# Hardcoded credentials (replace with your values)
TENANT_ID = "your_tenant_id"
CLIENT_ID = "your_client_id"
CLIENT_SECRET = "your_client_secret"
SITE_HOST = "guardinsurancegroup.sharepoint.com"
SITE_PATH = "/sites/yoursite"  # e.g., /sites/yoursite
FILE_PATH = "/Shared Documents/myfile.docx"  # Relative to site root
USER_AGENT = "FabricApp/1.0 (your.email@example.com)"
LAKEHOUSE_FILE_PATH = "sharepoint_files/myfile.docx"  # Output path in lakehouse

# Optional: Use Azure Key Vault for credentials (recommended for security)
"""
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
credential = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
secret_client = SecretClient(vault_url="https://yourvault.vault.azure.net", credential=credential)
CLIENT_SECRET = secret_client.get_secret("client-secret").value
"""

# Step 1: Get access token for Service Principal
def get_access_token():
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    scope = "https://graph.microsoft.com/.default"  # App-only scope for Graph API
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": scope,
        "grant_type": "client_credentials"
    }
    try:
        response = requests.post(token_url, data=payload, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        token_data = response.json()
        if "access_token" in token_data:
            print(f"Access token obtained: {token_data['access_token'][:20]}...")
            return token_data["access_token"]
        else:
            print(f"Token error: {token_data.get('error_description', 'No token returned')}")
            return None
    except Exception as e:
        print(f"Error obtaining access token: {e}")
        return None

# Step 2: Get SharePoint site ID
def get_site_id(access_token):
    graph_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_HOST}:{SITE_PATH}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT
    }
    try:
        print(f"Site ID request: URL={graph_url}, Headers={headers}")
        response = requests.get(graph_url, headers=headers)
        response.raise_for_status()
        site_data = response.json()
        if "id" in site_data:
            print(f"Site ID: {site_data['id']}")
            return site_data["id"]
        else:
            print(f"Site ID error: {site_data.get('error', 'No site ID returned')}")
            return None
    except Exception as e:
        print(f"Error retrieving site ID: {e}")
        return None

# Step 3: Download file using Microsoft Graph API
def download_graph_file(access_token, site_id):
    graph_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{FILE_PATH}:/content"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "*/*",
        "User-Agent": USER_AGENT
    }
    try:
        print(f"Graph file request: URL={graph_url}, Headers={headers}")
        response = requests.get(graph_url, headers=headers, stream=True)
        response.raise_for_status()
        return response.content  # Binary file content
    except requests.exceptions.HTTPError as e:
        print(f"Graph HTTP error: {e}, Response: {e.response.text}")
        return None
    except Exception as e:
        print(f"Graph request error: {e}")
        return None

# Step 4: Save file to lakehouse
def save_to_lakehouse(file_content, file_path):
    try:
        lakehouse_path = f"/lakehouse/default/Files/{file_path}"
        os.makedirs(os.path.dirname(lakehouse_path), exist_ok=True)
        with open(lakehouse_path, "wb") as f:
            f.write(file_content)
        print(f"File saved to lakehouse: {lakehouse_path}")
        return True
    except Exception as e:
        print(f"Error saving file to lakehouse: {e}")
        return False

# Main execution
def main():
    # Get access token
    access_token = get_access_token()
    if not access_token:
        print("Failed to obtain access token. Exiting.")
        return
    
    # Get site ID
    site_id = get_site_id(access_token)
    if not site_id:
        print("Failed to retrieve site ID. Exiting.")
        return
    
    # Download file
    file_content = download_graph_file(access_token, site_id)
    if not file_content:
        print("Failed to download file. Exiting.")
        return
    
    # Save to
