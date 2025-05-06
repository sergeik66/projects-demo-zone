import requests
from pyspark.sql import SparkSession
import os
import json
import base64
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("GraphSharePointConditionalDownload").getOrCreate()

# Hardcoded credentials (replace with your values)
TENANT_ID = "your_tenant_id"
CLIENT_ID = "your_client_id"
CLIENT_SECRET = "your_client_secret"
SITE_HOST = "guardinsurancegroup.sharepoint.com"
SITE_PATH = "/sites/DnAFluidityPlatform-ReferenceDataProducts"
FILE_PATH = "/Shared Documents/myfile.docx"  # Relative to site root
USER_AGENT = "FabricApp/1.0 spn-gdap-sharepoint"
LAKEHOUSE_FILE_PATH = "sharepoint_files/myfile.docx"  # Output path in lakehouse
LAST_TIME_MODIFIED = "2025-02-04"  # Comparison date

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
        print(f"Token request: URL={token_url}, Scope={scope}")
        response = requests.post(token_url, data=payload, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        token_data = response.json()
        if "access_token" in token_data:
            print(f"Access token obtained: {token_data['access_token'][:20]}...")
            return token_data["access_token"]
        else:
            print(f"Token error: {token_data.get('error_description', 'No token returned')}")
            return None
    except requests.exceptions.HTTPError as e:
        print(f"Token HTTP error: {e}, Response: {e.response.text}")
        return None
    except Exception as e:
        print(f"Error obtaining access token: {e}")
        return None

# Step 2: Decode and validate token
def validate_token(access_token):
    try:
        # Split JWT into header, payload, signature
        payload_b64 = access_token.split('.')[1]
        payload_b64 += '=' * (4 - len(payload_b64) % 4)  # Add padding
        payload_json = base64.b64decode(payload_b64).decode('utf-8')
        payload = json.loads(payload_json)
        print(f"Token payload: {json.dumps(payload, indent=2)}")
        # Check key claims
        required_audience = "https://graph.microsoft.com"
        required_permissions = ["Sites.Read.All", "Files.Read.All"]
        audience = payload.get("aud")
        roles = payload.get("roles", [])
        tenant_id = payload.get("tid")
        if audience != required_audience:
            print(f"Invalid token audience: Expected {required_audience}, Got {audience}")
            return False
        if not roles:
            print("No roles found in token. Ensure Microsoft Graph application permissions (Sites.Read.All, Files.Read.All) are assigned.")
            return False
        missing_permissions = [p for p in required_permissions if p not in roles]
        if missing_permissions:
            print(f"Missing required permissions: {missing_permissions}")
            return False
        if tenant_id != TENANT_ID:
            print(f"Invalid tenant ID: Expected {TENANT_ID}, Got {tenant_id}")
            return False
        print("Token validation successful! Roles found:", roles)
        return True
    except Exception as e:
        print(f"Error validating token: {e}")
        return False

# Step 3: Get SharePoint site ID
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
    except requests.exceptions.HTTPError as e:
        print(f"Site ID HTTP error: {e}, Response: {e.response.text}")
        return None
    except Exception as e:
        print(f"Error retrieving site ID: {e}")
        return None

# Step 4: Get file metadata
def get_file_metadata(access_token, site_id):
    graph_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{FILE_PATH}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT
    }
    try:
        print(f"Metadata request: URL={graph_url}, Headers={headers}")
        response = requests.get(graph_url, headers=headers)
        response.raise_for_status()
        metadata = response.json()
        # Map Graph API fields to SharePoint REST API equivalents
        result = {
            "Name": metadata.get("name"),
            "Length": metadata.get("size"),
            "TimeLastModified": metadata.get("lastModifiedDateTime")
        }
        print(f"File metadata: {result}")
        return result
    except requests.exceptions.HTTPError as e:
        print(f"Metadata HTTP error: {e}, Response: {e.response.text}")
        return None
    except Exception as e:
        print(f"Error retrieving metadata: {e}")
        return None

# Step 5: Download file using Microsoft Graph API
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
        print(f"Graph file HTTP error: {e}, Response: {e.response.text}")
        return None
    except Exception as e:
        print(f"Graph file request error: {e}")
        return None

# Step 6: Save file to lakehouse
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
    # Parse last modified time
    try:
        last_time_modified = datetime.strptime(LAST_TIME_MODIFIED, "%Y-%m-%d")
    except ValueError as e:
        print(f"Error parsing LAST_TIME_MODIFIED: {e}")
        return
    
    # Get access token
    access_token = get_access_token()
    if not access_token:
        print("Failed to obtain access token. Exiting.")
        return
    
    # Validate token
    if not validate_token(access_token):
        print("Token validation failed. Please add Microsoft Graph application permissions (Sites.Read.All, Files.Read.All) to the Service Principal.")
        return
    
    # Get site ID
    site_id = get_site_id(access_token)
    if not site_id:
        print("Failed to retrieve site ID. Exiting.")
        # Fallback: Hardcode site ID if known
        # site_id = "guardinsurancegroup.sharepoint.com,<site_guid>"
        # print(f"Using hardcoded site ID: {site_id}")
        return
    
    # Get file metadata
    metadata = get_file_metadata(access_token, site_id)
    if not metadata:
        print("Failed to retrieve file metadata. Exiting.")
        return
    
    # Compare TimeLastModified
    try:
        file_modified_time = datetime.strptime(metadata["TimeLastModified"], "%Y-%m-%dT%H:%M:%SZ")
        if file_modified_time <= last_time_modified:
            print(f"File not modified since {LAST_TIME_MODIFIED} (Last modified: {metadata['TimeLastModified']}). Skipping download.")
            return
        print(f"File modified after {LAST_TIME_MODIFIED} (Last modified: {metadata['TimeLastModified']}). Proceeding with download.")
    except ValueError as e:
        print(f"Error parsing TimeLastModified: {e}")
        return
    
    # Download file
    file_content = download_graph_file(access_token, site_id)
    if not file_content:
        print("Failed to download file. Exiting.")
        return
    
    # Save to lakehouse
    if save_to_lakehouse(file_content, LAKEHOUSE_FILE_PATH):
        print("SharePoint file download validated successfully!")
    else:
        print("Failed to save file to lakehouse.")

# Run the script
main()
