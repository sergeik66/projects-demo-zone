import msal
import requests
import json

# Configuration
tenant_id = "{your-tenant-id}"  # Directory (tenant) ID from Microsoft Entra ID
client_id = "{your-client-id}"  # Application (client) ID from app registration
username = "{service-account-username}"  # e.g., [email protected]
password = "{service-account-password}"  # Service account password (store securely in production)
team_id = "fbe2bf47-16c8-47cf-b4a5-4b9b187c508b"  # Replace with your team ID
channel_id = "19:4a95f7d8db4c4e7fae857bcebe0623e6@thread.tacv2"  # Replace with your shared channel ID
authority = f"https://login.microsoftonline.com/{tenant_id}"
scope = ["https://graph.microsoft.com/.default"]

# Initialize MSAL application
app = msal.PublicClientApplication(
    client_id=client_id,
    authority=authority
)

# Acquire token using ROPC
result = app.acquire_token_by_username_password(
    username=username,
    password=password,
    scopes=scope
)

# Check for authentication errors
if "access_token" not in result:
    print(f"Authentication failed: {result.get('error_description')}")
    exit()

# Get the access token
access_token = result["access_token"]

# Set headers for API request
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Post message to the shared channel
url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages"
payload = {
    "body": {
        "contentType": "html",
        "content": "<h2>Release Notes - Version 1.2.3</h2><p>Here are the latest updates...</p>"
    }
}

# Send the request
response = requests.post(url, headers=headers, json=payload)

# Handle the response
if response.status_code == 201:
    print("Message posted successfully!")
    print(json.dumps(response.json(), indent=2))
else:
    print(f"Failed to post message: {response.status_code} - {response.text}")
