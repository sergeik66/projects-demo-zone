import msal
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

def get_secret(secret_name, key_vault_name):
    key_vault_uri = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_uri, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value

def get_access_token(client_id, client_secret, tenant_id):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret
    )
    scope = ["https://graph.microsoft.com/.default"]
    result = app.acquire_token_for_client(scopes=scope)
    return result['access_token']

def send_email(subject, body, to_email, key_vault_name):
    client_id = get_secret("client_id", key_vault_name)
    client_secret = get_secret("client_secret", key_vault_name)
    tenant_id = get_secret("tenant_id", key_vault_name)
    
    access_token = get_access_token(client_id, client_secret, tenant_id)
    endpoint = "https://graph.microsoft.com/v1.0/me/sendMail"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    email_msg = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": to_email
                    }
                }
            ]
        }
    }
    response = requests.post(endpoint, headers=headers, json=email_msg)
    if response.status_code == 202:
        print('Mail Sent')
    else:
        print(f'Error: {response.status_code}')
        print(response.json())

# Example usage
key_vault_name = "your_key_vault_name"
send_email("Test Subject", "This is a test email body.", "recipient@example.com", key_vault_name)
