import pgpy
import fsspec
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from spark_engine.common.email_util import get_secret_notebookutils
from pgp import PGP  # Adjust import based on your module structure

# Service principal credentials (preferably set as environment variables)
client_id = os.getenv("AZURE_CLIENT_ID", "<your_client_id>")
client_secret = os.getenv("AZURE_CLIENT_SECRET", "<your_client_secret>")
tenant_id = os.getenv("AZURE_TENANT_ID", "<your_tenant_id>")

# Generate a new RSA key pair
key = pgpy.PGPKey.new(pgpy.constants.PubKeyAlgorithm.RSAEncryptOrSign, 2048)

# Add a User ID to the key
uid = pgpy.PGPUID.new("Test User", email="test@example.com")
key.add_uid(
    uid,
    usage={
        pgpy.constants.KeyFlags.Sign,
        pgpy.constants.KeyFlags.EncryptCommunications,
        pgpy.constants.KeyFlags.EncryptStorage
    },
    hashes=[pgpy.constants.HashAlgorithm.SHA256],
    ciphers=[pgpy.constants.SymmetricKeyAlgorithm.AES256],
    compression=[pgpy.constants.CompressionAlgorithm.ZIP]
)

# Extract the public and private keys as ASCII-armored strings
MOCK_PUBLIC_KEY = str(key.pubkey)  # ASCII-armored public key
MOCK_PRIVATE_KEY = str(key)        # ASCII-armored private key

# Optionally save the keys to files for debugging or reuse
with open('mock_public_key.asc', 'w') as pub_file:
    pub_file.write(MOCK_PUBLIC_KEY)

with open('mock_private_key.asc', 'w') as priv_file:
    priv_file.write(MOCK_PRIVATE_KEY)

# Store keys in Azure Key Vault using service principal
key_vault_name = "my-key-vault"
public_key_secret_name = "mock-public-key-secret"
private_key_secret_name = "mock-private-key-secret"

# Initialize Azure Key Vault client with service principal
vault_url = f"https://{key_vault_name}.vault.azure.net"
credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)
secret_client = SecretClient(vault_url=vault_url, credential=credential)

# Set secrets
try:
    secret_client.set_secret(public_key_secret_name, MOCK_PUBLIC_KEY)
    secret_client.set_secret(private_key_secret_name, MOCK_PRIVATE_KEY)
    print(f"Successfully set secrets {public_key_secret_name} and {private_key_secret_name} in {key_vault_name}")
except Exception as e:
    print(f"Failed to set secrets: {str(e)}")
    raise

# Decrypt file function (from previous requirement, processing all .pgp files)
def decrypt_file(
    input_folder: str,
    output_path: str,
    pgp_enabled: bool,
    key_vault_name: str = None,
    private_key_secret: str = None,
    public_key_secret: str = None,
    passphrase: str = None
) -> None:
    """
    Decrypt all files with .pgp extension in the input folder using PGP if pgp_enabled is True and required parameters are provided.
    
    Args:
        input_folder: Path to the folder containing encrypted files (e.g., .pgp files).
        output_path: Directory path for the decrypted output files.
        pgp_enabled: Flag to enable/disable PGP decryption.
        key_vault_name: Name of the key vault for retrieving secrets (optional).
        private_key_secret: Secret name for the private key (optional).
        public_key_secret: Secret name for the public key (optional).
        passphrase: Passphrase for the private key (optional).
    
    Raises:
        ValueError: If pgp_enabled is True but key_vault_name or private_key_secret is None,
                    or if no .pgp files are found in the input folder.
        FileNotFoundError: If the input folder does not exist.
        IOError: If decryption or file operations fail for any file.
    """
    if not pgp_enabled:
        print("PGP decryption is disabled. Skipping decryption.")
        return
    
    if key_vault_name is None or private_key_secret is None:
        raise ValueError("key_vault_name and private_key_secret must be provided when pgp_enabled is True.")

    # Initialize fsspec filesystem for OneLake
    fs = fsspec.filesystem(
        "abfss",
        account_name="onelake",
        account_host="onelake.dfs.fabric.microsoft.com"
    )

    # Check if the input folder exists
    if not fs.exists(input_folder):
        raise FileNotFoundError(f"Input folder not found: {input_folder}")

    # List files in the input folder and filter for .pgp extension
    files = [f for f in fs.ls(input_folder, detail=False) if f.lower().endswith(".pgp")]
    if not files:
        raise ValueError(f"No .pgp files found in {input_folder}")

    # Initialize PGP class
    pgp = PGP(
        key_vault_name=key_vault_name,
        public_key_secret=public_key_secret,
        private_key_secret=private_key_secret
    )
    
    # Decrypt each .pgp file
    for input_file in files:
        try:
            pgp.decrypt_file(
                input_file=input_file,
                output_path=output_path,
                passphrase=passphrase
            )
            print(f"File {input_file} decrypted successfully to {output_path}")
        except Exception as e:
            print(f"Failed to decrypt {input_file}: {str(e)}")
            raise IOError(f"Failed to decrypt {input_file}: {str(e)}")

# Example usage in the notebook
pgp_enabled = True
key_vault_name = "my-key-vault"
private_key_secret = "mock-private-key-secret"
public_key_secret = "mock-public-key-secret"
passphrase = "my-passphrase"

decrypt_file(
    input_folder="abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/7c6d771a-3b6f-4042-8a89-1a885973a93c/Files/templates/emails/output",
    output_path="abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/7c6d771a-3b6f-4042-8a89-1a885973a93c/Files/templates/emails/decrypted",
    pgp_enabled=pgp_enabled,
    key_vault_name=key_vault_name,
    private_key_secret=private_key_secret,
    public_key_secret=public_key_secret,
    passphrase=passphrase
)
