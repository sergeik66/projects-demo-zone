PGP Class Documentation
Overview
The PGP class (spark_engine/common/pgp_6_2.py) provides functionality for encrypting and decrypting files using Pretty Good Privacy (PGP) in a Microsoft Fabric environment. It integrates with OneLake storage via fsspec and retrieves cryptographic keys and passphrases from Azure Key Vault. The class is designed to handle .pgp files, such as encrypted CSV files (e.g., BOP PD & OS.csv.pgp), ensuring secure data processing in data pipelines.
Key features:
Encrypts files using a public key with automatic User ID addition if missing.

Decrypts .pgp files using a private key and passphrase.

Validates abfss:// paths for OneLake compatibility.

Loads keys and passphrases on-demand to prevent state-related errors (e.g., PGPError: is_unlocked).

Supports Azure Key Vault integration for secure secret management.

Class Details
Module
Path: spark_engine/common/pgp_6_2.py

Dependencies:
fsspec: For OneLake file system access.

pgpy: For PGP encryption/decryption.

os: For path manipulation.

spark_engine.common.email_util: For Key Vault secret retrieval (get_secret_notebookutils).

Class
python

class PGP:
    """A class to handle PGP encryption/decryption of files using fsspec and pgpy."""

Constructor
python

def __init__(
    self,
    key_vault_name: str,
    file_system_code: str = "abfss",
    public_key_secret: Optional[str] = None,
    private_key_secret: Optional[str] = None,
    public_key: Optional[str] = None,
    private_key: Optional[str] = None,
    passphrase_secret: Optional[str] = None
) -> None:

Parameters
key_vault_name (str): Name of the Azure Key Vault containing secrets.

file_system_code (str, optional): File system protocol (default: "abfss" for OneLake).

public_key_secret (str, optional): Secret name for the public key in Key Vault.

private_key_secret (str, optional): Secret name for the private key in Key Vault.

public_key (str, optional): Raw public key string (alternative to public_key_secret).

private_key (str, optional): Raw private key string (alternative to private_key_secret).

passphrase_secret (str, optional): Secret name for the passphrase in Key Vault.

Behavior
Initializes an fsspec filesystem for OneLake using abfss.

Loads and validates the public key from Key Vault or the provided string.

Adds a default User ID ("Anonymous") to the public key if none exists.

Stores configuration for later use in encryption/decryption.

Raises ValueError if neither public_key_secret nor public_key is provided.

Methods
encrypt_file
python

def encrypt_file(self, input_file: str, output_path: str) -> 'PGP':

Purpose: Encrypts a file using the public key and saves it with a .pgp extension.
Parameters:
input_file (str): Full abfss:// path to the input file (e.g., abfss://.../decrypted/BOP PD & OS.csv).

output_path (str): abfss:// path to the output directory (e.g., abfss://.../encrypted).

Returns: PGP: Self, for method chaining.
Behavior:
Validates that input_file starts with abfss:// and exists.

Reads the input file in chunks to handle large files.

Encrypts the content using the public key with pgpy.

Saves the encrypted file to output_path/<basename>.pgp.

Raises:
ValueError: If input_file lacks abfss://.

FileNotFoundError: If input_file does not exist.

IOError: If encryption or file writing fails.

decrypt_file
python

def decrypt_file(self, input_file: str, output_path: str) -> 'PGP':

Purpose: Decrypts a .pgp file using the private key and passphrase.
Parameters:
input_file (str): Full abfss:// path to the encrypted .pgp file (e.g., abfss://.../encrypted/BOP PD & OS.csv.pgp).

output_path (str): abfss:// path to the output directory (e.g., abfss://.../decrypted).

Returns: PGP: Self, for method chaining.
Behavior:
Validates that input_file starts with abfss:// and exists.

Loads the private key and passphrase from Key Vault or provided strings.

Reads the encrypted file in chunks.

Decrypts the content using pgpy.PGPKey and the passphrase.

Saves the decrypted file to output_path/<basename> (removing .pgp).

Logs debug information (passphrase length, key details, fingerprint).

Raises:
ValueError: If input_file lacks abfss://, or if required secrets are missing.

FileNotFoundError: If input_file does not exist.

IOError: If decryption or file writing fails.

How to Use the PGP Class
Prerequisites
Install Dependencies:
bash

%pip install pgpy fsspec azure-keyvault-secrets azure-identity

Set Environment Variables:
python

import os
os.environ["AZURE_CLIENT_ID"] = "<your_client_id>"
os.environ["AZURE_CLIENT_SECRET"] = "<your_client_secret>"
os.environ["AZURE_TENANT_ID"] = "<your_tenant_id>"

Key Vault Setup:
Store public key, private key, and passphrase in Azure Key Vault.

Grant the service principal read access:
bash

az keyvault set-policy --name my-key-vault --spn <client_id> --secret-permissions get

File Storage:
Ensure input files are in OneLake with abfss:// paths.

Example: abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Files/....

Usage Examples
Example 1: Encrypt a CSV File
Encrypt a CSV file (BOP PD & OS.csv) and save the .pgp file to an output directory.
python

from spark_engine.common.pgp_6_2 import PGP
import fsspec

# Initialize PGP
pgp = PGP(
    key_vault_name="my-key-vault",
    public_key_secret="mock-public-key-secret",
    private_key_secret="mock-private-key-secret",
    passphrase_secret="mock-passphrase-secret"
)

# Define paths
input_file = "abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/f1da4f29-81af-4030-9190-889e5fa725bf/Files/user_data/pdos_webreport/decrypted/BOP PD & OS.csv"
output_path = "abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/f1da4f29-81af-4030-9190-889e5fa725bf/Files/user_data/pdos_webreport/encrypted"

# Encrypt file
pgp.encrypt_file(input_file, output_path)
print(f"Encrypted file saved to {output_path}/BOP PD & OS.csv.pgp")

Example 2: Decrypt a .pgp File
Decrypt a .pgp file and save the output CSV, then delete the encrypted file.
python

from spark_engine.common.pgp_6_2 import PGP
import fsspec

# Initialize filesystem
fs = fsspec.filesystem(
    "abfss",
    account_name="onelake",
    account_host="onelake.dfs.fabric.microsoft.com",
    client_id=os.getenv("AZURE_CLIENT_ID"),
    client_secret=os.getenv("AZURE_CLIENT_SECRET"),
    tenant_id=os.getenv("AZURE_TENANT_ID")
)

# Initialize PGP
pgp = PGP(
    key_vault_name="my-key-vault",
    public_key_secret="mock-public-key-secret",
    private_key_secret="mock-private-key-secret",
    passphrase_secret="mock-passphrase-secret"
)

# Define paths
input_file = "abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/f1da4f29-81af-4030-9190-889e5fa725bf/Files/user_data/pdos_webreport/encrypted/BOP PD & OS.csv.pgp"
output_path = "abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/f1da4f29-81af-4030-9190-889e5fa725bf/Files/user_data/pdos_webreport/decrypted"

# Decrypt file
pgp.decrypt_file(input_file, output_path)

# Verify content
decrypted_file = f"{output_path}/BOP PD & OS.csv"
with fs.open(decrypted_file, "r") as f:
    print(f"Decrypted content preview: {f.read()[:100]}...")

# Delete encrypted file
if fs.exists(input_file):
    fs.delete(input_file)
    print(f"Deleted encrypted file: {input_file}")

Example 3: Integrate with Notebook
Use the PGP class in a notebook to decrypt multiple .pgp files, as in your refactored notebook.
python

from spark_engine.common.pgp_6_2 import PGP
import fsspec
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def decrypt_files(input_folder: str, output_folder: str, key_vault_name: str, private_key_secret: str, public_key_secret: str, passphrase_secret: str):
    fs = fsspec.filesystem("abfss", account_name="onelake", account_host="onelake.dfs.fabric.microsoft.com")
    if not fs.exists(input_folder):
        raise FileNotFoundError(f"Input folder not found: {input_folder}")
    
    files = [f for f in fs.ls(input_folder) if f.endswith(".csv.pgp")]
    if not files:
        raise ValueError(f"No .csv.pgp files found in {input_folder}")

    pgp = PGP(
        key_vault_name=key_vault_name,
        public_key_secret=public_key_secret,
        private_key_secret=private_key_secret,
        passphrase_secret=passphrase_secret
    )

    for input_file in files:
        try:
            decrypted_file = f"{output_folder}/{os.path.basename(input_file).removesuffix('.pgp')}"
            if fs.exists(decrypted_file):
                fs.delete(decrypted_file)
            pgp.decrypt_file(input_file, output_folder)
            with fs.open(decrypted_file, "r") as f:
                logger.info(f"Decrypted {decrypted_file}, content: {f.read()[:100]}...")
            fs.delete(input_file)
            logger.info(f"Deleted encrypted file: {input_file}")
        except Exception as e:
            logger.error(f"Failed to decrypt {input_file}: {str(e)}")
            raise

# Usage
decrypt_files(
    input_folder="abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/f1da4f29-81af-4030-9190-889e5fa725bf/Files/user_data/pdos_webreport/encrypted",
    output_folder="abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/f1da4f29-81af-4030-9190-889e5fa725bf/Files/user_data/pdos_webreport/decrypted",
    key_vault_name="my-key-vault",
    private_key_secret="mock-private-key-secret",
    public_key_secret="mock-public-key-secret",
    passphrase_secret="mock-passphrase-secret"
)

Troubleshooting
Missing abfss://: Ensure all paths start with abfss://. The class raises ValueError if not.

PGPError: is_unlocked: Use the provided PGP class version, which loads keys on-demand.

"Test content" Issue: Filter for .csv.pgp files and avoid test files in your pipeline.

File Not Found: Verify file paths using fs.exists() before processing.

Key Vault Errors: Check secret names and permissions in Azure Key Vault.

Debugging:
Enable debug logs in the notebook to view passphrase length, key fingerprints, etc.

Example:
python

logging.getLogger().setLevel(logging.DEBUG)

Best Practices
Secure Secrets:
Store keys and passphrases in Azure Key Vault, not in code.

Use strong passphrases (e.g., 28 characters, as in your case).

Validate Paths:
Always use full abfss:// paths for OneLake files.

Check folder existence before processing.

Clean Up:
Delete encrypted .pgp files after decryption to save storage (as implemented).

Remove temporary decrypted files if no longer needed.

Error Handling:
Wrap encrypt_file and decrypt_file calls in try-except blocks.

Log errors with file paths and details.

Testing:
Test with a small CSV file before processing large datasets.

Verify decrypted content matches the original.

