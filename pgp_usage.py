import pgpy
import fsspec
import os
from spark_engine.common.email_util import get_secret_notebookutils

# Generate a new RSA key pair (use the same key pair for both encryption and decryption)
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

# Initialize the PGP class with the key strings
pgp = PGP(
    key_vault_name="",  # Empty since using direct keys
    public_key_secret="",
    private_key_secret="",
    public_key=MOCK_PUBLIC_KEY,
    private_key=MOCK_PRIVATE_KEY
)

# Encrypt a file
input_file = "abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/7c6d771a-3b6f-4042-8a89-1a885973a93c/Files/templates/emails/deduplication_msg.json"
output_encrypted_path = "abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/7c6d771a-3b6f-4042-8a89-1a885973a93c/Files/templates/emails/output"
pgp.encrypt_file(
    input_file=input_file,
    output_path=output_encrypted_path
)

# Decrypt the file
pgp.decrypt_file(
    input_file=f"{output_encrypted_path}/deduplication_msg.json.pgp",
    output_path="abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/7c6d771a-3b6f-4042-8a89-1a885973a93c/Files/templates/emails/decrypted",
    passphrase=None  # Set to None since the private key is unprotected
)

from pgp import PGP  # Adjust import based on your module structure

def decrypt_file(
    input_file: str,
    output_path: str,
    pgp_enabled: bool,
    key_vault_name: str = None,
    private_key_secret: str = None,
    public_key_secret: str = None,
    passphrase: str = None
) -> None:
    """
    Decrypt a file using PGP if pgp_enabled is True and required parameters are provided.
    
    Args:
        input_file: Path to the encrypted input file (e.g., .pgp file).
        output_path: Directory path for the decrypted output file.
        pgp_enabled: Flag to enable/disable PGP decryption.
        key_vault_name: Name of the key vault for retrieving secrets (optional).
        private_key_secret: Secret name for the private key (optional).
        public_key_secret: Secret name for the public key (optional).
        passphrase: Passphrase for the private key (optional).
    
    Raises:
        ValueError: If pgp_enabled is True but key_vault_name or private_key_secret is None.
        FileNotFoundError: If the input file does not exist.
        IOError: If decryption or file operations fail.
    """
    if not pgp_enabled:
        print("PGP decryption is disabled. Skipping decryption.")
        return
    
    if key_vault_name is None or private_key_secret is None:
        raise ValueError("key_vault_name and private_key_secret must be provided when pgp_enabled is True.")

    # Initialize PGP class
    pgp = PGP(
        key_vault_name=key_vault_name,
        public_key_secret=public_key_secret,
        private_key_secret=private_key_secret
    )
    
    # Decrypt the file
    pgp.decrypt_file(
        input_file=input_file,
        output_path=output_path,
        passphrase=passphrase
    )
    print(f"File decrypted successfully to {output_path}")

# Example usage in the notebook
pgp_enabled = True
key_vault_name = "my-key-vault"
private_key_secret = "private-key-secret"
public_key_secret = "public-key-secret"
passphrase = "my-passphrase"

decrypt_file(
    input_file="abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/7c6d771a-3b6f-4042-8a89-1a885973a93c/Files/templates/emails/output/deduplication_msg.json.pgp",
    output_path="abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/7c6d771a-3b6f-4042-8a89-1a885973a93c/Files/templates/emails/decrypted",
    pgp_enabled=pgp_enabled,
    key_vault_name=key_vault_name,
    private_key_secret=private_key_secret,
    public_key_secret=public_key_secret,
    passphrase=passphrase
)
