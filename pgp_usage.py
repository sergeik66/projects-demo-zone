import pgpy
import fsspec
import os
from spark_engine.common.email_util import get_secret_notebookutils

# Generate a new RSA key pair
key, _ = pgpy.PGPKey.new(pgpy.constants.PubKeyAlgorithm.RSAEncryptOrSign, 2048)

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
pgp.encrypt_file(
    input_file="abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/7c6d771a-3b6f-4042-8a89-1a885973a93c/Files/templates/emails/deduplication_msg.json",
    output_path="abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/7c6d771a-3b6f-4042-8a89-1a885973a93c/Files/templates/emails/output"
)
