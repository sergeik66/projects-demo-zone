The script supports:
Encryption of a file using a public key.

Decryption of a .pgp file using a private key and passphrase.

Command-line arguments for input/output files, key files, and passphrase.

Optional deletion of encrypted files after decryption.

Logging for debugging and verification.

No GPG dependency, using .pgp files as specified.

Features
Command-Line Interface: Uses argparse for flexible arguments.

Cross-Platform: Runs on local machines or cloud environments (no Fabric dependencies).

Secure Key Handling: Loads keys fresh for each operation to avoid state issues.

File Deletion: Supports deleting encrypted .pgp files after decryption (as per your request).

Logging: Provides detailed logs with content previews for verification.

Error Handling: Robust validation and exception handling.

No GPG: Uses pgpy for .pgp files, as specified.

Prerequisites
Install Dependencies:
bash

pip install pgpy

Prepare Keys:
Generate or obtain PGP public and private keys (ASCII-armored).

Example key generation (using pgpy):
python

from pgpy import PGPKey, PGPUID
key = PGPKey.new(pgpy.constants.PubKeyAlgorithm.RSAEncryptOrSign, 2048)
uid = PGPUID.new("Anonymous")
key.add_uid(uid)
with open("public_key.asc", "w") as f:
    f.write(str(key.pubkey))
with open("private_key.asc", "w") as f:
    f.write(str(key))

Ensure the private key is protected with a passphrase (e.g., SunsetM0untain$2025SecureKey123).

File Setup:
Input file (e.g., BOP PD & OS.csv) and output directories must exist.

Encrypted files should have .pgp extension for decryption.

Usage Instructions
Save the Script:
Save as pgp_tool.py.

Make executable (Linux/Mac):
bash

chmod +x pgp_tool.py

Encrypt a File:
bash

./pgp_tool.py encrypt \
    --input-file BOP PD & OS.csv \
    --output-dir encrypted \
    --public-key public_key.asc \
    --verbose

Output: encrypted/BOP PD & OS.csv.pgp

Logs: File path, encryption status, and errors (if any).

Decrypt a File:
bash

./pgp_tool.py decrypt \
    --input-file encrypted/BOP PD & OS.csv.pgp \
    --output-dir decrypted \
    --private-key private_key.asc \
    --passphrase "SunsetM0untain$2025SecureKey123" \
    --delete-encrypted \
    --verbose

Output: decrypted/BOP PD & OS.csv

Deletes: encrypted/BOP PD & OS.csv.pgp (if --delete-encrypted is used).

Logs: Passphrase details, key fingerprint, content preview, and deletion status.

Example Workflow:
bash

# Create directories
mkdir -p encrypted decrypted

# Encrypt
./pgp_tool.py encrypt --input-file data.csv --output-dir encrypted --public-key public_key.asc

# Decrypt
./pgp_tool.py decrypt --input-file encrypted/data.csv.pgp --output-dir decrypted --private-key private_key.asc --passphrase "your_passphrase" --delete-encrypted

Troubleshooting
File Not Found: Ensure input_file and output_dir exist. Check paths with absolute paths if relative paths fail.

Invalid Key: Verify key files are ASCII-armored and readable. Check logs for key loading errors.

Passphrase Error: Ensure the passphrase matches the private key. Use --verbose to debug.

PGPError: The script loads keys fresh, avoiding the is_unlocked issue. If it occurs, verify key integrity.

Content Issues: If decrypted content is incorrect (e.g., "Test content"), ensure the correct .pgp file is processed.

Cloud Deployment:
For cloud environments, store keys securely (e.g., AWS Secrets Manager, Azure Key Vault) and modify the script to fetch them.

Example for Azure Key Vault:
python

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://my-key-vault.vault.azure.net", credential=credential)
passphrase = client.get_secret("mock-passphrase-secret").value

Example Output
Encryption:
bash

2025-06-04 09:38:00 - INFO - Encrypted data.csv to encrypted/data.csv.pgp

Decryption:
bash

2025-06-04 09:38:05 - DEBUG - Passphrase length: 28
2025-06-04 09:38:05 - DEBUG - Passphrase start: Sunse...
2025-06-04 09:38:05 - DEBUG - Private key protected: True
2025-06-04 09:38:05 - DEBUG - Private key fingerprint: 1234567890ABCDEF
2025-06-04 09:38:05 - INFO - Decrypted encrypted/data.csv.pgp to decrypted/data.csv
2025-06-04 09:38:05 - INFO - Decrypted content preview: col1,col2\nval1,val2...
2025-06-04 09:38:05 - INFO - Deleted encrypted file: encrypted/data.csv.pgp

Notes
Portability: No Fabric or cloud-specific dependencies, suitable for local or cloud use.

Security: Avoid hardcoding passphrases; use environment variables or secret managers in production.

Scalability: Handles large files via chunked reading.

Context: Supports your .pgp file workflow with CSV files, key User IDs, and passphrase requirements.

If Issues Persist:
Share command-line output and logs (--verbose).

Provide key file snippets (first few lines) and input file details.

Specify the environment (e.g., local, AWS, Azure).

