PGP Tool
A command-line Python script (pgp_tool.py) for encrypting and decrypting files using Pretty Good Privacy (PGP). This tool is designed to be portable across environments, including local machines and cloud platforms (e.g., AWS, Azure, GCP), without requiring Microsoft Fabric or cloud-specific dependencies. It supports .pgp files, such as encrypted CSVs (e.g., BOP PD & OS.csv.pgp), and provides secure file encryption/decryption using pgpy.
Features
Encrypt Files: Encrypt any file (e.g., CSV, text) using a PGP public key, appending a .pgp extension.

Decrypt Files: Decrypt .pgp files using a private key and passphrase, with optional deletion of encrypted files.

Command-Line Interface: Simple and intuitive arguments via argparse.

Cross-Platform: Runs on Windows, Linux, macOS, or cloud environments without Fabric dependencies.

Secure Key Handling: Loads keys fresh for each operation to avoid state issues (e.g., PGPError: is_unlocked).

Logging: Detailed logs with file paths, key details, and content previews for debugging.

No GPG Dependency: Uses pgpy for .pgp file operations, as specified.

Large File Support: Processes files in chunks to handle large datasets efficiently.

User ID Support: Automatically adds a User ID to public keys if missing, ensuring compatibility.

Installation
Clone or Download:
Download pgp_tool.py or clone the repository:
bash

git clone <repository_url>
cd <repository_directory>

Install Dependencies:
Requires Python 3.6+ and the pgpy library.

Install via pip:
bash

pip install pgpy

Make Executable (Linux/macOS, optional):
bash

chmod +x pgp_tool.py

Prerequisites
PGP Keys:
Obtain or generate a PGP public key (for encryption) and private key (for decryption).

Keys should be ASCII-armored (e.g., starting with -----BEGIN PGP PUBLIC KEY BLOCK-----).

Example key generation using pgpy:
python

from pgpy import PGPKey, PGPUID
key = PGPKey.new(pgpy.constants.PubKeyAlgorithm.RSAEncryptOrSign, 2048)
uid = PGPUID.new("Anonymous")
key.add_uid(uid)
with open("public_key.asc", "w") as f:
    f.write(str(key.pubkey))
with open("private_key.asc", "w") as f:
    f.write(str(key))

Ensure the private key is protected with a strong passphrase (e.g., 28 characters like SunsetM0untain$2025SecureKey123).

File Setup:
Prepare input files (e.g., BOP PD & OS.csv) and ensure output directories exist.

For decryption, input files must have a .pgp extension.

Environment (Optional for Cloud):
For cloud deployments, ensure Python and pgpy are installed.

Optionally, integrate with secret managers (e.g., Azure Key Vault, AWS Secrets Manager) by modifying the script.

Usage
The script is run from the command line with the following syntax:
bash

./pgp_tool.py <action> --input-file <input> --output-dir <output> [options]

Actions
encrypt: Encrypt a file using a public key.

decrypt: Decrypt a .pgp file using a private key and passphrase.

Options
--input-file: Path to the input file (required).

--output-dir: Directory for output files (required).

--public-key: Path to the public key file (required for encryption).

--private-key: Path to the private key file (required for decryption).

--passphrase: Passphrase for the private key (required for protected keys).

--delete-encrypted: Delete the encrypted .pgp file after decryption (optional).

--verbose: Enable debug logging for detailed output (optional).

Examples
Encrypt a CSV File:
bash

./pgp_tool.py encrypt \
    --input-file BOP PD & OS.csv \
    --output-dir encrypted \
    --public-key public_key.asc \
    --verbose

Output: encrypted/BOP PD & OS.csv.pgp

Logs:

2025-06-04 10:40:00 - INFO - Encrypted BOP PD & OS.csv to encrypted/BOP PD & OS.csv.pgp

Decrypt a .pgp File:
bash

./pgp_tool.py decrypt \
    --input-file encrypted/BOP PD & OS.csv.pgp \
    --output-dir decrypted \
    --private-key private_key.asc \
    --passphrase "SunsetM0untain$2025SecureKey123" \
    --delete-encrypted \
    --verbose

Output: decrypted/BOP PD & OS.csv

Deletes: encrypted/BOP PD & OS.csv.pgp

Logs:

2025-06-04 10:40:05 - DEBUG - Passphrase length: 28
2025-06-04 10:40:05 - DEBUG - Passphrase start: Sunse...
2025-06-04 10:40:05 - DEBUG - Private key protected: True
2025-06-04 10:40:05 - DEBUG - Private key fingerprint: 1234567890ABCDEF
2025-06-04 10:40:05 - INFO - Decrypted encrypted/BOP PD & OS.csv.pgp to decrypted/BOP PD & OS.csv
2025-06-04 10:40:05 - INFO - Decrypted content preview: col1,col2\nval1,val2...
2025-06-04 10:40:05 - INFO - Deleted encrypted file: encrypted/BOP PD & OS.csv.pgp

Full Workflow:
bash

# Create directories
mkdir -p encrypted decrypted

# Encrypt
./pgp_tool.py encrypt --input-file data.csv --output-dir encrypted --public-key public_key.asc

# Decrypt
./pgp_tool.py decrypt --input-file encrypted/data.csv.pgp --output-dir decrypted --private-key private_key.asc --passphrase "your_passphrase" --delete-encrypted

Troubleshooting
File Not Found:
Ensure input-file and output-dir exist. Use absolute paths if relative paths fail.

Example: realpath BOP PD & OS.csv

Invalid Key:
Verify key files are ASCII-armored (e.g., start with -----BEGIN PGP PUBLIC KEY BLOCK-----).

Check file permissions and contents using --verbose.

Passphrase Error:
Confirm the passphrase matches the private key.

Use quotes for passphrases with spaces or special characters.

Debug with --verbose to inspect passphrase length and start.

Unexpected Content (e.g., "Test content"):
Ensure the correct .pgp file is processed.

Verify the input file was encrypted with the corresponding public key.

PGPError:
The script loads keys fresh to avoid state issues like is_unlocked == False.

If errors persist, validate key integrity with pgpy or regenerate keys.

Cloud Deployment:
For cloud environments, install Python and pgpy on the instance.

Store keys securely (e.g., in AWS Secrets Manager) and modify the script to fetch them:
python

import boto3
client = boto3.client("secretsmanager")
passphrase = client.get_secret_value(SecretId="pgp-passphrase")["SecretString"]

Best Practices
Secure Passphrases:
Avoid hardcoding passphrases in scripts.

Use environment variables or secret managers:
bash

export PGP_PASSPHRASE="your_passphrase"
./pgp_tool.py decrypt ... --passphrase "$PGP_PASSPHRASE"

Key Management:
Store keys in secure locations (e.g., encrypted storage, secret managers).

Use strong keys (e.g., RSA 2048-bit) with User IDs for compatibility.

File Cleanup:
Use --delete-encrypted to remove .pgp files after decryption to save storage.

Delete temporary decrypted files if not needed.

Validation:
Test with small files before processing large datasets.

Preview decrypted content using logs to verify correctness.

Error Handling:
Check logs for detailed error messages.

Use --verbose for debugging key issues or content mismatches.

Notes
Context: Designed for .pgp files like CSVs, with support for keys with User IDs and strong passphrases.

No GPG: Relies solely on pgpy, avoiding GPG dependencies.

Portability: No Fabric or cloud-specific dependencies, suitable for any Python environment.

License: MIT License (or specify your preferred license).

Contributing
Contributions are welcome! Please submit issues or pull requests to the repository. For feature requests or bug reports, include:
Command-line output and logs (--verbose).

Key file snippets (first few lines, anonymized).

Input file details and environment (e.g., local, cloud).

License
MIT License
Saving the README
To save this as README.md, include it in your project directory alongside pgp_tool.py. You can create it manually or use a Python script:
python

with open("README.md", "w") as f:
    f.write("""
# PGP Tool
...
""")  # Paste the above Markdown content

Notes
Alignment with Requirements: The README supports your use case (.pgp files, command-line, non-Fabric) and addresses prior issues (path errors, PGPError, test content).

User-Friendly: Clear instructions and examples make it accessible to others.

If Refinements Needed: Let me know if you want to add specific sections (e.g., repository link, version history) or adjust the tone.

