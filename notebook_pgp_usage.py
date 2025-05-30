# Databricks notebook source
# Import required libraries
import fsspec
import os
from spark_engine.common.pgp import PGP
from spark_engine.common.email_util import get_secret_notebookutils
import logging
import pgpy

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
key_vault_name = "my-key-vault"
public_key_secret = "mock-public-key-secret"
private_key_secret = "mock-private-key-secret"
passphrase_secret = "mock-passphrase-secret"
base_path = "abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/7c6d771a-3b6f-4042-8a89-1a885973a93c/Files/templates/emails"
decrypted_folder = f"{base_path}/decrypted"
encrypted_folder = f"{base_path}/encrypted"

def validate_pgp_key(key_str: str, key_type: str) -> None:
    """
    Validate that a key string is in ASCII-armored PGP format.
    
    Args:
        key_str: Key string to validate.
        key_type: Type of key (e.g., 'public', 'private').
    
    Raises:
        ValueError: If the key is not in valid ASCII-armored format.
    """
    if not key_str:
        raise ValueError(f"Empty {key_type} key retrieved from Key Vault")
    if not key_str.startswith(f"-----BEGIN PGP {key_type.upper()} KEY BLOCK-----"):
        raise ValueError(f"Invalid {key_type} key format: Missing ASCII-armored header")
    try:
        pgpy.PGPKey.from_blob(key_str)
    except Exception as e:
        raise ValueError(f"Invalid {key_type} key format: {str(e)}")

def validate_file(fs, file_path: str) -> None:
    """
    Validate that a file exists and has a valid size.
    
    Args:
        fs: fsspec filesystem object.
        file_path: Path to the file.
    
    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty or size is None.
    """
    if not fs.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    try:
        file_info = fs.info(file_path)
        if file_info.get("size") is None:
            raise ValueError(f"File {file_path} has unknown size")
        if file_info["size"] == 0:
            raise ValueError(f"File {file_path} is empty")
    except Exception as e:
        raise ValueError(f"Failed to retrieve file info for {file_path}: {str(e)}")

def validate_pgp_message(file_path: str, content: bytes) -> None:
    """
    Validate that a file contains a valid PGP message.
    
    Args:
        file_path: Path to the file.
        content: File content as bytes.
    
    Raises:
        ValueError: If the content is not a valid PGP message.
    """
    try:
        # Attempt to parse a small portion to check format
        pgpy.PGPMessage.from_blob(content[:1024])
    except Exception as e:
        raise ValueError(f"Invalid PGP message in {file_path}: {str(e)}")

# Encryption function
def encrypt_files(input_folder: str, output_folder: str, pgp_enabled: bool) -> None:
    """
    Encrypt all files in the input folder and save to the output folder.
    
    Args:
        input_folder: Path to folder with files to encrypt (decrypted folder).
        output_folder: Path to save encrypted files (encrypted folder).
        pgp_enabled: Flag to enable/disable PGP encryption.
    
    Raises:
        ValueError: If required parameters are missing or no files are found.
        FileNotFoundError: If input folder does not exist.
        IOError: If encryption or file operations fail.
    """
    if not pgp_enabled:
        logger.info("PGP encryption disabled. Skipping encryption.")
        return
    
    logger.info(f"Starting encryption from {input_folder} to {output_folder}")
    
    # Initialize fsspec filesystem with credentials
    try:
        fs = fsspec.filesystem(
            "abfss",
            account_name="onelake",
            account_host="onelake.dfs.fabric.microsoft.com",
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            tenant_id=os.getenv("AZURE_TENANT_ID")
        )
    except Exception as e:
        logger.error(f"Error initializing fsspec filesystem: {str(e)}")
        raise IOError(f"Error initializing fsspec filesystem: {str(e)}")

    # Check if input folder exists
    if not fs.exists(input_folder):
        raise FileNotFoundError(f"Input folder not found: {input_folder}")

    # List files in input folder (exclude .pgp files)
    try:
        files = [f for f in fs.ls(input_folder, detail=False) if not f.lower().endswith((".pgp", ".gpg"))]
        if not files:
            raise ValueError(f"No files to encrypt in {input_folder}")
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        raise IOError(f"Error listing files in input folder: {str(e)}")

    # Retrieve and validate keys
    try:
        public_key = get_secret_notebookutils(public_key_secret, key_vault_name)
        private_key = get_secret_notebookutils(private_key_secret, key_vault_name)
        validate_pgp_key(public_key, "public")
        validate_pgp_key(private_key, "private")
    except Exception as e:
        logger.error(f"Key validation failed: {str(e)}")
        raise ValueError(f"Failed to validate PGP keys: {str(e)}")

    # Initialize PGP class
    try:
        pgp = PGP(
            key_vault_name=key_vault_name,
            public_key_secret=public_key_secret,
            private_key_secret=private_key_secret,
            passphrase_secret=passphrase_secret
        )
    except Exception as e:
        logger.error(f"Error initializing PGP class: {str(e)}")
        raise ValueError(f"Error initializing PGP class: {str(e)}")

    # Encrypt each file
    for input_file in files:
        try:
            validate_file(fs, input_file)
            logger.debug(f"Validated file: {input_file}")
            pgp.encrypt_file(input_file, output_folder)
            logger.info(f"Encrypted {input_file} successfully")
        except Exception as e:
            logger.error(f"Failed to encrypt {input_file}: {str(e)}")
            raise IOError(f"Failed to encrypt {input_file}: {str(e)}")

# Decryption function
def decrypt_files(input_folder: str, output_folder: str, pgp_enabled: bool) -> None:
    """
    Decrypt all .pgp or .gpg files in the input folder and save to the output folder.
    
    Args:
        input_folder: Path to folder with encrypted files (encrypted folder).
        output_folder: Path to save decrypted files (decrypted folder).
        pgp_enabled: Flag to enable/disable PGP decryption.
    
    Raises:
        ValueError: If required parameters are missing or no .pgp/.gpg files are found.
        FileNotFoundError: If input folder does not exist.
        IOError: If decryption or file operations fail, with warnings for unsupported files.
    """
    if not pgp_enabled:
        logger.info("PGP decryption disabled. Skipping decryption.")
        return
    
    logger.info(f"Starting decryption from {input_folder} to {output_folder}")
    
    # Initialize fsspec filesystem with credentials
    try:
        fs = fsspec.filesystem(
            "abfss",
            account_name="onelake",
            account_host="onelake.dfs.fabric.microsoft.com",
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            tenant_id=os.getenv("AZURE_TENANT_ID")
        )
    except Exception as e:
        logger.error(f"Error initializing fsspec filesystem: {str(e)}")
        raise IOError(f"Error initializing fsspec filesystem: {str(e)}")

    # Check if input folder exists
    if not fs.exists(input_folder):
        raise FileNotFoundError(f"Input folder not found: {input_folder}")

    # List .pgp and .gpg files
    try:
        files = [f for f in fs.ls(input_folder, detail=False) if f.lower().endswith((".pgp", ".gpg"))]
        if not files:
            raise ValueError(f"No .pgp or .gpg files found in {input_folder}")
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        raise IOError(f"Error listing files in input folder: {str(e)}")

    # Retrieve and validate keys
    try:
        public_key = get_secret_notebookutils(public_key_secret, key_vault_name)
        private_key = get_secret_notebookutils(private_key_secret, key_vault_name)
        validate_pgp_key(public_key, "public")
        validate_pgp_key(private_key, "private")
    except Exception as e:
        logger.error(f"Key validation failed: {str(e)}")
        raise ValueError(f"Failed to validate PGP keys: {str(e)}")

    # Initialize PGP class
    try:
        pgp = PGP(
            key_vault_name=key_vault_name,
            public_key_secret=public_key_secret,
            private_key_secret=private_key_secret,
            passphrase_secret=passphrase_secret
        )
    except Exception as e:
        logger.error(f"Error initializing PGP class: {str(e)}")
        raise ValueError(f"Error initializing PGP class: {str(e)}")

    # Decrypt each file
    for input_file in files:
        try:
            validate_file(fs, input_file)
            # Read and validate PGP message
            with fs.open(input_file, "rb") as rb_file:
                content = rb_file.read()
            validate_pgp_message(input_file, content)
            logger.debug(f"Validated PGP message: {input_file}")
            pgp.decrypt_file(input_file, output_folder)
            logger.info(f"Decrypted {input_file} successfully")
        except NotImplementedError as e:
            logger.warning(f"Skipping {input_file}: Unsupported PGP packet type ({str(e)})")
            continue
        except Exception as e:
            logger.error(f"Failed to decrypt {input_file}: {str(e)}")
            raise IOError(f"Failed to decrypt {input_file}: {str(e)}")

# Execute encryption and decryption
pgp_enabled = True

try:
    # Encrypt files from decrypted to encrypted folder
    encrypt_files(
        input_folder=decrypted_folder,
        output_folder=encrypted_folder,
        pgp_enabled=pgp_enabled
    )
    
    # Decrypt files from encrypted to decrypted folder
    decrypt_files(
        input_folder=encrypted_folder,
        output_folder=decrypted_folder,
        pgp_enabled=pgp_enabled
    )
except Exception as e:
    logger.error(f"Error during processing: {str(e)}")
    raise
