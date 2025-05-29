# Databricks notebook source
# Import required libraries
import fsspec
import os
from spark_engine.common.pgp import PGP
from spark_engine.common.email_util import get_secret_notebookutils
import logging

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
    
    # Initialize fsspec filesystem
    try:
        fs = fsspec.filesystem("abfss", account_name="onelake", account_host="onelake.dfs.fabric.microsoft.com")
    except Exception as e:
        logger.error(f"Error initializing fsspec filesystem: {str(e)}")
        raise IOError(f"Error initializing fsspec filesystem: {str(e)}")

    # Check if input folder exists
    if not fs.exists(input_folder):
        raise FileNotFoundError(f"Input folder not found: {input_folder}")

    # List files in input folder (exclude .pgp files)
    try:
        files = [f for f in fs.ls(input_folder, detail=False) if not f.lower().endswith(".pgp")]
        if not files:
            raise ValueError(f"No files to encrypt in {input_folder}")
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        raise IOError(f"Error listing files in input folder: {str(e)}")

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
            pgp.encrypt_file(input_file, output_folder)
            logger.info(f"Encrypted {input_file} successfully")
        except Exception as e:
            logger.error(f"Failed to encrypt {input_file}: {str(e)}")
            raise IOError(f"Failed to encrypt {input_file}: {str(e)}")

# Decryption function
def decrypt_files(input_folder: str, output_folder: str, pgp_enabled: bool) -> None:
    """
    Decrypt all .pgp files in the input folder and save to the output folder.
    
    Args:
        input_folder: Path to folder with encrypted files (encrypted folder).
        output_folder: Path to save decrypted files (decrypted folder).
        pgp_enabled: Flag to enable/disable PGP decryption.
    
    Raises:
        ValueError: If required parameters are missing or no .pgp files are found.
        FileNotFoundError: If input folder does not exist.
        IOError: If decryption or file operations fail.
    """
    if not pgp_enabled:
        logger.info("PGP decryption disabled. Skipping decryption.")
        return
    
    logger.info(f"Starting decryption from {input_folder} to {output_folder}")
    
    # Initialize fsspec filesystem
    try:
        fs = fsspec.filesystem("abfss", account_name="onelake", account_host="onelake.dfs.fabric.microsoft.com")
    except Exception as e:
        logger.error(f"Error initializing fsspec filesystem: {str(e)}")
        raise IOError(f"Error initializing fsspec filesystem: {str(e)}")

    # Check if input folder exists
    if not fs.exists(input_folder):
        raise FileNotFoundError(f"Input folder not found: {input_folder}")

    # List .pgp files
    try:
        files = [f for f in fs.ls(input_folder, detail=False) if f.lower().endswith(".pgp")]
        if not files:
            raise ValueError(f"No .pgp files found in {input_folder}")
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        raise IOError(f"Error listing files in input folder: {str(e)}")

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
            pgp.decrypt_file(input_file, output_folder)
            logger.info(f"Decrypted {input_file} successfully")
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
