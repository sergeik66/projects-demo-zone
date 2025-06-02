# Databricks notebook source
import fsspec
import os
from spark_engine.common.pgp_6_2 import PGP
from spark_engine.common.email_util import get_secret_notebookutils
import logging
import pgpy

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

key_vault_name = "my-key-vault"
public_key_secret = "mock-public-key-secret"
private_key_secret = "mock-private-key-secret"
passphrase_secret = "mock-passphrase-secret"
base_path = "abfss://ab08da5e-0f71-423b-a811-bd0af21f182b@onelake.dfs.fabric.microsoft.com/f1da4f29-81af-4030-9190-889e5fa725bf/Files/user_data/pdos_webreport"
decrypted_folder = f"{base_path}/decrypted"
encrypted_folder = f"{base_path}/encrypted"

def validate_pgp_key(key_str: str, key_type: str) -> None:
    if not key_str:
        raise ValueError(f"Empty {key_type} key retrieved from Key Vault")
    if not key_str.startswith(f"-----BEGIN PGP {key_type.upper()} KEY BLOCK-----"):
        raise ValueError(f"Invalid {key_type} key format: Missing ASCII-armored header")
    try:
        pgpy.PGPKey.from_blob(key_str)
    except Exception as e:
        raise ValueError(f"Invalid {key_type} key format: {str(e)}")

def validate_file(fs, file_path: str) -> None:
    if not file_path.startswith("abfss://"):
        raise ValueError(f"File path must start with 'abfss://': {file_path}")
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
    try:
        pgpy.PGPMessage.from_blob(content[:1024])
    except Exception as e:
        raise ValueError(f"Invalid PGP message in {file_path}: {str(e)}")

def encrypt_files(input_folder: str, output_folder: str, pgp_enabled: bool) -> None:
    if not pgp_enabled:
        logger.info("PGP encryption disabled. Skipping encryption.")
        return
    
    logger.info(f"Starting encryption from {input_folder} to {output_folder}")
    
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

    if not fs.exists(input_folder):
        raise FileNotFoundError(f"Input folder not found: {input_folder}")

    try:
        files = [f for f in fs.ls(input_folder, detail=False) if not f.lower().endswith(".pgp") and f.lower().endswith(".csv")]
        if not files:
            raise ValueError(f"No CSV files to encrypt in {input_folder}")
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        raise IOError(f"Error listing files in input folder: {str(e)}")

    try:
        public_key = get_secret_notebookutils(public_key_secret, key_vault_name)
        private_key = get_secret_notebookutils(private_key_secret, key_vault_name)
        validate_pgp_key(public_key, "public")
        validate_pgp_key(private_key, "private")
    except Exception as e:
        logger.error(f"Key validation failed: {str(e)}")
        raise ValueError(f"Failed to validate PGP keys: {str(e)}")

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

    for input_file in files:
        try:
            validate_file(fs, input_file)
            logger.debug(f"Validated file: {input_file}")
            encrypted_file = os.path.join(output_folder, os.path.basename(input_file) + ".pgp")
            if fs.exists(encrypted_file):
                logger.warning(f"Encrypted file {encrypted_file} already exists, deleting")
                fs.rm(encrypted_file)
            pgp.encrypt_file(input_file, output_folder)
            logger.info(f"Encrypted {input_file} successfully")
        except Exception as e:
            logger.error(f"Failed to encrypt {input_file}: {str(e)}")
            raise IOError(f"Failed to encrypt {input_file}: {str(e)}")

def decrypt_files(input_folder: str, output_folder: str, pgp_enabled: bool) -> None:
    if not pgp_enabled:
        logger.info("PGP decryption disabled. Skipping decryption.")
        return
    
    logger.info(f"Starting decryption from {input_folder} to {output_folder}")
    
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

    if not fs.exists(input_folder):
        raise FileNotFoundError(f"Input folder not found: {input_folder}")

    try:
        files = [f for f in fs.ls(input_folder, detail=False) if f.lower().endswith(".pgp")]
        if not files:
            raise ValueError(f"No .pgp files found in {input_folder}")
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        raise IOError(f"Error listing files in input folder: {str(e)}")

    try:
        public_key = get_secret_notebookutils(public_key_secret, key_vault_name)
        private_key = get_secret_notebookutils(private_key_secret, key_vault_name)
        validate_pgp_key(public_key, "public")
        validate_pgp_key(private_key, "private")
    except Exception as e:
        logger.error(f"Key validation failed: {str(e)}")
        raise ValueError(f"Failed to validate PGP keys: {str(e)}")

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

    for input_file in files:
        try:
            validate_file(fs, input_file)
            with fs.open(input_file, "rb") as rb_file:
                content = rb_file.read()
            validate_pgp_message(input_file, content)
            logger.debug(f"Validated PGP message: {input_file}")
            decrypted_file = os.path.join(output_folder, os.path.basename(input_file).removesuffix(".pgp"))
            if fs.exists(decrypted_file):
                logger.warning(f"Decrypted file {decrypted_file} already exists, deleting")
                fs.rm(decrypted_file)
            pgp.decrypt_file(input_file, output_folder)
            logger.info(f"Decrypted {input_file} successfully")
            with fs.open(decrypted_file, "r") as f:
                content = f.read()
                logger.info(f"Decrypted content preview: {content[:100]}...")
        except Exception as e:
            logger.error(f"Failed to decrypt {input_file}: {str(e)}")
            raise IOError(f"Failed to decrypt {input_file}: {str(e)}")

pgp_enabled = True

try:
    encrypt_files(
        input_folder=decrypted_folder,
        output_folder=encrypted_folder,
        pgp_enabled=pgp_enabled
    )
    decrypt_files(
        input_folder=encrypted_folder,
        output_folder=decrypted_folder,
        pgp_enabled=pgp_enabled
    )
except Exception as e:
    logger.error(f"Error during processing: {str(e)}")
    raise
