# Databricks notebook source
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

import fsspec
import notebookutils as nu
from spark_engine.common.lakehouse import LakehouseManager
from spark_engine.common.pgp_6_2 import PGP
from spark_engine.sparkengine import SparkEngine

# Configuration
CONFIG = {
    "logs_lakehouse_name": "den_lhw_pdi_001_observability",
    "log_file_prefix": "Metadata_Logs",
    "key_vault_name": None,  # Set via secretsScope
    "workspace_id": None,  # Optional, for backward compatibility
    "lh_raw_id": None,  # Optional
    "lh_observability_id": None,  # Optional
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def get_current_timestamp() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.utcnow()

def get_file_location_url(lakehouse_name: str, relative_path: str) -> str:
    """
    Construct an abfss URL for a file in a lakehouse.

    Args:
        lakehouse_name: Name of the lakehouse.
        relative_path: Relative path within the lakehouse Files directory.

    Returns:
        str: Full abfss URL.
    """
    logger.debug(f"Constructing abfss path for lakehouse: {lakehouse_name}, path: {relative_path}")
    lakehouse_manager = LakehouseManager(lakehouse_name=lakehouse_name)
    base_path = f"{lakehouse_manager.lakehouse_path}/Files"
    return f"abfss://{base_path.lstrip('abfss://')}/{relative_path.lstrip('/')}"

def send_message_to_logs(
    message_metadata: Dict,
    metadata_config: Dict,
    log_file_url: str,
    processing_start_time: datetime,
    elt_id: str,
    run_id: str,
    invocation_id: str,
    product_name: str,
    feed_name: str,
) -> None:
    """
    Log metadata to a JSON file in OneLake.

    Args:
        message_metadata: Metadata to include in the log.
        metadata_config: Configuration dictionary with dataset details.
        log_file_url: abfss URL for the log file.
        processing_start_time: Start time of processing.
        elt_id: ELT identifier.
        run_id: Run identifier.
        invocation_id: Invocation identifier.
        product_name: Product name.
        feed_name: Feed name.

    Raises:
        IOError: If writing to the log file fails.
    """
    message = {
        "product_name": product_name,
        "feed_name": feed_name,
        "dataset_name": metadata_config.get("datasetName", ""),
        "source_system": metadata_config.get("sourceSystemProperties", {}).get("sourceSystemName", ""),
        "metadata": message_metadata,
        "zone": "Curated",
        "stage": "Transformation",
        "orchestration_tool": "spark",
        "zone_start_date_time": str(processing_start_time),
        "zone_end_date_time": str(get_current_timestamp()),
        "elt_id": elt_id,
        "run_id": run_id,
        "invocation_id": invocation_id,
    }

    try:
        nu.fs.put(log_file_url, json.dumps(message), overwrite=True)
        logger.info(f"Logged metadata to {log_file_url}")
    except Exception as e:
        logger.error(f"Failed to write log to {log_file_url}: {str(e)}")
        raise IOError(f"Failed to write log: {str(e)}")

def decrypt_files(
    input_folder: str,
    output_folder: str,
    key_vault_name: str,
    private_key_secret: str,
    public_key_secret: Optional[str] = None,
    passphrase_secret: Optional[str] = None,
) -> List[str]:
    """
    Decrypt .pgp files in the input folder and delete encrypted files after success.

    Args:
        input_folder: abfss path to the folder containing .pgp files.
        output_folder: abfss path for decrypted files.
        key_vault_name: Name of the key vault.
        private_key_secret: Secret name for the private key.
        public_key_secret: Secret name for the public key (optional).
        passphrase_secret: Secret name for the passphrase (optional).

    Returns:
        List[str]: List of decrypted file paths.

    Raises:
        ValueError: If required parameters are missing or paths are invalid.
        FileNotFoundError: If input folder or files are missing.
        IOError: If decryption fails.
    """
    if not all([key_vault_name, private_key_secret, passphrase_secret]):
        raise ValueError("key_vault_name, private_key_secret, and passphrase_secret are required.")
    if not input_folder.startswith("abfss://") or not output_folder.startswith("abfss://"):
        raise ValueError(f"Paths must start with 'abfss://': {input_folder}, {output_folder}")

    fs = fsspec.filesystem(
        protocol="abfss",
        account_name="onelake",
        account_host="onelake.dfs.fabric.microsoft.com"
    )

    if not fs.exists(input_folder):
        raise FileNotFoundError(f"Input folder not found: {input_folder}")

    files = [f for f in fs.ls(input_folder, detail=False) if f.lower().endswith(".csv.pgp")]
    if not files:
        raise ValueError(f"No .csv.pgp files found in {input_folder}")

    pgp = PGP(
        key_vault_name=key_vault_name,
        public_key_secret=public_key_secret,
        private_key_secret=private_key_secret,
        passphrase_secret=passphrase_secret,
    )

    decrypted_files = []
    for input_file in files:
        try:
            decrypted_file = os.path.join(output_folder, os.path.basename(input_file).removesuffix(".pgp"))
            if fs.exists(decrypted_file):
                logger.warning(f"Decrypted file {decrypted_file} exists, deleting")
                fs.delete(decrypted_file)

            pgp.decrypt_file(input_file, output_folder)
            if fs.exists(decrypted_file):
                with fs.open(decrypted_file, "r") as f:
                    content = f.read()[:100]
                    logger.info(f"Decrypted {decrypted_file}, content preview: {content[:100]}...")
                fs.delete(input_file)
                logger.info(f"Deleted encrypted file: {input_file}")
                decrypted_files.append(decrypted_file)
            else:
                logger.error(f"Decrypted file {decrypted_file} not created")
                raise IOError(f"Decryption failed for {input_file}: Output file missing")
        except Exception as e:
            logger.error(f"Failed to decrypt {input_file}: {str(e)}")
            raise IOError(f"Failed to decrypt {input_file}: {str(e)}")

    return decrypted_files

# Main execution
try:
    processing_start_time = get_current_timestamp()
    metadata_config = json.loads(dataset_config_json, strict=False)

    # Validate required parameters
    required_params = ["secretsScope", "product_name", "feed_name", "elt_id", "run_id", "invocation_id"]
    missing_params = [p for p in required_params if p not in globals() or globals()[p] is None or globals()[p] == ""]
    if missing_params:
        raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")

    CONFIG["key_vault_name"] = secretsScope
    raw_lakehouse_name = metadata_config["rawProperties"]["lakehouseName"]
    dataset_file_name = metadata_config.get("datasetName", "")
    raw_directory = metadata_config.get("rawProperties", {}).get("directory", "")

    # Construct log file path
    log_file_relative_path = f"{CONFIG['log_file_prefix']}/{uuid4()}.json"
    log_file_url = get_file_location_url(CONFIG["logs_lakehouse_name"], log_file_relative_path)

    # Construct data location
    data_location = get_file_location_url(raw_lakehouse_name, raw_directory)
    logger.info(f"Data location: {data_location}")

    # Handle PGP decryption if enabled
    pgp_enabled = metadata_config["sourceSystemProperties"].get("pgpEnabled", False)
    if pgp_enabled:
        private_key_secret = metadata_config["sourceSystemProperties"].get("privateKeySecret")
        passphrase_secret = metadata_config["sourceSystemProperties"].get("passphraseSecret")
        public_key_secret = metadata_config["sourceSystemProperties"].get("publicKeySecret")
        if not all([private_key_secret, passphrase_secret, CONFIG["key_vault_name"]]):
            raise ValueError("key_vault_name, private_key_secret, and passphrase_secret are required for PGP.")

        output_folder = get_file_location_url(raw_lakehouse_name, f"{dataset_file_name}/decrypted/{run_id}")
        decrypted_files = decrypt_files(
            input_folder=data_location,
            output_folder=output_folder,
            key_vault_name=CONFIG["key_vault_name"],
            private_key_secret=private_key_secret,
            public_key_secret=public_key_secret,
            passphrase_secret=passphrase_secret,
        )
        data_location = output_folder
        logger.info(f"Decrypted files: {decrypted_files}")

    # Ingest data
    try:
        data = (
            SparkEngine.ingest(dataset_config_json)
            .source_data(data_location)
            .perform_pre_check(dataset_config_folder_name, dataset_file_name)
            .start_ingest(elt_id, run_id)
            .metrics()
        )
    except Exception as e:
        data = {
            "ingestion": {
                "error_message": str(e),
                "startTime": str(processing_start_time),
            }
        }
        logger.error(f"Data ingestion failed: {str(e)}")
        raise

    message_metadata = {"runOutput": data}
    send_message_to_logs(
        message_metadata=message_metadata,
        metadata_config=metadata_config,
        log_file_url=log_file_url,
        processing_start_time=processing_start_time,
        elt_id=elt_id,
        run_id=run_id,
        invocation_id=invocation_id,
        product_name=product_name,
        feed_name=feed_name,
    )

    nu.notebook.exit(json.dumps(data))

except Exception as e:
    logger.error(f"Notebook execution failed: {str(e)}")
    raise
