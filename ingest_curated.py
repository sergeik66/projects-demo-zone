# Imports
import json
from datetime import datetime
from uuid import uuid4
from spark_engine.sparkengine import SparkEngine
from spark_engine.common.lakehouse import LakehouseManager
from spark_engine.common.pgp_6_2 import PGP
import fsspec
import notebookutils as nu

def get_current_timestamp() -> object:
    return datetime.utcnow()

def get_file_location_url(lakehouse_name,file_relative_path) -> str:
    print(f"lakehouse_name: {lakehouse_name}")
    lakehouse_manager = LakehouseManager(lakehouse_name=lakehouse_name)
    lakehouse_files_path = f"{lakehouse_manager.lakehouse_path}/Files"
    
    return f"{lakehouse_files_path}/{file_relative_path}"

def send_message_to_logs(message_metadata: object, metadata_config_dict: object, log_file_name: str) -> object:
    message = {
        "product_name": product_name,
        "feed_name": feed_name,
        "dataset_name": metadata_config_dict['datasetName'],
        "source_system": metadata_config_dict['sourceSystemProperties']['sourceSystemName'],
        "metadata": message_metadata,
        "zone": "Curated",
        "stage": "Transformation",
        "orchestration_tool": "spark",
        "zone_start_date_time": str(processing_start_time),
        "zone_end_date_time": str(get_current_timestamp()),
        "elt_id": elt_id,
        "run_id": run_id,
        "invocation_id": invocation_id
    }

    output_message = json.dumps(message)

    # save message content to a log file later processesing
    try:   
        nu.fs.put(log_file_name, output_message, True)
    except Exception as error:
        raise error

def decrypt_file(
    input_folder: str,
    output_path: str,
    key_vault_name: str = None,
    private_key_secret: str = None,
    public_key_secret: str = None,
    passphrase_secret: str = None
) -> None:
    """
    Decrypt a file using PGP if requred parameters are provided.

    Args:
        input_folder: Path to the folder containing encrypted files (e.g., .pgp files).
        output_path: Directory path for the decrypted output file.
        key_vault_name: Name of the key vault for retrieving secrets (optional).
        private_key_secret: Secret name for the private key (optional).
        public_key_secret: Secret name for the public key (optional).
        passphrase: Passphrase for the private key (optional).
    
    Raises:
        ValueError: If pgp_enabled is True but key_vault_name or private_key_secret is None.
        FileNotFoundError: If the input file does not exist.
        IOError: If decryption or file operations fail.
    """
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
        private_key_secret=private_key_secret,
        passphrase_secret=passphrase_secret
    )
    
    # Decrypt each .pgp file
    for input_file in files:
        try:
            input_file = f"abfss://{input_file}"
            pgp.decrypt_file(
                input_file=input_file,
                output_path=output_path
            )
            print(f"File {input_file} decrypted successfully to {output_path}")
        except Exception as e:
            print(f"Failed to decrypt {input_file}: {str(e)}")
            raise IOError(f"Failed to decrypt {input_file}: {str(e)}")

processing_start_time = get_current_timestamp()
metadata_config_dict = json.loads(dataset_config_json, strict=False)

# get log file url
logs_lakehouse_name = "den_lhw_pdi_001_observability"
log_file_relative_path = f"Metadata_Logs/{uuid4()}.json"
raw_lakehouse_name = metadata_config_dict['rawProperties']['lakehouseName']

# for backward compatability
# check if additional parameters were passed:
requred_params = ["workspace_id","lh_raw_id","lh_observability_id"]
missing_params = []

for param in requred_params:
    if param not in locals() or eval(param) is None or eval(param) == '':
        missing_params.append(param)
# construct paths for log and yaml files
if missing_params:
    log_file_name = get_file_location_url(logs_lakehouse_name, log_file_relative_path)
    data_location = get_file_location_url(raw_lakehouse_name, raw_directory)
    print("Constracting abfss path with LakehouseManager class")
else:
    log_file_name = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_observability_id}/Files/{log_file_relative_path}"
    data_location = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_raw_id}/Files/{raw_directory}"
    print(f"Constracting abfss path with additional parameters:workspace_id={workspace_id},lh_observability_id={lh_observability_id},lh_raw_id={lh_raw_id}")              

# check if PGP encryption enabled and private key secret for decryption
pgp_enabled = metadata_config_dict["sourceSystemProperties"].get("pgpEnabled")
key_vault_name = secretsScope
if pgp_enabled:
    private_key_secret = metadata_config_dict["sourceSystemProperties"].get("privateKeySecret")
    passphrase_secret = metadata_config_dict["sourceSystemProperties"].get("passphraseSecret")
    public_key_secret = metadata_config_dict["sourceSystemProperties"].get("publicKeySecret")
    if private_key_secret is None or key_vault_name is None or passphrase_secret is None:
        raise ValueError("key_vault_name, private_key_secret and passphrase_secret must be provided when pgpEnabled is True.")

    # construct decrypted path
    output_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_raw_id}/Files/{dataset_file_name}/decrypted/{run_id}"
    decrypt_file(
        input_folder=data_location,
        output_path=output_path,
        key_vault_name=key_vault_name,
        private_key_secret=private_key_secret,
        passphrase_secret=passphrase_secret,
        public_key_secret=public_key_secret
    )
    # remove folder with encrypted file(s)
    nu.fs.rm(data_location, True)
    data_location = output_path

# instantiate Ingest class instance and call its methods 
try:
        data = (
            SparkEngine.ingest(dataset_config_json)
            .source_data(data_location)
            .perform_pre_check(dataset_config_folder_name,dataset_file_name)
            .start_ingest(elt_id,run_id)
            .metrics()
        )
except Exception as error:
    data = {
        "ingestion":{
             "error_message": str(error),
             "startTime": str(processing_start_time) 
        }
    }
    print("Exception occured while processing the data: ", error) 
    raise error
finally:
    message_metadata = {"runOutput": data}
    send_message_to_logs(message_metadata, metadata_config_dict, log_file_name)

nu.notebook.exit(json.dumps(data))
