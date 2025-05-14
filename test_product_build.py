import json
import os
import time
from datetime import datetime
from uuid import uuid4
from spark_engine.sparkengine import SparkEngine
from spark_engine.common.lakehouse import LakehouseManager
import notebookutils as nu
from concurrent.futures import ThreadPoolExecutor, as_completed

# Global variables
feed_name = 'claims_bop_demo'
run_id = 'f4b58b89-aaa7-45d9-b301-82fe25c28de9'
elt_id = '016d62c1-d885-4936-8d29-80b09868f589'
product_name = 'BOP'
source_system = 'Curated'
invocation_id = 'f4b58b89-aaa7-45d9-b301-82fe25c28de9'
elt_start_date_time = "12/12/2024 13:19:26"
processing_start_time = elt_start_date_time
zone_name = "Product"
stage_name = "Transformation"

def get_current_timestamp():
    return datetime.utcnow()

def get_file_location_url(lakehouse_name, file_relative_path):
    lakehouse_manager = LakehouseManager(lakehouse_name=lakehouse_name)
    lakehouse_files_path = f"{lakehouse_manager.lakehouse_path}/Files"
    return f"{lakehouse_files_path}/{file_relative_path}"

def send_message_to_logs(message_metadata, log_file_name, file_name):
    message = {
        "product_name": product_name,
        "feed_name": feed_name,
        "dataset_name": file_name,
        "source_system": source_system,
        "metadata": message_metadata,
        "zone": zone_name,
        "stage": stage_name,
        "orchestration_tool": "spark",
        "zone_start_date_time": str(processing_start_time),
        "zone_end_date_time": str(get_current_timestamp()),
        "elt_id": elt_id,
        "run_id": run_id,
        "invocation_id": invocation_id
    }
    output_message = json.dumps(message)
    try:
        nu.fs.put(log_file_name, output_message, True)
    except Exception as error:
        raise error

def process_data(product_config_path, product_name, feed_name, file_name, elt_id, run_id, processing_start_time, log_file_name):
    max_retries = 3
    retry_delay = 60
    for attempt in range(max_retries):
        try:
            data = (
                SparkEngine.transform(product_config_path)
                .configure_transform(
                    product_name=product_name,
                    feed_name=feed_name,
                    dataset_name=file_name
                )
                .start_transform(elt_id=elt_id, run_id=run_id)
                .metrics()
            )
            return data
        except Exception as error:
            is_429 = False
            error_message = str(error).lower()
            if "429" in error_message or "too many requests" in error_message:
                is_429 = True
            elif hasattr(error, 'status_code') and error.status_code == 429:
                is_429 = True
            elif hasattr(error, 'response') and hasattr(error.response, 'status_code') and error.response.status_code == 429:
                is_429 = True
            if is_429 and attempt < max_retries - 1:
                print(f"Received 429 for {file_name}, attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
            else:
                data = {
                    "ingestion": {
                        "error_message": str(error),
                        "startTime": str(processing_start_time)
                    }
                }
                print(f"Exception occurred while processing the data for {file_name}: {error}")
                raise error
        finally:
            message_metadata = {"runOutput": data if 'data' in locals() else {}}
            send_message_to_logs(message_metadata, log_file_name, file_name)

def get_spark_max_workers():
    try:
        spark_cores = os.environ.get("SPARK_EXECUTOR_CORES")
        if spark_cores:
            total_cores = int(spark_cores) * int(os.environ.get("SPARK_EXECUTOR_INSTANCES", 1))
            max_workers = max(1, total_cores - 1)
            print(f"Detected {total_cores} Spark cores from environment, setting max_workers to {max_workers}")
            return max_workers
        import multiprocessing
        cpu_count = multiprocessing.cpu_count()
        max_workers = max(1, cpu_count - 1)
        print(f"Using CPU count {cpu_count}, setting max_workers to {max_workers}")
        return max_workers
    except Exception as e:
        print(f"Could not determine Spark cores, defaulting to max_workers=3: {e}")
        return 3

def process_file(file_info, missing_params):
    file_name = file_info["fileName"]
    model_config_folder_name = file_info["modelConfigFolderName"]
    config_file_relative_path = f"{model_config_folder_name}/{file_name}.yaml"
    log_file_relative_path = f"Metadata_Logs/{uuid4()}.json"
    if missing_params:
        log_file_name = get_file_location_url("den_lhw_pdi_001_observability", log_file_relative_path)
        product_config_path = get_file_location_url("den_lhw_pdi_001_metadata", config_file_relative_path)
        print(f"Constructing abfss path with LakehouseManager class for {file_name}")
    else:
        log_file_name = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_observability_id}/Files/{log_file_relative_path}"
        product_config_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_metadata_id}/Files/{config_file_relative_path}"
        print(f"Constructing abfss path with additional parameters for {file_name}: workspace_id={workspace_id}, lh_observability_id={lh_observability_id}, lh_metadata_id={lh_metadata_id}")
    print(f"Processing file: {file_name}")
    return process_data(
        product_config_path=product_config_path,
        product_name=product_name,
        feed_name=feed_name,
        file_name=file_name,
        elt_id=elt_id,
        run_id=run_id,
        processing_start_time=processing_start_time,
        log_file_name=log_file_name
    )

%pip install pytest pytest-mock
import pytest
print(pytest.__version__)

temp_file_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_observability_id}/Files/test_{uuid4()}.py"
nu.fs.put(temp_file_path, test_code, True)
result = pytest.main([temp_file_path, "-v"])
nu.fs.rm(temp_file_path)


# Install pytest and pytest-mock if not already installed
%pip install pytest pytest-mock

# Import required libraries for testing
import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime
import os
import time
import tempfile
import notebookutils as nu

# Sample JSON payload for testing
SAMPLE_PAYLOAD = {
    "loadGroupA": [
        {
            "files": [
                {"fileName": "dim_catastrophe", "modelConfigFolderName": "demo_product"},
                {"fileName": "dim_date", "modelConfigFolderName": "demo_product"},
            ]
        }
    ],
    "loadGroupB": [{"files": []}],
}

# Define mock_globals dictionary explicitly
MOCK_GLOBALS = {
    "feed_name": "claims_bop_demo",
    "run_id": "f4b58b89-aaa7-45d9-b301-82fe25c28de9",
    "elt_id": "016d62c1-d885-4936-8d29-80b09868f589",
    "product_name": "BOP",
    "source_system": "Curated",
    "invocation_id": "f4b58b89-aaa7-45d9-b301-82fe25c28de9",
    "elt_start_date_time": "12/12/2024 13:19:26",
    "processing_start_time": "12/12/2024 13:19:26",
    "zone_name": "Product",
    "stage_name": "Transformation",
}

# Mock global variables fixture
@pytest.fixture
def mock_globals():
    return MOCK_GLOBALS

# Mock LakehouseManager
@pytest.fixture
def mock_lakehouse_manager():
    manager = Mock()
    manager.lakehouse_path = "abfss://test@lakehouse"
    return manager

# Mock SparkEngine
@pytest.fixture
def mock_spark_engine():
    spark_engine = Mock()
    transform = Mock()
    transform.configure_transform.return_value = transform
    transform.start_transform.return_value = transform
    transform.metrics.return_value = {"status": "success"}
    spark_engine.transform.return_value = transform
    return spark_engine

# Test functions
def test_get_current_timestamp():
    """Test get_current_timestamp returns a datetime object."""
    result = get_current_timestamp()
    assert isinstance(result, datetime)

def test_get_file_location_url(mock_lakehouse_manager):
    """Test get_file_location_url constructs correct path."""
    with patch("builtins.LakehouseManager", return_value=mock_lakehouse_manager):
        result = get_file_location_url("test_lakehouse", "test_path/file.txt")
        assert result == "abfss://test@lakehouse/Files/test_path/file.txt"

def test_send_message_to_logs(mocker, mock_globals):
    """Test send_message_to_logs formats and writes log message."""
    mocker.patch.dict(globals(), mock_globals)
    mock_nu = mocker.patch("builtins.nu")
    message_metadata = {"runOutput": {"status": "success"}}
    log_file_name = "logs/test.json"
    file_name = "test_file"

    send_message_to_logs(message_metadata, log_file_name, file_name)

    mock_nu.fs.put.assert_called_once()
    call_args = mock_nu.fs.put.call_args[0]
    assert call_args[0] == log_file_name
    assert call_args[2] is True
    log_message = json.loads(call_args[1])
    assert log_message["product_name"] == mock_globals["product_name"]
    assert log_message["dataset_name"] == file_name

def test_process_data_success(mocker, mock_spark_engine, mock_globals):
    """Test process_data handles successful transformation."""
    mocker.patch.dict(globals(), mock_globals)
    mocker.patch("builtins.SparkEngine", mock_spark_engine)
    mocker.patch("builtins.send_message_to_logs")
    mocker.patch("builtins.time.sleep")

    result = process_data(
        product_config_path="config.yaml",
        product_name="BOP",
        feed_name="claims_bop_demo",
        file_name="dim_catastrophe",
        elt_id="016d62c1-d885-4936-8d29-80b09868f589",
        run_id="f4b58b89-aaa7-45d9-b301-82fe25c28de9",
        processing_start_time="12/12/2024 13:19:26",
        log_file_name="logs/test.json",
    )

    assert result == {"status": "success"}
    mock_spark_engine.transform.assert_called_once_with("config.yaml")

def test_process_data_429_retry(mocker, mock_spark_engine, mock_globals):
    """Test process_data retries on 429 error."""
    mocker.patch.dict(globals(), mock_globals)
    mocker.patch("builtins.SparkEngine", mock_spark_engine)
    mocker.patch("builtins.send_message_to_logs")
    mock_sleep = mocker.patch("builtins.time.sleep")

    error_429 = Exception("HTTP 429 Too Many Requests")
    error_429.status_code = 429
    mock_spark_engine.transform.side_effect = [error_429, Mock(metrics=lambda: {"status": "success"})]

    result = process_data(
        product_config_path="config.yaml",
        product_name="BOP",
        feed_name="claims_bop_demo",
        file_name="dim_catastrophe",
        elt_id="016d62c1-d885-4936-8d29-80b09868f589",
        run_id="f4b58b89-aaa7-45d9-b301-82fe25c28de9",
        processing_start_time="12/12/2024 13:19:26",
        log_file_name="logs/test.json",
    )

    assert result == {"status": "success"}
    assert mock_sleep.call_count == 1

def test_get_spark_max_workers_env_vars(mocker):
    """Test get_spark_max_workers with environment variables."""
    mocker.patch.dict(os.environ, {
        "SPARK_EXECUTOR_CORES": "2",
        "SPARK_EXECUTOR_INSTANCES": "4"
    })
    result = get_spark_max_workers()
    assert result == 7

def test_process_file(mocker, mock_spark_engine, mock_lakehouse_manager, mock_globals):
    """Test process_file constructs paths and calls process_data."""
    mocker.patch.dict(globals(), mock_globals)
    mocker.patch("builtins.SparkEngine", mock_spark_engine)
    mocker.patch("builtins.LakehouseManager", return_value=mock_lakehouse_manager)
    mock_process_data = mocker.patch("builtins.process_data")

    file_info = {"fileName": "dim_catastrophe", "modelConfigFolderName": "demo_product"}
    missing_params = ["workspace_id"]

    process_file(file_info, missing_params)

    mock_process_data.assert_called_once()
    call_kwargs = mock_process_data.call_args[1]
    assert call_kwargs["product_config_path"] == "abfss://test@lakehouse/Files/demo_product/dim_catastrophe.yaml"
    assert call_kwargs["file_name"] == "dim_catastrophe"

# Save tests to a temporary file and run pytest
test_code = f"""
import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime
import os
import time

# Sample payload
SAMPLE_PAYLOAD = {json.dumps(SAMPLE_PAYLOAD)}

# Mock globals
@pytest.fixture
def mock_globals():
    return {json.dumps(MOCK_GLOBALS)}

@pytest.fixture
def mock_lakehouse_manager():
    manager = Mock()
    manager.lakehouse_path = "abfss://test@lakehouse"
    return manager

@pytest.fixture
def mock_spark_engine():
    spark_engine = Mock()
    transform = Mock()
    transform.configure_transform.return_value = transform
    transform.start_transform.return_value = transform
    transform.metrics.return_value = {{"status": "success"}}
    spark_engine.transform.return_value = transform
    return spark_engine

# Test functions
def test_get_current_timestamp():
    result = get_current_timestamp()
    assert isinstance(result, datetime)

def test_get_file_location_url(mock_lakehouse_manager):
    with patch("builtins.LakehouseManager", return_value=mock_lakehouse_manager):
        result = get_file_location_url("test_lakehouse", "test_path/file.txt")
        assert result == "abfss://test@lakehouse/Files/test_path/file.txt"

def test_send_message_to_logs(mocker, mock_globals):
    mocker.patch.dict(globals(), mock_globals)
    mock_nu = mocker.patch("builtins.nu")
    message_metadata = {{"runOutput": {{"status": "success"}}}}
    log_file_name = "logs/test.json"
    file_name = "test_file"
    send_message_to_logs(message_metadata, log_file_name, file_name)
    mock_nu.fs.put.assert_called_once()
    call_args = mock_nu.fs.put.call_args[0]
    assert call_args[0] == log_file_name
    assert call_args[2] is True
    log_message = json.loads(call_args[1])
    assert log_message["product_name"] == mock_globals["product_name"]
    assert log_message["dataset_name"] == file_name

def test_process_data_success(mocker, mock_spark_engine, mock_globals):
    mocker.patch.dict(globals(), mock_globals)
    mocker.patch("builtins.SparkEngine", mock_spark_engine)
    mocker.patch("builtins.send_message_to_logs")
    mocker.patch("builtins.time.sleep")
    result = process_data(
        product_config_path="config.yaml",
        product_name="BOP",
        feed_name="claims_bop_demo",
        file_name="dim_catastrophe",
        elt_id="016d62c1-d885-4936-8d29-80b09868f589",
        run_id="f4b58b89-aaa7-45d9-b301-82fe25c28de9",
        processing_start_time="12/12/2024 13:19:26",
        log_file_name="logs/test.json",
    )
    assert result == {{"status": "success"}}
    mock_spark_engine.transform.assert_called_once_with("config.yaml")

def test_process_data_429_retry(mocker, mock_spark_engine, mock_globals):
    mocker.patch.dict(globals(), mock_globals)
    mocker.patch("builtins.SparkEngine", mock_spark_engine)
    mocker.patch("builtins.send_message_to_logs")
    mock_sleep = mocker.patch("builtins.time.sleep")
    error_429 = Exception("HTTP 429 Too Many Requests")
    error_429.status_code = 429
    mock_spark_engine.transform.side_effect = [error_429, Mock(metrics=lambda: {{"status": "success"}})]
    result = process_data(
        product_config_path="config.yaml",
        product_name="BOP",
        feed_name="claims_bop_demo",
        file_name="dim_catastrophe",
        elt_id="016d62c1-d885-4936-8d29-80b09868f589",
        run_id="f4b58b89-aaa7-45d9-b301-82fe25c28de9",
        processing_start_time="12/12/2024 13:19:26",
        log_file_name="logs/test.json",
    )
    assert result == {{"status": "success"}}
    assert mock_sleep.call_count == 1

def test_get_spark_max_workers_env_vars(mocker):
    mocker.patch.dict(os.environ, {{"SPARK_EXECUTOR_CORES": "2", "SPARK_EXECUTOR_INSTANCES": "4"}})
    result = get_spark_max_workers()
    assert result == 7

def test_process_file(mocker, mock_spark_engine, mock_lakehouse_manager, mock_globals):
    mocker.patch.dict(globals(), mock_globals)
    mocker.patch("builtins.SparkEngine", mock_spark_engine)
    mocker.patch("builtins.Lake
    nu.fs.rm(temp_file_path)

print(f"Pytest completed with exit code: {result}")
