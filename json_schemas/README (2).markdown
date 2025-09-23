# Fabric Interface Python Library

## Overview
The `FabricInterface` Python library provides a programmatic way to interact with Microsoft Fabric data pipelines using its REST APIs. This library allows you to create, monitor, and manage pipeline runs within a specified Fabric workspace. It leverages Azure credentials for authentication and includes methods to handle pipeline execution and status checking.

## Features
- **Create Pipeline Runs**: Initiate a new pipeline run in a specified Fabric workspace with optional parameters.
- **Monitor Pipeline Status**: Retrieve the current state of a pipeline run.
- **Await Completion**: Wait for a pipeline run to complete, with configurable timeout and polling intervals.
- **Simplified Authentication**: Uses Azure `ClientSecretCredential` for secure access to Fabric APIs.

## Prerequisites
- Python 3.8+
- Required Python packages:
  - `requests`
  - `azure-identity`
  - `notebookutils` (for Azure Databricks environments)
- Azure credentials (Service Principal) with access to the Fabric workspace.
- Access to an Azure Key Vault for retrieving secrets (optional, depending on credential management).

## Installation
1. Clone or download the repository containing `fbric_interface.py`.
2. Install the required dependencies:
   ```bash
   pip install requests azure-identity
   ```
3. Ensure `notebookutils` is available if running in an Azure Databricks environment.

## Usage
Below is an example of how to use the `FabricInterface` class to create and monitor a pipeline run.

```python
from fbric_interface import FabricInterface

# Initialize the FabricInterface with a workspace ID
fabric = FabricInterface(workspace_id="your-workspace-id")

# Create a pipeline run with optional parameters
pipeline_item_id = "your-pipeline-item-id"
parameters = {"param1": "value1", "param2": "value2"}
run_response = fabric.create_run(pipeline_item_id, pipeline_parameters=parameters)

# Wait for the pipeline run to complete
result = fabric.await_run_completion(run_response, timeout_seconds=3600)
print(f"Pipeline run completed with status: {result.status}")
```

Alternatively, use the convenience method to create and await completion in one step:

```python
result = fabric.create_run_and_await_completion(pipeline_item_id, pipeline_parameters=parameters)
print(f"Pipeline run completed with status: {result.status}")
```

## Configuration
- **Workspace ID**: The Fabric workspace ID where the pipeline resides. If not provided, a default ID is used.
- **Azure Credentials**: The library expects Azure Service Principal credentials, which can be retrieved from an Azure Key Vault or passed directly to the `get_credentials` function.
- **Logging**: The library uses Python's `logging` module for debugging and error tracking. Configure the logger as needed for your environment.

## Key Components
- **FabricInterface Class**:
  - `__init__(workspace_id)`: Initializes the interface with a workspace ID and Azure credentials.
  - `create_run(pipeline_item_id, pipeline_parameters)`: Creates a new pipeline run.
  - `get_run_state(run_id)`: Retrieves the current state of a pipeline run.
  - `await_run_completion(run, timeout_seconds, poll_interval_seconds)`: Polls the pipeline run until completion or timeout.
  - `create_run_and_await_completion(pipeline_item_id, pipeline_parameters)`: Combines creation and monitoring in one call.
- **Data Classes**:
  - `PipelineRun`: Represents the state of a pipeline run (run ID, status, pipeline item ID, workspace ID).
  - `PipelineRunCreateResponse`: Represents the response from creating a pipeline run.
- **Helper Functions**:
  - `get_credentials`: Retrieves Azure Service Principal credentials for authentication.
  - `get_secret_notebookutils`: Utility function for fetching secrets from Azure Key Vault (Databricks-specific).

## Error Handling
- The library raises `requests.RequestException` for API-related errors.
- Timeout errors are raised if a pipeline run exceeds the specified `timeout_seconds`.
- Missing or invalid Azure credentials will raise a `ValueError`.

## Logging
The library uses a logger named `__name__` for debugging, info, and error messages. Configure the logger level and handlers in your application as needed.

## Limitations
- Requires a valid Azure Service Principal with appropriate permissions for Fabric APIs.
- The `notebookutils` dependency is specific to Azure Databricks; replace with custom secret retrieval logic for other environments.
- The library assumes the Fabric REST API endpoint (`https://api.fabric.microsoft.com/v1`) is accessible.

## License
This project is licensed under the MIT License.