import os
import logging
import time
import uuid
import requests
from azure.identity import ClientSecretCredential
from dataclasses import dataclass
from typing import Dict, Optional
from notebookutils import credentials
def get_secret_notebookutils(secret_name, key_vault_name):
    key_vault_uri = f"https://{key_vault_name}.vault.azure.net"
    secret = credentials.getSecret(key_vault_uri, secret_name)
    return secret

logger = logging.getLogger(__name__)
# Data classes to mimic SDK response structures
@dataclass
class PipelineRun:
    run_id: str
    status: str
    pipeline_item_id: str
    workspace_id: str

@dataclass
class PipelineRunCreateResponse:
    run_id: str
    pipeline_item_id: str
    workspace_id: str

class FabricInterface:
    """
    For interacting with Microsoft Fabric data pipelines using REST APIs.
    """
    def __init__(self, workspace_id: str = None):
        self.credentials = get_credentials()
        self.workspace_id = workspace_id or "02c3d55e-485c-419b-b587-21a51aeb261e"
        self.base_url = "https://api.fabric.microsoft.com/v1"
        logger.debug(f"Instantiated with Fabric workspace ID {self.workspace_id}")

    def create_run(self, pipeline_item_id: str, pipeline_parameters: Dict[str, str] = None) -> PipelineRunCreateResponse:
        """
        Creates a new pipeline run in the Fabric workspace using REST API.
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{pipeline_item_id}/jobs/instances?jobType=Pipeline"
        headers = self._get_headers()
        payload = {
            "executionData": {
            "parameters": pipeline_parameters or {}
            }
        }

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            
            if response.status_code == 201:
                run_id = response.json().get("id")
            elif response.status_code == 202:
                location = response.headers.get("Location")
                operation_id = response.headers.get("x-ms-operation-id")
                logger.info(f"Pipeline creation in progress. Track at: {location}")
                run_id = operation_id  # or None, depending on your tracking logic
            else:
                logger.error(f"Unexpected status code: {response.status_code}")
                run_id = None

            logger.info(f"Created pipeline run for item ID {pipeline_item_id} with run ID {run_id}")
            return PipelineRunCreateResponse(
                run_id=run_id,
                pipeline_item_id=pipeline_item_id,
                workspace_id=self.workspace_id
            )
        except requests.RequestException as e:
            logger.error(f"Failed to create pipeline run for item ID {pipeline_item_id}: {str(e)}")
            raise
    def get_run_state(self, run_id: str) -> PipelineRun:
        """
        Retrieves the state of a pipeline run using REST API.
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/jobs/instances/{run_id}"
        headers = self._get_headers()

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            status = data.get("status", "Unknown")
            pipeline_item_id = data.get("itemId", "Unknown")
            return PipelineRun(
                run_id=run_id,
                status=status,
                pipeline_item_id=pipeline_item_id,
                workspace_id=self.workspace_id
            )
        except requests.RequestException as e:
            logger.error(f"Failed to get pipeline run state for run ID {run_id}: {str(e)}")
            raise

    def await_run_completion(self, run: PipelineRunCreateResponse, timeout_seconds: int = 7200, poll_interval_seconds: int = 15) -> PipelineRun:
        """
        Waits for a pipeline run to complete or timeout.
        """
        start_time = time.time()
        while True:
            state = self.get_run_state(run.run_id)
            if state.status in ["Succeeded", "Failed", "Cancelled"]:
                logger.info(f"Pipeline run {run.run_id} completed with status '{state.status}'")
                return state
            elif (time.time() - start_time > timeout_seconds):
                logger.info(f"Timed out waiting for pipeline run {run.run_id} after {timeout_seconds} seconds. Current status: {state.status}")
                raise TimeoutError("Timed out waiting for run to complete")
            else:
                logger.debug(f"Status of run {run.run_id} is currently '{state.status}'")
                time.sleep(poll_interval_seconds)

    def create_run_and_await_completion(self, pipeline_item_id: str, pipeline_parameters: Dict[str, str] = None) -> PipelineRun:
        """
        Creates a pipeline run and waits for its completion.
        """
        run = self.create_run(pipeline_item_id=pipeline_item_id, pipeline_parameters=pipeline_parameters)
        return self.await_run_completion(run)

    def _get_headers(self) -> Dict[str, str]:
        """
        Gets the headers with the authorization token for API requests.
        """
        token = self.credentials.get_token("https://api.fabric.microsoft.com/.default").token
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

def get_credentials(
    azure_subscription_id: str = None,
    service_principal_client_id: str = None,
    service_principal_secret: str = None,
    service_principal_tenant: str = None
) -> ClientSecretCredential:
    """
    Retrieves credentials for authenticating with Fabric REST APIs.
    """
    # Replace these with your actual secret retrieval logic
    azure_subscription_id = azure_subscription_id or get_secret_notebookutils("azure-subscription-id", secretsScope)
    service_principal_client_id = service_principal_client_id or get_secret_notebookutils("spn-gdap-fabricpview-client-id", secretsScope)
    service_principal_secret = service_principal_secret or get_secret_notebookutils("spn-gdap-fabricpview-secret", secretsScope)
    service_principal_tenant = service_principal_tenant or get_secret_notebookutils("BHGuardTenantId", secretsScope)

    if not all([azure_subscription_id, service_principal_client_id, service_principal_secret, service_principal_tenant]):
        raise ValueError("Missing required Azure credentials")

    return ClientSecretCredential(
        tenant_id=service_principal_tenant,
        client_id=service_principal_client_id,
        client_secret=service_principal_secret
    )
