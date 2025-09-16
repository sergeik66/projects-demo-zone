from azure.identity import ClientSecretCredential
from azure.mgmt.fabric import FabricManagementClient
from azure.mgmt.fabric.models import PipelineRun, PipelineRunCreateResponse
from modules import constants
import os
import logging
import time
import uuid

logger = logging.getLogger(__name__)

class FabricInterface:
    """
    For interacting with Microsoft Fabric data pipelines.
    """
    def __init__(self, workspace_name: str = None, azure_resource_group_name: str = None):
        self._mc = get_management_client()
        if not workspace_name:
            self.workspace_name = os.getenv(constants.ENV_VAR_NAME_FABRIC_WORKSPACE_NAME)
        else:
            self.workspace_name = workspace_name
        if not azure_resource_group_name:
            self.azure_resource_group_name = os.getenv(constants.ENV_VAR_NAME_AZURE_RESOURCE_GROUP_NAME)
        else:
            self.azure_resource_group_name = azure_resource_group_name
        logger.debug(f"Instantiated with Fabric workspace name {self.workspace_name} and Resource Group name {self.azure_resource_group_name}")

    def create_run(self, pipeline_name: str, pipeline_parameters: dict[str] = None) -> PipelineRunCreateResponse:
        """
        Creates a new pipeline run in the Fabric workspace.
        """
        run_id = str(uuid.uuid4())
        run = self._mc.pipelines.create_run(
            resource_group_name=self.azure_resource_group_name,
            workspace_name=self.workspace_name,
            pipeline_name=pipeline_name,
            run_id=run_id,
            parameters=pipeline_parameters or {}
        )
        logger.info(f"Created pipeline run for {pipeline_name} with run ID {run.run_id}")
        return run

    def get_run_state(self, run_id: str) -> PipelineRun:
        """
        Retrieves the state of a pipeline run.
        """
        run_state = self._mc.pipeline_runs.get(
            resource_group_name=self.azure_resource_group_name,
            workspace_name=self.workspace_name,
            run_id=run_id
        )
        return run_state

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
                logger.debug(f"Sleeping for {poll_interval_seconds} seconds before polling again...")
                time.sleep(poll_interval_seconds)

    def create_run_and_await_completion(self, pipeline_name: str, pipeline_parameters: dict[str] = None) -> PipelineRun:
        """
        Creates a pipeline run and waits for its completion.
        """
        run = self.create_run(
            pipeline_name=pipeline_name,
            pipeline_parameters=pipeline_parameters
        )
        return self.await_run_completion(run)

def get_management_client(
        azure_subscription_id: str = None,
        service_principal_client_id: str = None,
        service_principal_secret: str = None,
        service_principal_tenant: str = None
) -> FabricManagementClient:
    """
    Retrieves a FabricManagementClient for interacting with Fabric pipelines.
    """
    if not azure_subscription_id:
        azure_subscription_id = os.getenv(constants.ENV_VAR_NAME_AZURE_SUBSCRIPTION_ID)
    if not service_principal_client_id:
        service_principal_client_id = os.getenv(constants.ENV_VAR_NAME_AZURE_SERVICE_PRINCIPAL_CLIENT_ID)
    if not service_principal_secret:
        service_principal_secret = os.getenv(constants.ENV_VAR_NAME_AZURE_SERVICE_PRINCIPAL_SECRET)
    if not service_principal_tenant:
        service_principal_tenant = os.getenv(constants.ENV_VAR_NAME_AZURE_SERVICE_PRINCIPAL_TENANT)

    if not azure_subscription_id:
        e = ValueError(f"Must supply an Azure subscription ID, via argument or environment variable {constants.ENV_VAR_NAME_AZURE_SUBSCRIPTION_ID}")
        logger.error(e)
        raise e
    if not service_principal_client_id:
        e = ValueError(f"Must supply an Azure service principal client ID, via argument or environment variable {constants.ENV_VAR_NAME_AZURE_SERVICE_PRINCIPAL_CLIENT_ID}")
        logger.error(e)
        raise e
    if not service_principal_secret:
        e = ValueError(f"Must supply an Azure service principal secret via argument or environment variable {constants.ENV_VAR_NAME_AZURE_SERVICE_PRINCIPAL_SECRET}")
        logger.error(e)
        raise e
    if not service_principal_tenant:
        e = ValueError(f"Must supply a service principal tenant ID via argument or environment variable {constants.ENV_VAR_NAME_AZURE_SERVICE_PRINCIPAL_TENANT}")
        logger.error(e)
        raise e

    credentials = ClientSecretCredential(
        tenant_id=service_principal_tenant,
        client_id=service_principal_client_id,
        client_secret=service_principal_secret
    )
    client = FabricManagementClient(credentials, azure_subscription_id)
    return client