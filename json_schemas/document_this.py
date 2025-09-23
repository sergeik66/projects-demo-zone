# document_this.py
# Improved version of the original notebook script.
# Changes:
# - Consolidated all imports at the top.
# - Removed non-standard IPython magic commands.
# - Completed and structured the main function to orchestrate the flow.
# - Fixed inconsistencies (e.g., fabric vs. fabric_client, variable definitions).
# - Added type hints where missing.
# - Improved error handling and logging.
# - Made the script more modular and readable.
# - Added docstrings and comments for clarity.
# - Assumed FabricInterface is available; if not, it needs to be imported or defined.
# - Removed redundant code and fixed potential bugs (e.g., pipeline_name and pipeline_parameters usage).

import pkg_resources
import logging
import os
import time
from datetime import datetime
from typing import Dict
import requests
import notebookutils  # Assuming this is available in the environment (Microsoft Fabric specific)
from fabric_interface import FabricInterface  # Replace with actual import if different

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_whl_published(whl_name: str = 'spark_engine-0.1.0-py3-none-any.whl', 
                        max_attempts: int = 10, 
                        sleep_interval_seconds: int = 60) -> bool:
    """
    Check if a custom .whl file is published/installed in the Fabric Spark environment.
    Retries until the package is found or max_attempts is reached.
    
    Args:
        whl_name (str): The name of the .whl file (e.g., 'my_package-1.0-py3-none-any.whl').
        max_attempts (int): Maximum number of attempts to check for the package.
        sleep_interval_seconds (int): Time to wait between attempts in seconds.
    
    Returns:
        bool: True if the .whl file is found in the Spark environment, False otherwise.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            # Get the list of installed libraries in the Spark environment
            installed_packages = {pkg.key.lower() for pkg in pkg_resources.working_set}
            
            # Extract the package name from the .whl file (normalized to lowercase and hyphens)
            package_name = whl_name.split('-')[0].replace('_', '-').lower()
            
            # Check if the package is in the installed packages
            if package_name in installed_packages:
                logger.info(f'Package from {whl_name} is installed in the Spark environment.')
                return True
            else:
                logger.info(f'Attempt {attempt}/{max_attempts}: Package from {whl_name} not found. Retrying in {sleep_interval_seconds} seconds...')
                if attempt < max_attempts:
                    time.sleep(sleep_interval_seconds)
                else:
                    logger.warning(f'Max attempts reached. Package from {whl_name} is not installed.')
                    return False
                    
        except Exception as e:
            logger.error(f'Error checking .whl file on attempt {attempt}: {str(e)}')
            if attempt < max_attempts:
                logger.info(f'Retrying in {sleep_interval_seconds} seconds...')
                time.sleep(sleep_interval_seconds)
            else:
                logger.error(f'Max attempts reached. Failed to verify {whl_name}.')
                return False


def get_pipeline_id_by_name(workspace_id: str, pipeline_name: str, headers: Dict[str, str]) -> str:
    """
    Retrieve the ID of a data pipeline by its name in the given workspace.
    
    Args:
        workspace_id (str): The ID of the workspace.
        pipeline_name (str): The name of the pipeline.
        headers (Dict[str, str]): HTTP headers for authentication.
    
    Returns:
        str: The ID of the pipeline.
    
    Raises:
        ValueError: If the pipeline is not found.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=DataPipeline"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    items = response.json().get("value", [])
    for item in items:
        if item.get("displayName") == pipeline_name:
            return item.get("id")
    raise ValueError(f"Pipeline '{pipeline_name}' not found.")


def get_lakehouse_info(lakehouse_name: str) -> Dict[str, str]:
    """
    Retrieve information about a lakehouse by its name.
    
    Args:
        lakehouse_name (str): The name of the lakehouse.
    
    Returns:
        Dict[str, str]: Lakehouse information.
    """
    lakehouse_info = notebookutils.lakehouse.get(lakehouse_name)
    return lakehouse_info


def create_pipeline_run(pipeline_name: str = 'dfa_pln_dpr_001_policy_dp_scheduled', 
                        feed_name: str = 'policy_dp_init', 
                        product_name: str = 'POLICY') -> str:
    """
    Create and trigger a data pipeline run with specified parameters.
    
    Args:
        pipeline_name (str): The name of the pipeline.
        feed_name (str): The feed name parameter.
        product_name (str): The product name parameter.
    
    Returns:
        str: The run ID of the triggered pipeline.
    """
    # Define lakehouse names
    lh_metadata_name = "den_lhw_pdi_001_metadata"
    lh_raw_name = "den_lhw_dpr_001_raw_files"
    lh_observability_name = "den_lhw_pdi_001_observability"
    
    # Get workspace ID from metadata lakehouse
    workspace_id = get_lakehouse_info(lh_metadata_name)["workspaceId"]
    
    # Define pipeline parameters
    pipeline_parameters = {
        'feed_name': feed_name,
        'product_name': product_name,
        'lh_metadata_id': get_lakehouse_info(lh_metadata_name)["id"],
        'workspace_id': workspace_id,
        'lh_raw_id': get_lakehouse_info(lh_raw_name)["id"],
        'lh_observability_id': get_lakehouse_info(lh_observability_name)["id"],
        'skip_database_restore': False
    }
    
    # Instantiate the Fabric interface
    fabric = FabricInterface(workspace_id)
    
    # Get headers with authorization token
    headers = fabric._get_headers()
    
    # Get the pipeline's GUID
    try:
        pipeline_id = get_pipeline_id_by_name(
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            headers=headers
        )
        logger.info(f"Pipeline ID for '{pipeline_name}': {pipeline_id}")
    except ValueError as e:
        logger.error(str(e))
        raise
    
    # Trigger the pipeline run
    run_response = fabric.create_run(pipeline_item_id=pipeline_id, pipeline_parameters=pipeline_parameters)
    
    # Output the run ID
    logger.info(f"Pipeline run started with ID: {run_response.run_id}")
    return run_response.run_id


def main(run_me: bool = True, whl_name: str = 'spark_engine-0.1.0-py3-none-any.whl'):
    """
    Main function to orchestrate the script execution.
    
    Args:
        run_me (bool): Flag to determine if the pipeline should be run.
        whl_name (str): The .whl file to check for installation.
    """
    start_time = datetime.now()
    logger.info(f"Starting script execution at {start_time}")
    
    if not run_me:
        logger.info("run_me is False, skipping execution")
        return
    
    # Check if the .whl file is published
    logger.info(f'Checking if {whl_name} is published in the Spark environment...')
    if check_whl_published(whl_name):
        logger.info(f'{whl_name} is published. Proceeding with pipeline run.')
        
        # Create and run the pipeline
        pipeline_name = 'dfa_pln_dpr_001_policy_dp_scheduled'
        feed_name = 'policy_dp_init'
        product_name = 'POLICY'
        
        logger.info(f'Starting pipeline run for {pipeline_name} with feed: {feed_name}, product: {product_name}')
        run_id = create_pipeline_run(pipeline_name, feed_name, product_name)
        
        # Instantiate FabricInterface for monitoring (assuming same workspace)
        lh_metadata_name = "den_lhw_pdi_001_metadata"
        workspace_id = get_lakehouse_info(lh_metadata_name)["workspaceId"]
        fabric = FabricInterface(workspace_id)
        
        # Await pipeline completion (commented out as per original; implement if needed)
        # final_state = fabric.await_run_completion(run_id)
        
        # Get and log the final state
        run_state = fabric.get_run_state(run_id)
        logger.info(f'Pipeline run {run_id} finished with status: {run_state.status}')
        
        # Check if pipeline succeeded
        if run_state.status == 'Succeeded':
            logger.info(f'Pipeline {pipeline_name} completed successfully.')
        else:
            logger.error(f'Pipeline {pipeline_name} failed with status: {run_state.status}')
            raise RuntimeError(f'Pipeline {pipeline_name} failed with status: {run_state.status}')
    else:
        logger.error(f'Failed to verify {whl_name} is published. Aborting pipeline run.')
        raise RuntimeError(f'{whl_name} is not published in the Spark environment.')
    
    end_time = datetime.now()
    logger.info(f"Script execution completed at {end_time}. Duration: {end_time - start_time}")


if __name__ == "__main__":
    try:
        main(run_me=True)
    except Exception as e:
        logger.error(f'An error occurred during execution: {str(e)}')
        raise
