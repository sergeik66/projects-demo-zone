
# In[ ]:


import pkg_resources
import logging
import os
import time


# In[10]:


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# In[29]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run den_nbk_pde_001_common


# In[42]:


def check_whl_published(whl_name: str = 'spark_engine-0.1.0-py3-none-any.whl', max_attempts: int = 10, sleep_interval_seconds: int = 60) -> bool:
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
            installed_packages = [pkg.key for pkg in pkg_resources.working_set]
            
            # Extract the package name from the .whl file
            package_name = whl_name.split('-')[0].replace('_', '-').lower()
            
            # Check if the package is in the installed packages
            if package_name in installed_packages:
                print(f'Package from {whl_name} is installed in the Spark environment.')
                return True
            else:
                print(f'Attempt {attempt}/{max_attempts}: Package from {whl_name} not found. Retrying in {sleep_interval_seconds} seconds...')
                if attempt < max_attempts:
                    time.sleep(sleep_interval_seconds)
                else:
                    print(f'Max attempts reached. Package from {whl_name} is not installed.')
                    return False
                    
        except Exception as e:
            print(f'Error checking .whl file on attempt {attempt}: {str(e)}')
            if attempt < max_attempts:
                print(f'Retrying in {sleep_interval_seconds} seconds...')
                time.sleep(sleep_interval_seconds)
            else:
                print(f'Max attempts reached. Failed to verify {whl_name}.')
                return False


# In[43]:


def get_pipeline_id_by_name(workspace_id: str, pipeline_name: str, headers: Dict[str, str]) -> str:
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=DataPipeline"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    items = response.json().get("value", [])
    for item in items:
        if item.get("displayName") == pipeline_name:
            return item.get("id")
    raise ValueError(f"Pipeline '{pipeline_name}' not found.")


# In[44]:


def get_lakehouse_info(lakehouse_name:str) -> str:
    lakehouse_info = notebookutils.lakehouse.get(lakehouse_name)
    return lakehouse_info


# In[ ]:


def main(run_me: bool = True):
    """Main function to orchestrate notebook execution."""
    start_time = datetime.now()
    logger.info(f"Starting notebook execution at {start_time}")
    
    if not run_me:
        logger.info("run_me is False, skipping notebook execution")
        return


# In[45]:


def create_pipeline_run(pipeline_name: str = 'dfa_pln_dpr_001_policy_dp_scheduled', feed_name: str = 'policy_dp_init', product_name: str = 'POLICY'):
    """ Create Data Pipeline run."""
    
    # Define pipeline parameters
    lh_metadata_name = "den_lhw_pdi_001_metadata"
    lh_raw_name = "den_lhw_dpr_001_raw_files"
    lh_observability_name = "den_lhw_pdi_001_observability"
    workspace_id = get_lakehouse_info(lh_metadata_name)["workspaceId"]

    pipeline_parameters = {
            'feed_name': feed_name,
            'product_name': product_name,
            'lh_metadata_id': get_lakehouse_info(lh_metadata_name)["id"],
            'workspace_id': workspace_id,
            'lh_raw_id': get_lakehouse_info(lh_raw_name)["id"],
            'lh_observability_id': get_lakehouse_info(lh_observability_name)["id"],
            'skip_database_restore': False
        }

    # Instantiate the interface
    fabric = FabricInterface(workspace_id)
    # Get headers with authorization token
    headers = fabric._get_headers()
    
    # Call the function to get the pipeline's GUID
    try:
        pipeline_id = get_pipeline_id_by_name(
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            headers=headers
        )
        print(f"Pipeline ID for '{pipeline_name}': {pipeline_id}")
    except ValueError as e:
        print(str(e))

    # Trigger the pipeline run
    run_response = fabric.create_run(pipeline_item_id=pipeline_id, pipeline_parameters=pipeline_parameters)
    # Output the run ID
    print(f"Pipeline run started with ID: {run_response.run_id}")


# In[ ]:


if __name__ == "__main__":

    # Create  pipeline run
    run_response = create_pipeline_run()
    # Output the run ID
    print(f"Pipeline run started with ID: {run_response.run_id}")


# In[ ]:


if __name__ == "__main__":

    # Specify .whl file to check for
    whl_name = 'spark_engine-0.1.0-py3-none-any.whl'

    try:
        # Check if the .whl file is published
        logger.info(f'Checking if {whl_name} is published in the Spark environment...')
        if check_whl_published(whl_name):
            logger.info(f'{whl_name} is published. Proceeding with pipeline run.')

            # Create and run the pipeline, awaiting completion
            logger.info(f'Starting pipeline run for {pipeline_name} with parameters: {pipeline_parameters}')
            pipeline_run = fabric_client.create_run(pipeline_name=pipeline_name, pipeline_parameters=pipeline_parameters)
            
            # Await pipeline completion
            # final_state = fabric_client.await_run_completion(pipeline_run)
            
            # Get and log the final state
            run_state = fabric_client.get_run_state(pipeline_run.run_id)
            logger.info(f'Pipeline run {pipeline_run.run_id} finished with status: {run_state.status}')
            
            # Check if pipeline succeeded
            if run_state.status == 'Succeeded':
                logger.info(f'Pipeline {pipeline_name} completed successfully.')
            else:
                logger.error(f'Pipeline {pipeline_name} failed with status: {run_state.status}')
                raise RuntimeError(f'Pipeline {pipeline_name} failed with status: {run_state.status}')
        else:
            logger.error(f'Failed to verify {whl_name} is published. Aborting pipeline run.')
            raise RuntimeError(f'{whl_name} is not published in the Spark environment.')
            
    except Exception as e:
        logger.error(f'An error occurred: {str(e)}')
        raise
