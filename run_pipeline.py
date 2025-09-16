# run_pipeline.py

# Import the FabricInterface from fabric_interface.py
from fabric_interface import FabricInterface

from pyspark.sql import SparkSession
import pkg_resources
import logging
import os
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_whl_published(whl_name: str, max_attempts: int = 10, sleep_interval_seconds: int = 30) -> bool:
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
            # Initialize Spark session
            spark = SparkSession.builder.getOrCreate()
            
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

if __name__ == "__main__":
    # Define pipeline parameters
    pipeline_parameters = {
        'feed_name': 'sample_feed',
        'product_name': 'sample_product',
        'lh_metadata_id': 'metadata_123',
        'workspace_id': 'ws_456',
        'lh_raw_id': 'raw_789',
        'lh_observability_id': 'obs_101'
    }

    # Initialize FabricInterface
    fabric_client = FabricInterface()

    # Specify the pipeline name and .whl file to check
    pipeline_name = 'sample_pipeline'
    whl_name = 'my_package-1.0-py3-none-any.whl'

    try:
        # Check if the .whl file is published
        logger.info(f'Checking if {whl_name} is published in the Spark environment...')
        if check_whl_published(whl_name):
            logger.info(f'{whl_name} is published. Proceeding with pipeline run.')
            # Create and run the pipeline, awaiting completion
            logger.info(f'Starting pipeline run for {pipeline_name} with parameters: {pipeline_parameters}')
            pipeline_run = fabric_client.create_run(pipeline_name=pipeline_name, pipeline_parameters=pipeline_parameters)
            
            # Await pipeline completion
            final_state = fabric_client.await_run_completion(pipeline_run)
            
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
