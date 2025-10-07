import json
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from datetime import datetime
import sys
import os
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'notebook_execution_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

async def execute_notebook(notebook_path, progress_bar):
    """Execute a single notebook and handle errors."""
    try:
        if not os.path.exists(f"{notebook_path}.ipynb"):
            logger.error(f"Notebook file {notebook_path}.ipynb not found")
            raise FileNotFoundError(f"Notebook {notebook_path}.ipynb not found")
        
        logger.info(f"Starting execution of {notebook_path}")
        with open(f"{notebook_path}.ipynb", 'r', encoding='utf-8') as f:
            nb = nbformat.read(f, as_version=4)
        
        ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
        
        with ThreadPoolExecutor() as executor:
            await asyncio.get_event_loop().run_in_executor(
                executor,
                lambda: ep.preprocess(nb, {'metadata': {'path': './'}})
            )
        logger.info(f"Successfully executed {notebook_path}")
        progress_bar.update(1)
        return True
    except Exception as e:
        logger.error(f"Error executing {notebook_path}: {str(e)}")
        progress_bar.update(1)
        return False

async def execute_group(notebooks, group_number, total_notebooks):
    """Execute a group of notebooks in parallel with progress tracking."""
    try:
        logger.info(f"Starting execution of Group {group_number} with notebooks: {notebooks}")
        with tqdm(total=len(notebooks), desc=f"Group {group_number} Progress", leave=False) as pbar:
            tasks = [execute_notebook(notebook, pbar) for notebook in notebooks]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        failed = [notebook for notebook, result in zip(notebooks, results) if result is False]
        if failed:
            logger.warning(f"Some notebooks in Group {group_number} failed: {failed}")
        else:
            logger.info(f"Completed Group {group_number} successfully")
        return all(results)
    except Exception as e:
        logger.error(f"Error processing Group {group_number}: {str(e)}")
        return False

def parse_notebooks(config):
    """Parse notebook groups from configuration."""
    try:
        groups = []
        total_notebooks = 0
        
        # Process Group A
        if "notebooksGroupA" in config:
            group_a_notebooks = []
            for group in config["notebooksGroupA"]:
                if "notebooks" in group:
                    for notebook in group["notebooks"]:
                        if "notebookName" in notebook:
                            group_a_notebooks.append(notebook["notebookName"])
                            total_notebooks += 1
            if group_a_notebooks:
                groups.append(group_a_notebooks)
                logger.info(f"Parsed Group A notebooks: {group_a_notebooks}")
        
        # Process Group B
        if "notebooksGroupB" in config:
            group_b_notebooks = []
            for group in config["notebooksGroupB"]:
                if "notebookName" in group:
                    for notebook in group["notebookName"]:
                        if "notebookName" in notebook:
                            group_b_notebooks.append(notebook["notebookName"])
                            total_notebooks += 1
            if group_b_notebooks:
                groups.append(group_b_notebooks)
                logger.info(f"Parsed Group B notebooks: {group_b_notebooks}")
        
        # Process Group C
        if "notebooksGroupC" in config:
            group_c_notebooks = []
            for group in config["notebooksGroupC"]:
                if isinstance(group, dict) and "files" in group and isinstance(group["files"], dict) and "notebookName" in group["files"]:
                    for notebook in group["files"]["notebookName"]:
                        if "notebookName" in notebook:
                            group_c_notebooks.append(notebook["notebookName"])
                            total_notebooks += 1
            if group_c_notebooks:
                groups.append(group_c_notebooks)
                logger.info(f"Parsed Group C notebooks: {group_c_notebooks}")
        
        if not groups:
            logger.warning("No valid notebook groups found in configuration")
        return groups, total_notebooks
    except Exception as e:
        logger.error(f"Error parsing configuration: {str(e)}")
        raise

async def main(config_file):
    """Main function to orchestrate notebook execution with progress tracking."""
    try:
        logger.info(f"Starting notebook orchestration with config: {config_file}")
        
        if not os.path.exists(config_file):
            logger.error(f"Configuration file {config_file} not found")
            raise FileNotFoundError(f"Configuration file {config_file} not found")
        
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        groups, total_notebooks = parse_notebooks(config)
        logger.info(f"Total notebooks to process: {total_notebooks}")
        
        with tqdm(total=total_notebooks, desc="Overall Progress") as main_pbar:
            for i, group in enumerate(groups, 1):
                success = await execute_group(group, i, total_notebooks)
                if not success:
                    logger.warning(f"Group {i} had execution failures, continuing with next group")
                main_pbar.update(len(group))
        
        logger.info("Notebook orchestration completed")
    except Exception as e:
        logger.error(f"Fatal error in orchestration: {str(e)}")
        raise

if __name__ == "__main__":
    config_file = "config.json"
    try:
        asyncio.run(main(config_file))
    except Exception as e:
        logger.critical(f"Program terminated due to error: {str(e)}")
        sys.exit(1)