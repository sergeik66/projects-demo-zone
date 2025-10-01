# process_alerts.py
import json
import os
import logging
from typing import Dict, Optional
from spark_engine.common.email_util import send_email 
from spark_engine.common.lakehouse import LakehouseManager
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'alerts_{datetime.now().strftime("%Y%m%d")}.log'
)
logger = logging.getLogger(__name__)

class AlertProcessingError(Exception):
    """Custom exception for alert processing errors"""
    pass

def get_template_location_url(
    lakehouse_name: str = "den_lhw_pdi_001_metadata",
    notification_type: str = "emails",
    file_name: str = ""
) -> Optional[str]:
    """
    Get the URL for the template location in the lakehouse.
    
    Args:
        lakehouse_name: Name of the lakehouse
        notification_type: Type of notification (e.g., emails)
        file_name: Name of the template file
    
    Returns:
        String URL path or None if error occurs
    """
    try:
        lakehouse_manager = LakehouseManager(lakehouse_name=lakehouse_name)
        template_path = f"{lakehouse_manager.lakehouse_path}/Files/templates/{notification_type}/"
        return f"{template_path}/{file_name}"
    except Exception as e:
        logger.error(f"Failed to get template location URL: {str(e)}")
        return None

def replace_tokens_in_json_object(json_object: Dict, param_dict: Dict) -> Optional[Dict]:
    """
    Replace tokens in JSON object with parameter values.
    
    Args:
        json_object: JSON object to process
        param_dict: Dictionary of token-value pairs
    
    Returns:
        Processed JSON dictionary or None if error occurs
    """
    try:
        value = json.dumps(json_object)
        for k, v in param_dict.items():
            # Ensure value is string and handle None values
            v = str(v) if v is not None else ""
            value = value.replace('{' + k + '}', v)
        return json.loads(value)
    except (json.JSONDecodeError, TypeError) as e:
        logger.error(f"Error replacing tokens in JSON: {str(e)}")
        return None

def read_json_file(file_location: str) -> Optional[Dict]:
    """
    Read and parse JSON file from the specified location.
    
    Args:
        file_location: Path to the JSON file
    
    Returns:
        Parsed JSON dictionary or None if error occurs
    """
    try:
        if not os.path.exists(file_location):
            logger.error(f"Template file not found at: {file_location}")
            return None
        
        jsonDf = spark.read.text(file_location, wholetext=True)
        content = jsonDf.first()["value"]
        return json.loads(content)
    except (json.JSONDecodeError, Exception) as e:
        logger.error(f"Error reading JSON file {file_location}: {str(e)}")
        return None

def process_alerts(
    elt_id: str,
    template_name: str,
    trigger_time: str,
    pipeline_name: str,
    data_product: str,
    database_names: str,
    workspace_id: str,
    pipeline_id: str,
    run_id: str
) -> bool:
    """
    Main function to process alerts and send email notifications.
    
    Returns:
        True if email sent successfully, False otherwise
    """
    try:
        # Validate required parameters
        required_params = {
            'elt_id': elt_id,
            'template_name': template_name,
            'trigger_time': trigger_time,
            'pipeline_name': pipeline_name,
            'data_product': data_product,
            'database_names': database_names,
            'workspace_id': workspace_id,
            'pipeline_id': pipeline_id,
            'run_id': run_id
        }
        
        for param, value in required_params.items():
            if not value:
                logger.error(f"Missing required parameter: {param}")
                raise AlertProcessingError(f"Missing required parameter: {param}")

        # Get workspace name and key vault
        try:
            workspace_name = notebookutils.runtime.context.get("currentWorkspaceName")
            key_vault_name = secretsScope
        except Exception as e:
            logger.error(f"Error getting workspace details: {str(e)}")
            raise AlertProcessingError("Failed to get workspace details")

        # Create comprehensive replacement tokens dictionary
        replacement_tokens = {
            'elt_id': elt_id,
            'template_name': template_name,
            'trigger_time': trigger_time,
            'pipeline_name': pipeline_name,
            'data_product': data_product,
            'database_names': database_names,
            'workspace_id': workspace_id,
            'pipeline_id': pipeline_id,
            'run_id': run_id,
            'workspace_name': workspace_name,
            'key_vault_name': key_vault_name
        }

        # Get template location
        template_location = get_template_location_url(file_name=template_name)
        if not template_location:
            raise AlertProcessingError("Failed to get template location")

        # Read and process template
        template_data = read_json_file(template_location)
        if not template_data:
            raise AlertProcessingError("Failed to read template file")

        # Replace tokens
        processed_template = replace_tokens_in_json_object(template_data, replacement_tokens)
        if not processed_template:
            raise AlertProcessingError("Failed to process template tokens")

        # Prepare email parameters
        input_params = {
            "subject": processed_template.get("subject", ""),
            "body": processed_template.get("body", {}).get("content", ""),
            "to_email": processed_template.get("emailRecipient", ""),
            "from_account": processed_template.get("emailSender", ""),
            "key_vault_name": key_vault_name
        }

        # Validate email parameters
        for param in ["subject", "body", "to_email", "from_account"]:
            if not input_params[param]:
                logger.error(f"Missing email parameter: {param}")
                raise AlertProcessingError(f"Missing email parameter: {param}")

        # Send email
        send_email(**input_params)
        logger.info("Email sent successfully")
        return True

    except AlertProcessingError as e:
        logger.error(f"Alert processing error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in alert processing: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    # Example usage with parameters
    success = process_alerts(
        elt_id=elt_id,
        template_name=template_name,
        trigger_time=trigger_time,
        pipeline_name=pipeline_name,
        data_product=data_product,
        database_names=database_names,
        workspace_id=workspace_id,
        pipeline_id=pipeline_id,
        run_id=run_id
    )
    
    if not success:
        logger.error("Alert processing failed")
        exit(1)
