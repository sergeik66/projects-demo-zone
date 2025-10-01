import json
import os
from spark_engine.common.email_util import send_email 
from spark_engine.common.lakehouse import LakehouseManager

# parameters
elt_id = ""
template_name = ""
trigger_time = ""
pipeline_name = ""
data_product = ""
database_names = ""
workspace_id = ""
pipeline_id = ""
run_id = ""

def get_template_location_url(lakehouse_name="den_lhw_pdi_001_metadata",notification_type="emails",file_name="") -> str:
    lakehouse_manager = LakehouseManager(lakehouse_name=lakehouse_name)
    template_path = f"{lakehouse_manager.lakehouse_path}/Files/templates/{notification_type}/"
    return f"{template_path}/{file_name}"
def replace_tokens_in_json_object(json_object: dict, param_dict: dict):
    value = json.dumps(json_object)
    for k, v in param_dict.items():
        value = value.replace('{' + k + '}', v)

    value_dict = json.loads(value)
    return value_dict
def read_json_file(file_location):
    jsonDf = spark.read.text(file_location, wholetext=True)
    content = jsonDf.first()["value"]
    return json.loads(content)

# main processesing
template_name_location = get_template_location_url(file_name=template_name)

# set replacement tokens value 
workspace_name = workspace_name = notebookutils.runtime.context.get("currentWorkspaceName")
key_vault_name = secretsScope 

replacement_tokens = {
    'workspace_name': workspace_name,
    'workspace_id': error_details
    }

# load template file and replace tokens
template_name_location = get_template_location_url(file_name=template_name)
value = read_json_file(template_name_location)
request_dict = replace_tokens_in_json_object(value, replacement_tokens)

# set parameters and send email
input_params = {
    "subject" : request_dict["subject"],
    "body" : request_dict["body"]["content"],
    "to_email" : request_dict["emailRecipient"],
    "from_account" : request_dict["emailSender"],
    "key_vault_name" : secretsScope
}

send_email(**input_params)
