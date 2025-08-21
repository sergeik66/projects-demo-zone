import notebookutils as nu
import fsspec
import json
def list_folder_contents(path, result_list=None):
    if result_list is None:
        result_list = []
    try:  
        items = nu.fs.ls(path)
        for item in items:
            if item.isDir:
                # don't add to the list, print it for visibility
                print(f"Skipped Folder: {item.name.rstrip('/')}")
                list_folder_contents(item.path, result_list)
            else:
                # add file entry with folder names as the parent folder
                parent_folder = path.split('/')[-1] if '/' in path else "" 
                if parent_folder != 'watermark':
                    result_list.append({"folderName": parent_folder, "fileName": item.name}) 
                    # print(f"Add to the list:'folderName': {parent_folder}, 'fileName': {item.name}")
    except Exception as e:
        print(f"Error accessing {path}: {str(e)}") 
    return result_list
def generate_watermark(source_config_folder_name, source_config_file_name):
    input_path = source_config_folder_name + source_config_file_name
    watermarkPath = f"{source_config_folder_name}watermark/{source_config_file_name}"

    onelake_fs = fsspec.filesystem("abfss", **storage_options)

    dataset = json.load(onelake_fs.open(input_path, "r"))
    sourceSystemProperties = dataset["sourceSystemProperties"]

    # set initial values
    query = ''
    columnsList = "*"
    whereClause = ''
    watermarkValue = ''
    sourceWatermarkIdentifier = ''
    jsonWatermarkQuery = ''

    if dataset["datasetTypeName"] in ["database","file"]:
        # build column list
        columnsList = ",".join(sourceSystemProperties.get("includeSpecificColumns", ["*"]))

        if sourceSystemProperties["ingestType"] == "watermark":
            # add watermark column to select list
            sourceWatermarkIdentifier = sourceSystemProperties["sourceWatermarkIdentifier"]
            columnsList += f",{sourceWatermarkIdentifier} as dl_watermark"

            # set watermark value using existing value if available
            watermarkValue = '1900-01-01T00:00:00Z'
            if onelake_fs.exists(watermarkPath):
                existingWatermarkJson = json.load(onelake_fs.open(watermarkPath, "r", encoding="utf-8-sig"))
                if existingWatermarkJson["sourceWatermarkIdentifier"] == sourceWatermarkIdentifier and existingWatermarkJson["watermarkValue"]:
                    watermarkValue = existingWatermarkJson["watermarkValue"]

            # build where clause
            whereClause = f"WHERE {sourceWatermarkIdentifier} > CAST('{watermarkValue}' AS datetime2)"

        filterExpression = sourceSystemProperties.get('filterExpression')

        if sourceSystemProperties.get("isDynamicQuery") and filterExpression:
            filterExpression = filterExpression.strip().lower()

            if filterExpression[0:6] == "where ":
                filterExpression = filterExpression.replace("where", "", 1).lstrip()
            elif filterExpression[0:4] == "and ":
                filterExpression = filterExpression.replace("and", "", 1).lstrip()

            if not whereClause:
                whereClause = "where " + filterExpression
            else:
                whereClause = f"{whereClause} and {filterExpression}"

        # set query
        query = f"SELECT {columnsList} FROM {dataset.get('datasetSchema', 'dbo')}.{dataset['datasetName']} {whereClause}"

        # build json for watermark file
        jsonWatermarkQuery = {
            "query": query,
            "sourceWatermarkIdentifier": sourceWatermarkIdentifier,
            "watermarkValue": watermarkValue
        }

        # write json to lakehouse
        with onelake_fs.open(watermarkPath, "w") as json_file:
            json.dump(jsonWatermarkQuery, json_file, indent=4)
            
        print(f"Watermark file generated at: {watermarkPath}")

storage_options = {
    "account_name": "onelake",
    "account_host": "onelake.dfs.fabric.microsoft.com",
}
# get workspace and lakehouse ids
lakehouse = nu.lakehouse.get("den_lhw_pdi_001_metadata")
workspace_id = lakehouse["workspaceId"]
lakehouse_id = lakehouse["id"]

#set the datasets folder path
datasets_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/datasets"

# print(f"Listing contents of: {datasets_path}")
# call the function to get the list of dataset files
contents = list_folder_contents(datasets_path)

for item in contents:
    source_config_folder_name = f"{datasets_path}/{item['folderName']}/"
    source_config_file_name = f"{item['fileName']}"
    generate_watermark(source_config_folder_name, source_config_file_name)
