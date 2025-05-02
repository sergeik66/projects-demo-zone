from api_client.core.sharepoint_api import SharePointAPI
from api_client.core.utils import save_sharepoint_file_to_lakehouse

sp_api = SharePointAPI(
    tenant_id="YOUR_TENANT_ID",
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    site_url="https://yourtenant.sharepoint.com/sites/yoursite",
    user_agent=USER_AGENT
)

content = sp_api.get_file_by_server_relative_url(
    "/sites/yoursite/Shared Documents/myfile.docx",
    get_content=True
)
if content:
    save_sharepoint_file_to_lakehouse(content, "/lakehouse/default/Files/myfile.docx")



# Get file metadata
metadata = sp_api.get_file_by_server_relative_url(
    "/sites/yoursite/Shared Documents/myfile.docx",
    select="Name,Length,TimeLastModified"
)
if metadata:
    df = spark.createDataFrame([metadata])
    save_dataframe_to_lakehouse(df, "/lakehouse/default/Files/file_metadata")
    df.show()

# Get file content
content = sp_api.get_file_by_server_relative_url(
    "/sites/yoursite/Shared Documents/myfile.docx",
    get_content=True
)
if content:
    with open("/dbfs/workshop_temp/myfile.docx", "wb") as f:
        f.write(content)
