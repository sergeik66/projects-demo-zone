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
