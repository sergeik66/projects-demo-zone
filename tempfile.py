def save_to_lakehouse(file_content, lakehouse_path, file_path, extention):
    try:
        full_file_path = f"{lakehouse_path}/{file_path}"
        os.makedirs(os.path.dirname(lakehouse_path), exist_ok=True)
        with tempfile.NamedTemporaryFile(delete=False, suffix=extention) as temp_file:
            temp_path = temp_file.name
            temp_file.write(file_content)
        fs.cp(temp_path, full_file_path)
        os.remove(temp_path)    
        print(f"File saved to lakehouse: {full_file_path}")
        return True
    except Exception as e:
        print(f"Error saving file to lakehouse: {e}")
        return False


azcopy failed, cmd:azcopy copy '/tmp/tmpe282k15r.xlsx' 'https://onelake.blob.fabric.microsoft.com/a72bf9c2-7aab-42ab-b2c7-42833f3ca89e/b66e51f1-4687-4a21-ac17-9f609ec41cc2/Files/reference_codes/window_protection_covering_types_code/affd4711-1713-4afc-98f6-32870353b2fg/Ref_Window Protection Covering Types_v1.xlsx'  --trusted-microsoft-suffixes="*.pbidedicated.windows.net;*.pbidedicated.windows-int.net;*.fabric.microsoft.com"  --skip-version-check exit:1 stdout:b"INFO: Scanning...\nINFO: AZCOPY_OAUTH_TOKEN_INFO is set.\nINFO: Autologin not specified.\nINFO: Authenticating to destination using Azure AD\nINFO: Any empty folders will not be processed, because source and/or destination doesn't have full folder support\n\nJob 87d79623-f1af-7c46-629e-4381e6ceb027 has started\nLog file is located at: /home/trusted-service-user/.azcopy/87d79623-f1af-7c46-629e-4381e6ceb027.log\n\n\r0.0 %, 0 Done, 1 Failed, 0 Pending, 0 Skipped, 1 Total, 2-sec Throughput (Mb/s): 0.0607\n\n\nJob 87d79623-f1af-7c46-629e-4381e6ceb027 summary\nElapsed Time (Minutes): 0.0337\nNumber of File Transfers: 1\nNumber of Folder Property Transfers: 0\nNumber of Symlink Transfers: 0\nTotal Number of Transfers: 1\nNumber of File Transfers Completed: 0\nNumber of Folder Transfers Completed: 0\nNumber of File Transfers Failed: 1\nNumber of Folder Transfers Failed: 0\nNumber of File Transfers Skipped: 0\nNumber of Folder Transfers Skipped: 0\nTotal Number of Bytes Transferred: 0\nFinal Job Status: Failed\n\n" stderr:b'' exception: azcopy not exit 0, please check the azcopy log.
