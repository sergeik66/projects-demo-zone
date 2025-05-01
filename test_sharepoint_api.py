import pytest
from unittest.mock import patch, Mock
from api_client.core.sharepoint_api import SharePointAPI

@pytest.fixture
def sharepoint_api():
    return SharePointAPI(
        tenant_id="test_tenant",
        client_id="test_client",
        client_secret="test_secret",
        site_url="https://test.sharepoint.com/sites/testsite",
        user_agent="TestApp/1.0"
    )

def test_get_access_token_success(sharepoint_api):
    with patch.object(sharepoint_api, "_make_request") as mock_make_request:
        mock_make_request.return_value = {"access_token": "test_token"}
        
        token = sharepoint_api._get_access_token()
        assert token == "test_token"
        assert sharepoint_api.access_token == "test_token"
        mock_make_request.assert_called_once_with(
            endpoint=sharepoint_api.token_url,
            method="POST",
            form_data={
                "client_id": "test_client",
                "client_secret": "test_secret",
                "scope": "https://test.sharepoint.com/.default",
                "grant_type": "client_credentials"
            }
        )

def test_fetch_data_success(sharepoint_api):
    with patch.object(sharepoint_api, "_get_access_token") as mock_get_token:
        with patch.object(sharepoint_api, "_make_request") as mock_make_request:
            mock_get_token.return_value = "test_token"
            mock_make_request.return_value = {"d": {"results": [{"Title": "Item1"}]}}
            
            result = sharepoint_api.fetch_data("web/lists")
            assert result == [{"Title": "Item1"}]
            mock_make_request.assert_called_once_with(
                "web/lists",
                params=None,
                headers={"Accept": "application/json;odata=verbose"}
            )

def test_get_lists(sharepoint_api):
    with patch.object(sharepoint_api, "fetch_data") as mock_fetch_data:
        mock_fetch_data.return_value = [{"Title": "List1"}]
        
        result = sharepoint_api.get_lists()
        assert result == [{"Title": "List1"}]
        mock_fetch_data.assert_called_once_with("web/lists")

def test_get_list_items(sharepoint_api):
    with patch.object(sharepoint_api, "fetch_data") as mock_fetch_data:
        mock_fetch_data.return_value = [{"Title": "Item1", "Id": 1}]
        
        result = sharepoint_api.get_list_items("MyList", select="Title,Id")
        assert result == [{"Title": "Item1", "Id": 1}]
        mock_fetch_data.assert_called_once_with(
            "web/lists/getbytitle('MyList')/items",
            params={"$select": "Title,Id"}
        )

def test_create_list_item(sharepoint_api):
    with patch.object(sharepoint_api, "_make_request") as mock_make_request:
        mock_make_request.return_value = {"d": {"Id": 1, "Title": "New Item"}}
        
        result = sharepoint_api.create_list_item("MyList", {"Title": "New Item"})
        assert result == {"Id": 1, "Title": "New Item"}
        mock_make_request.assert_called_once_with(
            "web/lists/getbytitle('MyList')/items",
            method="POST",
            json_data={"__metadata": {"type": "SP.Data.MyListListItem"}, "Title": "New Item"},
            headers={"Accept": "application/json;odata=verbose", "Content-Type": "application/json;odata=verbose"}
        )

def test_get_file_by_server_relative_url_metadata(sharepoint_api):
    with patch.object(sharepoint_api, "_get_access_token") as mock_get_token:
        with patch.object(sharepoint_api, "_make_request") as mock_make_request:
            mock_get_token.return_value = "test_token"
            mock_make_request.return_value = {"d": {"Name": "myfile.docx", "Length": 1024}}
            
            result = sharepoint_api.get_file_by_server_relative_url(
                "/sites/testsite/Shared Documents/myfile.docx",
                select="Name,Length"
            )
            assert result == {"Name": "myfile.docx", "Length": 1024}
            mock_make_request.assert_called_once_with(
                "web/GetFileByServerRelativeUrl('/sites/testsite/Shared Documents/myfile.docx')",
                params={"$select": "Name,Length"},
                headers={"Accept": "application/json;odata=verbose"}
            )

def test_get_file_by_server_relative_url_content(sharepoint_api):
    with patch.object(sharepoint_api, "_get_access_token") as mock_get_token:
        with patch.object(sharepoint_api, "_make_request") as mock_make_request:
            mock_get_token.return_value = "test_token"
            mock_make_request.return_value = b"file_content"
            
            result = sharepoint_api.get_file_by_server_relative_url(
                "/sites/testsite/Shared Documents/myfile.docx",
                get_content=True
            )
            assert result == b"file_content"
            mock_make_request.assert_called_once_with(
                "web/GetFileByServerRelativeUrl('/sites/testsite/Shared Documents/myfile.docx')/$value",
                headers={"Accept": "*/*"},
                method="GET"
            )
