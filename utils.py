import csv
from pathlib import Path
from typing import List, Dict, Any

def save_nws_data_to_csv(data: Any, filename: str = "nws_data.csv", data_type: str = "forecast") -> None:
    """
    Save NWS API data to a CSV file.
    
    Parameters:
    data (Any): NWS API response data
    filename (str): Output CSV file name
    data_type (str): Type of data (e.g., "forecast", "alerts", "points")
    """
    if not data:
        print("No NWS data to save.")
        return
    
    try:
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            
            if data_type == "forecast" and isinstance(data, dict) and "periods" in data:
                headers = [
                    "Start Time", "Temperature (°F)", "Wind Speed", "Wind Direction",
                    "Short Forecast", "Precipitation Probability (%)", "Relative Humidity (%)",
                    "Dewpoint (°F)"
                ]
                writer.writerow(headers)
                for period in data["periods"]:
                    row = [
                        period.get("startTime", ""),
                        period.get("temperature", ""),
                        period.get("windSpeed", ""),
                        period.get("windDirection", ""),
                        period.get("shortForecast", ""),
                        period.get("probabilityOfPrecipitation", {}).get("value", ""),
                        period.get("relativeHumidity", {}).get("value", ""),
                        period.get("dewpoint", {}).get("value", "")
                    ]
                    writer.writerow(row)
            
            elif data_type == "alerts" and isinstance(data, dict) and "features" in data:
                headers = ["ID", "Event", "Effective", "Expires", "Area", "Description"]
                writer.writerow(headers)
                for feature in data["features"]:
                    props = feature.get("properties", {})
                    row = [
                        props.get("id", ""),
                        props.get("event", ""),
                        props.get("effective", ""),
                        props.get("expires", ""),
                        ", ".join(props.get("areaDesc", "").split(";")),
                        props.get("description", "")
                    ]
                    writer.writerow(row)
            
            elif data_type == "points" and isinstance(data, dict):
                headers = ["Grid ID", "Grid X", "Grid Y", "Forecast Zone", "County"]
                writer.writerow(headers)
                row = [
                    data.get("gridId", ""),
                    data.get("gridX", ""),
                    data.get("gridY", ""),
                    data.get("forecastZone", ""),
                    data.get("county", "")
                ]
                writer.writerow(row)
            
            else:
                # Generic handler for other data types
                if isinstance(data, list) and data:
                    headers = list(data[0].keys())
                    writer.writerow(headers)
                    for item in data:
                        writer.writerow([item.get(key, "") for key in headers])
                elif isinstance(data, dict):
                    headers = list(data.keys())
                    writer.writerow(headers)
                    writer.writerow([data.get(key, "") for key in headers])
                else:
                    print(f"Unsupported data format for {data_type}")
                    return
        
        print(f"NWS data saved to {filename}")
    except IOError as e:
        print(f"Error saving NWS CSV: {e}")

def save_graph_users_to_csv(users: List[Dict], filename: str = "graph_users.csv") -> None:
    """
    Save Microsoft Graph user data to a CSV file.
    
    Parameters:
    users (List[Dict]): List of user dictionaries
    filename (str): Output CSV file name
    """
    if not users:
        print("No Graph user data to save.")
        return
    headers = ["User ID", "Display Name", "Mail", "User Principal Name"]
    try:
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for user in users:
                row = [
                    user.get("id", ""),
                    user.get("displayName", ""),
                    user.get("mail", ""),
                    user.get("userPrincipalName", "")
                ]
                writer.writerow(row)
        print(f"Graph users saved to {filename}")
    except IOError as e:
        print(f"Error saving Graph users CSV: {e}")

def save_sharepoint_file_to_lakehouse(content: bytes, path: str) -> None:
    """
    Save SharePoint file content to the Fabric lakehouse.
    
    Args:
        content: Binary file content from SharePoint (e.g., from GetFileByServerRelativeUrl).
        path: Lakehouse path (e.g., '/lakehouse/default/Files/myfile.docx').
    """
    try:
        import os
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(content)
        print(f"SharePoint file saved to {path}")
    except IOError as e:
        print(f"Error saving SharePoint file to lakehouse: {e}")
