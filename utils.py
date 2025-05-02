import csv
from pathlib import Path
from typing import List, Dict

def save_forecast_to_csv(periods: List[Dict], filename: str = "weather_forecast.csv") -> None:
    if not periods:
        print("No forecast data to save.")
        return
    headers = [
        "Start Time", "Temperature (°F)", "Wind Speed", "Wind Direction",
        "Short Forecast", "Precipitation Probability (%)", "Relative Humidity (%)",
        "Dewpoint (°F)"
    ]
    try:
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for period in periods:
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
        print(f"Forecast saved to {filename}")
    except IOError as e:
        print(f"Error saving forecast CSV: {e}")

def save_graph_users_to_csv(users: List[Dict], filename: str = "graph_users.csv") -> None:
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
