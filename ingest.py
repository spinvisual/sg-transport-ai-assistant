import requests
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import os
from datetime import datetime

# 1. Load configuration from your .env file
load_dotenv()

# Setup Google Cloud Client
# This automatically looks for the path in your GOOGLE_APPLICATION_CREDENTIALS .env variable
client = bigquery.Client()

LTA_KEY = os.getenv('LTA_API_KEY')
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
DATASET_ID = 'sg_transport'
TABLE_ID = 'bus_arrivals_raw'

def fetch_bus_arrivals(bus_stop_codes):
    """Fetch real-time arrivals from LTA Datamall"""
    url = "https://datamall2.mytransport.sg/ltaodataservice/v3/BusArrival"
    headers = {'AccountKey': LTA_KEY}
    all_data = []

    for stop_code in bus_stop_codes:
        print(f"Fetching data for Stop: {stop_code}...")
        try:
            params = {'BusStopCode': stop_code}
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                services = response.json().get('Services', [])
                for svc in services:
                    # We capture the 'NextBus' details (Arrival time, Load, etc.)
                    next_bus = svc.get('NextBus', {})
                    all_data.append({
                        'timestamp': datetime.now().isoformat(),
                        'bus_stop_code': stop_code,
                        'service_no': svc.get('ServiceNo'),
                        'operator': svc.get('Operator'),
                        'estimated_arrival': next_bus.get('EstimatedArrival'),
                        'load': next_bus.get('Load'),
                        'feature': next_bus.get('Feature'), # e.g., Wheelchair accessible
                        'type': next_bus.get('Type')        # e.g., Double Decker
                    })
            else:
                print(f"Error {response.status_code} for stop {stop_code}")
        except Exception as e:
            print(f"Connection error: {e}")

    return pd.DataFrame(all_data)

# Strategic Monitoring: Yishun-CBD Corridor & Test Stops
STOPS_TO_MONITOR = ['83139', '59109', '59149', '58009']

if __name__ == "__main__":
    # A. Fetch the data
    df_arrivals = fetch_bus_arrivals(STOPS_TO_MONITOR)

    if not df_arrivals.empty:
        # B. Define the full table path: project_id.dataset.table
        destination_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # C. Upload to BigQuery
        print(f"Uploading {len(df_arrivals)} rows to BigQuery...")
        df_arrivals.to_gbq(
            destination_table=destination_table,
            project_id=PROJECT_ID,
            if_exists='append' # This keeps historical records for reliability analysis
        )
        print("Ingestion Complete!")
    else:
        print("No data fetched. Check your LTA_API_KEY.")