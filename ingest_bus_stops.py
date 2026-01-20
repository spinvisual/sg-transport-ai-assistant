import requests
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

# Point to your key specifically
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sg-transport-key.json"

client = bigquery.Client()
LTA_KEY = os.getenv('LTA_API_KEY')
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')

def fetch_all_bus_stops():
    url = "https://datamall2.mytransport.sg/ltaodataservice/BusStops"
    headers = {'AccountKey': LTA_KEY}
    all_stops = []
    skip = 0

    while True:
        print(f"Fetching stops starting from index {skip}...")
        response = requests.get(url, headers=headers, params={'$skip': skip})
        
        # DEBUG: If the server returns an error, show us the code
        if response.status_code != 200:
            print(f"Error from LTA: Status {response.status_code}")
            print(f"Response Content: {response.text}")
            break

        try:
            data = response.json().get('value', [])
        except Exception as e:
            print(f"Failed to parse JSON. Server returned: {response.text[:200]}")
            break
        
        if not data:
            break
        
        all_stops.extend(data)
        skip += 500 

    return pd.DataFrame(all_stops)

if __name__ == "__main__":
    if not LTA_KEY:
        print("ERROR: LTA_API_KEY not found in .env file!")
    else:
        df_stops = fetch_all_bus_stops()
        
        if not df_stops.empty:
            df_stops.columns = [c.lower() for c in df_stops.columns]
            table_id = f"{PROJECT_ID}.sg_transport.bus_stops_master"
            print(f"Uploading {len(df_stops)} bus stops to BigQuery...")
            df_stops.to_gbq(table_id, project_id=PROJECT_ID, if_exists='replace')
            print("Master List Uploaded Successfully!")
        else:
            print("No data was fetched. Check your API Key.")