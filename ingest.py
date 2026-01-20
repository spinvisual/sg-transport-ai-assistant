import requests
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import os
import time
from datetime import datetime

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sg-transport-key.json"
client = bigquery.Client()

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
# Use your active AccountKey here [cite: 58]
LTA_ACCOUNT_KEY = '/Ret98ASSvOCW9XhLdGSTg==' 

BUS_STOPS = ['44259', '44639']

def run_patrol():
    # Use the v3 HTTPS URL specified in the Jan 2026 Guide [cite: 25, 243]
    url = "https://datamall2.mytransport.sg/ltaodataservice/v3/BusArrival"
    
    headers = {
        'AccountKey': LTA_ACCOUNT_KEY,
        'accept': 'application/json'
    }
    
    all_rows = []

    for stop in BUS_STOPS:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Checking stop: {stop}...")
        
        try:
            # Use mandatory parameter 'BusStopCode' [cite: 243]
            response = requests.get(url, headers=headers, params={'BusStopCode': stop}, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                services = data.get('Services', [])
                
                # If no services, it might be outside operating hours [cite: 248, 249]
                if not services:
                    print(f"   -> No buses in service for stop {stop} right now.")
                else:
                    print(f"   -> Success! Found {len(services)} bus services.")
                
                for s in services:
                    s['bus_stop_code'] = stop
                    s['extracted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    all_rows.append(s)
            else:
                print(f"   -> Error {response.status_code}. URL: {response.url}")
                
        except Exception as e:
            print(f"   -> Connection failed: {e}")

    # Uploading to BigQuery
    if all_rows:
        try:
            # 1. Create a "flat" version of the data
            # LTA data is 'nested' (NextBus, NextBus2), so we flatten it
            df = pd.json_normalize(all_rows)
            
            # 2. Clean the column names (Lowercase and replace dots with underscores)
            # BigQuery doesn't like dots (e.g., nextbus.estimatedarrival)
            df.columns = [c.lower().replace('.', '_') for c in df.columns]
            
            table_id = "sg_transport.bus_arrivals_raw"
            print(f"   -> Uploading {len(df)} rows to BigQuery...")
            
            # 3. Use the direct pandas_gbq call
            import pandas_gbq
            pandas_gbq.to_gbq(
                df, 
                table_id, 
                project_id=PROJECT_ID, 
                if_exists='append' # This will now create the table if it's missing
            )
            print("   -> [!!!] DATA SUCCESSFULLY LANDED IN BIGQUERY [!!!]")
            
        except Exception as e:
            print(f"   -> BigQuery Error: {e}")

if __name__ == "__main__":
    print("--- Starting Continuous Patrol (LTA Guide v6.6) ---")
    while True:
        run_patrol()
        # Updated frequency to 30 seconds as per guide 
        print("--- Waiting 30 seconds ---")
        time.sleep(30)