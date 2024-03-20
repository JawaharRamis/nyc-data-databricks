# import requests

# API_URL = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json?crash_date=2023-02-21T00:00:00.000'

# response = requests.get(API_URL)

# print(response)
import requests
import pandas as pd
from sodapy import Socrata
headers = {"X-App-Token": "TDZkrnd7Wnm6YN8fhneT2UjqW"}

client = Socrata("data.cityofnewyork.us", "TDZkrnd7Wnm6YN8fhneT2UjqW", timeout=10)

results = client.get("h9gi-nx95", where ="crash_date > '2023-09-10T00:00:00'", order="crash_date ASC", limit=50000)

results_df = pd.DataFrame.from_records(results)

# # Handle potential empty DataFrame scenario
if len(results_df) ==0:
    print("No data retrieved from Socrata API.")


print(len(results_df))