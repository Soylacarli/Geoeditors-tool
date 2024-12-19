import pandas as pd
from datetime import date, datetime, timedelta
import requests
import os

# Constants
BASE_URL = "https://analytics.wikimedia.org/published/datasets"
COLS = [
    "wiki_db", "project", "country", "country_code", "activity_level", 
    "count_eps", "sum_eps", "count_release_thresh", "editors", "edits", "time"
]

def get_url(ds, data_type):
    ##Generate the URL for a given dataset and type."""
    d = datetime.strptime(ds, "%Y-%m").date()
    if d < date(2023, 7, 1) or d > date.today():
        raise ValueError("Not a valid date. Please use a month between July 2023 and now.")
    if data_type == "monthly":
        return f"{BASE_URL}/geoeditors_monthly/{ds}.tsv"
    elif data_type == "weekly":
        return f"{BASE_URL}/geoeditors_weekly/{ds}.tsv"
    return None

def generate_date_list():
    ##Generate a list of dates from July 2023 to the current month."""
    current_date = datetime.now()
    start_date = datetime(2023, 7, 1)
    date_list = []
    while start_date < current_date.replace(day=1):
        date_list.append(start_date.strftime("%Y-%m"))
        start_date += timedelta(days=30)
    return sorted(list(set(date_list)))

def fetch_and_merge_data(date_list, data_type="monthly"):
    ##Fetch and merge data for the specified date range and type."""
    master_df = pd.DataFrame()

    for ds in date_list:
        try:
            url = get_url(ds, data_type)
            df = pd.read_csv(url, delimiter='\t', names=COLS, on_bad_lines='warn')
            master_df = pd.concat([master_df, df], axis=0, ignore_index=True)
        except Exception as e:
            print(f"Failed to fetch data for {ds}: {e}")

    if master_df.empty:
        raise ValueError("No data could be fetched. Please check the parameters.")

    return master_df

# Save the master DataFrame to a CSV file
def save_data(master_df, country=None):
    os.makedirs('./data', exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"./data/master_df{'-' + country if country else ''}-{timestamp}.csv"
    master_df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
