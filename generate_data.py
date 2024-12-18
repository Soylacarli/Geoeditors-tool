import pandas as pd
from datetime import datetime, timedelta
import os

# Base URL for Wikimedia dataset
base_url = "https://analytics.wikimedia.org/published/datasets"
cols = ["wiki_db", "project", "country", "country_code", "activity_level", 
        "count_eps", "sum_eps", "count_release_thresh", "editors", "edits", "time"]

# Folder to store data
DATA_FOLDER = "./data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

def get_url(ds, type):
    """Generate URL for the dataset."""
    d = datetime.strptime(ds, "%Y-%m").date()
    if d < datetime(2023, 7, 1).date() or d > datetime.today().date():
        print(f"Invalid date: {ds}. Must be between July 2023 and the current month.")
        return None
    if type == "monthly":
        return f"{base_url}/geoeditors_monthly/{ds}.tsv"
    elif type == "weekly":
        return f"{base_url}/geoeditors_weekly/{ds}.tsv"
    return None

def generate_date_list(start_date="2023-07-01"):
    """Generate a list of months from start_date to the previous month."""
    current_date = datetime.now()
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    date_list = []
    while start_date < current_date.replace(day=1):
        date_list.append(start_date.strftime("%Y-%m"))
        start_date += timedelta(days=30)  # Add 30 days to move to the next month
    return sorted(list(set(date_list)))

def fetch_and_merge_data(dates, type="monthly"):
    """Fetch and merge data from the dataset URLs."""
    masterdf = pd.DataFrame()
    for ds in dates:
        try:
            url = get_url(ds, type)
            if url:
                df = pd.read_csv(url, delimiter='\t', names=cols, on_bad_lines='warn')
                masterdf = pd.concat([masterdf, df], axis=0, ignore_index=True)
                print(f"Data for {ds} successfully added.")
        except Exception as e:
            print(f"Error processing {ds}: {e}")
    return masterdf
