import pandas as pd
import requests
import io
from datetime import datetime, timedelta
from etl_config import *


url = source_url


def fetch_between_timestamps(start_timestamp, end_timestamp):
    params = {
        "$where": f"created_date > '{start_timestamp}' AND created_date < '{end_timestamp}'",
        "$limit": 50000,  # Max rows per request
        "$order": "created_date ASC",  # Sort to process data sequentially
    }
    response = requests.get(url, params=params)
    return pd.read_csv(io.StringIO(response.text))


def fetch_upsert(start_timestamp, end_timestamp):
    end_datetime = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
    start_closed_datetime = end_datetime - timedelta(days=30)
    start_closed_timestamp = start_closed_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    upsert_params = {
        "$where": f"created_date > '{start_timestamp}' AND closed_date > '{start_closed_timestamp}' AND closed_date < '{end_timestamp}'",
        "$limit": 50000,  # Max rows per request
        "$order": "created_date ASC",  # Sort to process data sequentially
    }

    response = requests.get(url, params=upsert_params)
    return pd.read_csv(io.StringIO(response.text))


def fetch_one_day(end_timestamp):
    end_datetime = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
    start_datetime = end_datetime - timedelta(days=1)
    start_timestamp = start_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    return fetch_between_timestamps(start_timestamp, end_timestamp)


def fetch_multiple_days(end_timestamp, n_days=10):
    end_datetime = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
    all_data = []

    for lookback in range(n_days):
        end_datetime = end_datetime - timedelta(days=lookback)
        start_datetime = end_datetime - timedelta(days=lookback + 1)
        start_timestamp = start_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        end_timestamp = end_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

        all_data.append(fetch_between_timestamps(start_timestamp, end_timestamp))

    return pd.concat(all_data, axis=0)
