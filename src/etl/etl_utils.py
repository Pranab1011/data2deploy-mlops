import pandas as pd
import requests
import io
from datetime import datetime, timedelta
from etl.etl_config import *
import psycopg2


url = source_url


def fetch_between_timestamps(start_timestamp, end_timestamp):
    params = {
        "$where": f"created_date > '{start_timestamp}' AND created_date < '{end_timestamp}'",
        "$limit": 50000,  # Max rows per request
        "$order": "created_date ASC",  # Sort to process data sequentially
    }
    response = requests.get(url, params=params)
    return pd.read_csv(io.StringIO(response.text), low_memory=False)


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
    return pd.read_csv(io.StringIO(response.text), low_memory=False)


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

        print(
            f"fetching for start date: {start_timestamp} and end date: {end_timestamp}"
        )
        fetched_data = fetch_between_timestamps(start_timestamp, end_timestamp)
        print(fetched_data.shape[0])

        all_data.append(fetched_data)

    return pd.concat(all_data, axis=0)


def write_to_postgres(
    df, host, port, dbname, user, password, create_table_query, insert_query
):
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    cursor = conn.cursor()

    cursor.execute(create_table_query)
    conn.commit()

    # Insert data
    for _, row in df.iterrows():
        cursor.execute(
            insert_query,
            (
                row["unique_key"],
                row["created_date"],
                row["closed_date"],
                row["agency"],
                row["agency_name"],
                row["complaint_type"],
                row["descriptor"],
                row["location_type"],
                row["incident_zip"],
                row["incident_address"],
                row["street_name"],
                row["cross_street_1"],
                row["cross_street_2"],
                row["intersection_street_1"],
                row["intersection_street_2"],
                row["address_type"],
                row["city"],
                row["landmark"],
                row["facility_type"],
                row["status"],
                row["due_date"],
                row["resolution_description"],
                row["resolution_action_updated_date"],
                row["community_board"],
                row["bbl"],
                row["borough"],
                row["x_coordinate_state_plane"],
                row["y_coordinate_state_plane"],
                row["open_data_channel_type"],
                row["park_facility_name"],
                row["park_borough"],
                row["vehicle_type"],
                row["taxi_company_borough"],
                row["taxi_pick_up_location"],
                row["bridge_highway_name"],
                row["bridge_highway_direction"],
                row["road_ramp"],
                row["bridge_highway_segment"],
                row["latitude"],
                row["longitude"],
                row["location"],
            ),
        )
    conn.commit()

    cursor.close()
    conn.close()
