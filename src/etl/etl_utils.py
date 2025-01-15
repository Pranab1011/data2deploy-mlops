import time
from psycopg2.extras import execute_batch
import pandas as pd
import requests
import io
from datetime import datetime, timedelta
from etl.etl_config import *
import psycopg2
from tqdm import tqdm
import calendar
import os


url = source_url
host = os.getenv("DB_HOST")
port = "5432"
dbname = "data_dev"
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")


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
        "$where": f"created_date < '{start_timestamp}' AND closed_date > '{start_timestamp}' AND closed_date < '{end_timestamp}'",
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
        time.sleep(5)

    return pd.concat(all_data, axis=0).drop("location", axis=1)


def write_to_postgres(
    df, create_table_query, insert_query
):
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    cursor = conn.cursor()

    cursor.execute(create_table_query)
    conn.commit()

    # Insert data
    for _, row in tqdm(df.iterrows()):
        cursor.execute(
            insert_query,
            (
                row["unique_key"],  # BIGINT
                row["created_date"],  # VARCHAR(255)
                row["closed_date"],  # VARCHAR(255)
                row["agency"],  # VARCHAR(255)
                row["agency_name"],  # VARCHAR(255)
                row["complaint_type"],  # VARCHAR(255)
                row["location_type"],  # VARCHAR(255)
                row["incident_zip"],  # DOUBLE PRECISION
                row["city"],  # VARCHAR(100)
                row["facility_type"],  # DOUBLE PRECISION
                row["status"],  # VARCHAR(255)
                row["due_date"],  # VARCHAR(255)
                row["borough"],  # VARCHAR(255)
                row["park_borough"],  # VARCHAR(50)
                row["vehicle_type"],  # DOUBLE PRECISION
                row["taxi_company_borough"],  # VARCHAR(255)
                row["road_ramp"],  # VARCHAR(50)
                row["bridge_highway_segment"],  # VARCHAR(255)
                row["latitude"],  # DOUBLE PRECISION
                row["longitude"],  # DOUBLE PRECISION
            ),
        )
    conn.commit()

    cursor.close()
    conn.close()


def backfill_90days():
    today = datetime.now()
    last_day_of_previous_month = calendar.monthrange(
        today.year, today.month - 1 if today.month > 1 else 12
    )[1]
    prev_month = today.month - 1 if today.month > 1 else 12
    prev_year = today.year if today.month > 1 else today.year - 1
    end_timestamp = (
        f"{prev_year}-{prev_month:02d}-{last_day_of_previous_month:02d}T00:00:00.000"
    )

    df = fetch_multiple_days(end_timestamp, 90)

    write_to_postgres(
        df,
        host,
        port
    )


def monthly_insert():
    today = datetime.now()
    last_day_of_previous_month = calendar.monthrange(
        today.year, today.month - 1 if today.month > 1 else 12
    )[1]
    prev_month = today.month - 1 if today.month > 1 else 12
    prev_year = today.year if today.month > 1 else today.year - 1
    end_timestamp = (
        f"{prev_year}-{prev_month:02d}-{last_day_of_previous_month:02d}T00:00:00.000"
    )

    df = fetch_multiple_days(end_timestamp, 30)

    write_to_postgres(
        df,
        host,
        port
    )


def monthly_upsert():
    today = datetime.now()
    last_day_of_previous_month = calendar.monthrange(
        today.year, today.month - 1 if today.month > 1 else 12
    )[1]
    first_day_of_current_month = datetime(today.year, today.month, 1)

    prev_month = today.month - 1 if today.month > 1 else 12
    prev_year = today.year if today.month > 1 else today.year - 1
    end_timestamp = (
        f"{prev_year}-{prev_month:02d}-{last_day_of_previous_month:02d}T00:00:00.000"
    )

    first_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    first_day_formatted = first_day_of_previous_month.strftime("%Y-%m-%dT00:00:00.000")

    df = fetch_upsert(start_timestamp=first_day_formatted, end_timestamp=end_timestamp)

    data_to_insert = df.to_records(index=False).tolist()
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )

    with conn.cursor() as cursor:
        execute_batch(cursor, upsert_query, data_to_insert)
        conn.commit()

    print("Upsert completed successfully.")
    conn.close()
