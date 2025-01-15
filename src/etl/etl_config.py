source_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"

raw_table_name = "raw_test"
create_raw_table_query = f"""
    CREATE TABLE IF NOT EXISTS {raw_table_name} (
    unique_key BIGINT PRIMARY KEY,
    created_date VARCHAR(255),
    closed_date VARCHAR(255),
    agency VARCHAR(255),
    agency_name VARCHAR(255),
    complaint_type VARCHAR(255),
    location_type VARCHAR(255),
    incident_zip DOUBLE PRECISION,
    city VARCHAR(100),
    facility_type VARCHAR(255),
    status VARCHAR(255),
    due_date VARCHAR(255),
    borough VARCHAR(255),
    park_borough VARCHAR(50),
    vehicle_type VARCHAR(255),
    taxi_company_borough VARCHAR(255),
    road_ramp VARCHAR(50),
    bridge_highway_segment VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);"""

insert_raw_table_query = f"""
INSERT INTO {raw_table_name} (
    unique_key, created_date, closed_date, agency, agency_name, complaint_type, location_type, 
    incident_zip, city, facility_type, status, due_date, 
    borough, park_borough, vehicle_type, 
    taxi_company_borough, road_ramp, bridge_highway_segment, 
    latitude, longitude
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, 
    %s, %s
);
"""

upsert_query = f"""
INSERT INTO {raw_table_name} (
    unique_key, created_date, closed_date, agency, agency_name, complaint_type, location_type, 
    incident_zip, city, facility_type, status, due_date, 
    borough, park_borough, vehicle_type, 
    taxi_company_borough, road_ramp, bridge_highway_segment, 
    latitude, longitude
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, 
    %s, %s
) ON CONFLICT (unique_key) DO UPDATE SET
    created_date = EXCLUDED.created_date,
    closed_date = EXCLUDED.closed_date,
    agency = EXCLUDED.agency,
    agency_name = EXCLUDED.agency_name,
    complaint_type = EXCLUDED.complaint_type,
    location_type = EXCLUDED.location_type,
    incident_zip = EXCLUDED.incident_zip,
    city = EXCLUDED.city,
    facility_type = EXCLUDED.facility_type,
    status = EXCLUDED.status,
    due_date = EXCLUDED.due_date,
    borough = EXCLUDED.borough,
    park_borough = EXCLUDED.park_borough,
    vehicle_type = EXCLUDED.vehicle_type,
    taxi_company_borough = EXCLUDED.taxi_company_borough,
    road_ramp = EXCLUDED.road_ramp,
    bridge_highway_segment = EXCLUDED.bridge_highway_segment,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;
"""
