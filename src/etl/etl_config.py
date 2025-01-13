source_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"

raw_table_name = "raw_test"
create_raw_table_query = f"""
    CREATE TABLE IF NOT EXISTS {raw_table_name} (
    unique_key BIGINT PRIMARY KEY,
    created_date TIMESTAMP,
    closed_date TIMESTAMP,
    agency VARCHAR(10),
    agency_name VARCHAR(255),
    complaint_type VARCHAR(255),
    descriptor VARCHAR(255),
    location_type VARCHAR(255),
    incident_zip VARCHAR(10),
    incident_address VARCHAR(255),
    street_name VARCHAR(255),
    cross_street_1 VARCHAR(255),
    cross_street_2 VARCHAR(255),
    intersection_street_1 VARCHAR(255),
    intersection_street_2 VARCHAR(255),
    address_type VARCHAR(50),
    city VARCHAR(100),
    landmark VARCHAR(255),
    facility_type VARCHAR(100),
    status VARCHAR(50),
    due_date TIMESTAMP,
    resolution_description TEXT,
    resolution_action_updated_date TIMESTAMP,
    community_board VARCHAR(50),
    bbl BIGINT,
    borough VARCHAR(50),
    x_coordinate_state_plane DOUBLE PRECISION,
    y_coordinate_state_plane DOUBLE PRECISION,
    open_data_channel_type VARCHAR(50),
    park_facility_name VARCHAR(255),
    park_borough VARCHAR(50),
    vehicle_type VARCHAR(50),
    taxi_company_borough VARCHAR(50),
    taxi_pick_up_location VARCHAR(255),
    bridge_highway_name VARCHAR(255),
    bridge_highway_direction VARCHAR(50),
    road_ramp VARCHAR(50),
    bridge_highway_segment VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    location GEOGRAPHY(Point, 4326)
);"""

insert_raw_table_query = f"""
        INSERT INTO {raw_table_name} (
            unique_key, created_date, closed_date, agency, agency_name, complaint_type, descriptor, 
            location_type, incident_zip, incident_address, street_name, cross_street_1, cross_street_2, 
            intersection_street_1, intersection_street_2, address_type, city, landmark, facility_type, status, 
            due_date, resolution_description, resolution_action_updated_date, community_board, bbl, borough, 
            x_coordinate_state_plane, y_coordinate_state_plane, open_data_channel_type, park_facility_name, 
            park_borough, vehicle_type, taxi_company_borough, taxi_pick_up_location, bridge_highway_name, 
            bridge_highway_direction, road_ramp, bridge_highway_segment, latitude, longitude, location
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                  ST_GeomFromText(%s, 4326));
        """
