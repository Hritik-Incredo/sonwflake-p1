USE DATABASE PORTS_DB;
USE SCHEMA BRONZE;

-- creating bronze tables
CREATE OR REPLACE TABLE BRONZE.CONTAINER_MOVEMENT_RAW(
    container_id STRING,
    shipment_id STRING,
    terminal_id STRING,
    arrival_time STRING,
    departure_time STRING,
    status STRING,
    gate_id STRING,
    crane_id STRING,
    weight STRING
);


CREATE OR REPLACE TABLE BRONZE.SHIPMENT_DETAILS_RAW(
    shipment_id STRING,
    vessel_name STRING,
    carrier STRING,
    origin_port STRING,
    destination_port STRING,
    eta STRING,
    etd STRING,
    voyage_no STRING
);


CREATE OR REPLACE TABLE BRONZE.CUSTOMS_DATA_RAW(
    container_id STRING,
    import_export STRING,
    clearance_status STRING,
    clearance_time STRING,
    officer_id STRING,
    tariff_code STRING
);


