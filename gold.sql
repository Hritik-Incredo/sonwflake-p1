USE DATABASE PORTS_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE DAILY_CONTAINER_THROUGHPUT(
    terminal_id STRING,
    event_data  DATE,
    container_count NUMBER,
    last_updated_ts TIMESTAMP
);



CREATE OR REPLACE TABLE CUSTOMS_CLEARANCE_METRICS (
    clearance_date DATE,
    total_containers NUMBER,
    avg_clearance_hours NUMBER,
    delayed_containers NUMBER,
    last_updated_ts TIMESTAMP
);



CREATE OR REPLACE TABLE SHIPMENT_ON_TIME_PERFORMANCE(
    shipment_date DATE,
    total_shipments NUMBER,
    on_time_shipments NUMBER,
    delayed_shipments NUMBER,
    on_time_percentage NUMBER,
    last_updated_ts TIMESTAMP
);






