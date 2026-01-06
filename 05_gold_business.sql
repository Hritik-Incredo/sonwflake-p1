USE DATABASE PORTS_DB;
USE SCHEMA GOLD;


-- creating dim container
CREATE OR REPLACE TABLE DIM_CONTAINER AS
SELECT
    DISTINCT container_id
FROM
    SILVER.CONTAINER_MOVEMENT_TRANSFORMED_CLEAN;

    
-- create dim shipment
CREATE OR REPLACE TABLE DIM_SHIPMENT AS
SELECT
    DISTINCT shipment_id,
    vessel_name,
    carrier,
    origin_port,
    destination_port,
    voyage_no
FROM
    SILVER.SHIPMENT_DETAILS_TRANSFORMED_CLEAN;

    
-- create dim customs
CREATE OR REPLACE TABLE DIM_TERMINAL AS
SELECT
    DISTINCT terminal_id
FROM
    SILVER.CONTAINER_MOVEMENT_TRANSFORMED_CLEAN;


-- fact container
CREATE OR REPLACE TABLE FACT_CONTAINER_MOVEMENT AS
SELECT
    c.container_id,
    c.shipment_id,
    c.terminal_id,
    c.arrival_time,
    c.departure_time,
    c.dwell_hours,
    c.weight_kg,
    s.carrier,
    s.vessel_name,
    s.origin_port,
    s.destination_port,
    cu.import_export,
    cu.clearance_status,
    cu.clearance_time
FROM
    SILVER.CONTAINER_MOVEMENT_TRANSFORMED_CLEAN c
    LEFT JOIN SILVER.SHIPMENT_DETAILS_TRANSFORMED_CLEAN s ON c.shipment_id = s.shipment_id
    LEFT JOIN SILVER.CUSTOMS_DATA_TRANSFORMED_CLEAN cu ON c.container_id = cu.container_id;
