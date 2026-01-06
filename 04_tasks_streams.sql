USE DATABASE PORTS_DB;

USE SCHEMA task_stream;

-- ingesting data from azure blob to bronze

CREATE OR REPLACE STREAM CONTAINER_MOVEMENT_RAW_STREAM
ON TABLE BRONZE.CONTAINER_MOVEMENT_RAW
APPEND_ONLY = TRUE;

CREATE OR REPLACE TASK CONTAINER_MOVEMENT_RAW_TASK
WAREHOUSE = PORTS_WH
SCHEDULE = '2 MINUTES'
AS
COPY INTO BRONZE.CONTAINER_MOVEMENT_RAW
FROM @BRONZE.SNOW_FILES
PATTERN = 'container_movement.*\.csv';





CREATE OR REPLACE STREAM SHIPMENT_DETAILS_RAW_STREAM
ON TABLE BRONZE.SHIPMENT_DETAILS_RAW
APPEND_ONLY = TRUE;

CREATE OR REPLACE TASK SHIPMENT_DETAILS_RAW_TASK
WAREHOUSE = PORTS_WH
SCHEDULE = '2 MINUTES'
AS
COPY INTO BRONZE.SHIPMENT_DETAILS_RAW
FROM @BRONZE.SNOW_FILES
PATTERN = 'shipment_details.*\.csv';





CREATE OR REPLACE STREAM CUSOTMS_DATA_RAW_STREAM
ON TABLE BRONZE.CUSTOMS_DATA_RAW
APPEND_ONLY = TRUE;


CREATE OR REPLACE TASK CUSTOMS_DATA_RAW_TASK
WAREHOUSE = PORTS_WH
SCHEDULE = '2 MINUTES'
AS
COPY INTO BRONZE.CUSTOMS_DATA_RAW
FROM @BRONZE.SNOW_FILES
PATTERN = '.*customs_data.*\.csv';







-- bronze raw to silver transformed
CREATE OR REPLACE TASK CONTAINER_MOVEMENT_TRANSFORMED_TASK
WAREHOUSE = PORTS_WH
AFTER CONTAINER_MOVEMENT_RAW_TASK
AS
INSERT INTO SILVER.CONTAINER_MOVEMENT_TRANSFORMED
SELECT
    container_id,
    shipment_id,
    terminal_id,
    TRY_TO_TIMESTAMP(arrival_time, 'YYYY-MM-DD HH24:MI:SS') AS arrival_time,
    TRY_TO_TIMESTAMP(departure_time, 'YYYY-MM-DD HH24:MI:SS') AS departure_time,
    UPPER(status) AS status,
    gate_id,
    crane_id,
    TRY_TO_NUMBER(weight) AS weight_kg,
    DATEDIFF(
        'hour',
        TRY_TO_TIMESTAMP(arrival_time, 'YYYY-MM-DD HH24:MI:SS'),
        TRY_TO_TIMESTAMP(departure_time, 'YYYY-MM-DD HH24:MI:SS')
    ) AS dwell_hours
FROM
    CONTAINER_MOVEMENT_RAW_STREAM;





CREATE OR REPLACE TASK SHIPMENT_DETAILS_TRANSFORMED_TASK
WAREHOUSE = PORTS_WH
AFTER SHIPMENT_DETAILS_RAW_TASK
AS
INSERT INTO SILVER.SHIPMENT_DETAILS_TRANSFORMED
SELECT
    shipment_id,
    vessel_name,
    carrier,
    origin_port,
    destination_port,
    TRY_TO_TIMESTAMP(eta, 'YYYY-MM-DD HH24:MI:SS') AS eta,
    TRY_TO_TIMESTAMP(etd, 'YYYY-MM-DD HH24:MI:SS') AS etd,
    voyage_no
FROM
    SHIPMENT_DETAILS_RAW_STREAM;





CREATE OR REPLACE TASK CUSTOMS_DATA_TRANSFORMED_TASK
WAREHOUSE = PORTS_WH
AFTER CUSTOMS_DATA_RAW_TASK
AS
INSERT INTO SILVER.CUSTOMS_DATA_TRANSFORMED
SELECT
    container_id,
    import_export,
    clearance_status,
    TRY_TO_TIMESTAMP(clearance_time, 'YYYY-MM-DD HH24:MI:SS') AS clearance_time,
    officer_id,
    tariff_code
FROM
    CUSTOMS_DATA_RAW_STREAM;







-- spliting silver data into error and clean

CREATE OR REPLACE STREAM CONTAINER_MOVEMENT_TRANSFORMED_STREAM
ON TABLE SILVER.CONTAINER_MOVEMENT_TRANSFORMED
APPEND_ONLY = TRUE;


CREATE OR REPLACE PROCEDURE CONTAINER_MOVEMENT_TRANSFORMED_STREAM_SPLIT()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
 
    INSERT ALL
        -- Error records
        WHEN container_id IS NULL
          OR shipment_id IS NULL
          OR arrival_time IS NULL
          OR departure_time IS NULL
          OR dwell_hours < 0
        THEN INTO SILVER.CONTAINER_MOVEMENT_TRANSFORMED_ERROR
        VALUES (
            container_id,
            shipment_id,
            terminal_id,
            arrival_time,
            departure_time,
            status,
            gate_id,
            crane_id,
            weight_kg,
            dwell_hours,
            CURRENT_TIMESTAMP,
            CASE
                WHEN container_id IS NULL THEN 'MISSING_CONTAINER_ID'
                WHEN arrival_time IS NULL THEN 'INVALID_ARRIVAL_TIME'
                WHEN departure_time IS NULL THEN 'INVALID_DEPARTURE_TIME'
                WHEN dwell_hours < 0 THEN 'NEGATIVE_DWELL_TIME'
                ELSE 'UNKNOWN'
            END
        )
 
        -- Clean records
        WHEN container_id IS NOT NULL
         AND shipment_id IS NOT NULL
         AND arrival_time IS NOT NULL
         AND departure_time IS NOT NULL
         AND dwell_hours >= 0
        THEN INTO SILVER.CONTAINER_MOVEMENT_TRANSFORMED_CLEAN
        VALUES (
            container_id,
            shipment_id,
            terminal_id,
            arrival_time,
            departure_time,
            status,
            gate_id,
            crane_id,
            weight_kg,
            dwell_hours
        )
 
    SELECT
        container_id,
        shipment_id,
        terminal_id,
        arrival_time,
        departure_time,
        status,
        gate_id,
        crane_id,
        weight_kg,
        dwell_hours
    FROM CONTAINER_MOVEMENT_TRANSFORMED_STREAM;
 
    RETURN 'SPLIT COMPLETED SUCCESSFULLY';
 
END;
$$;
 



CREATE OR REPLACE TASK CONTAINER_MOVEMENT_TRANSFORMED_SPLIT_TASK
WAREHOUSE = PORTS_WH
AFTER CONTAINER_MOVEMENT_TRANSFORMED_TASK
AS
CALL  CONTAINER_MOVEMENT_TRANSFORMED_STREAM_SPLIT();







CREATE OR REPLACE STREAM SHIPMENT_DETAILS_TRANSFORMED_STREAM
ON TABLE SILVER.SHIPMENT_DETAILS_TRANSFORMED
APPEND_ONLY = TRUE;


CREATE OR REPLACE PROCEDURE SHIPMENT_DETAILS_TRANSFORMED_STREAM_SPLIT()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
 
    INSERT ALL
 
        -- ERROR RECORDS
        WHEN shipment_id IS NULL
          OR eta IS NULL
          OR etd IS NULL
          OR etd < eta
        THEN INTO SILVER.SHIPMENT_DETAILS_TRANSFORMED_ERROR
        VALUES (
            shipment_id,
            vessel_name,
            carrier,
            origin_port,
            destination_port,
            eta,
            etd,
            voyage_no,
            CURRENT_TIMESTAMP,
            CASE
                WHEN shipment_id IS NULL THEN 'MISSING_SHIPMENT_ID'
                WHEN eta IS NULL THEN 'INVALID_ETA'
                WHEN etd IS NULL THEN 'INVALID_ETD'
                WHEN etd < eta THEN 'ETD_BEFORE_ETA'
                ELSE 'UNKNOWN'
            END
        )
 
        -- CLEAN RECORDS
        WHEN shipment_id IS NOT NULL
         AND eta IS NOT NULL
         AND etd IS NOT NULL
         AND etd >= eta
        THEN INTO SILVER.SHIPMENT_DETAILS_TRANSFORMED_CLEAN
        VALUES (
            shipment_id,
            vessel_name,
            carrier,
            origin_port,
            destination_port,
            eta,
            etd,
            voyage_no
        )
 
    SELECT
        shipment_id,
        vessel_name,
        carrier,
        origin_port,
        destination_port,
        eta,
        etd,
        voyage_no
    FROM SILVER.SHIPMENT_DETAILS_TRANSFORMED_STREAM;
 
    RETURN 'SPLIT COMPLETED SUCCESSFULLY';
 
END;
$$;
 


CREATE OR REPLACE TASK SHIPMENT_DETAILS_TRANSFORMED_SPLIT_TASK
WAREHOUSE = PORTS_WH
AFTER SHIPMENT_DETAILS_TRANSFORMED_TASK
AS
CALL  SHIPMENT_DETAILS_TRANSFORMED_STREAM_SPLIT();






CREATE OR REPLACE STREAM CUSTOMS_DATA_TRANSFORMED_STREAM
ON TABLE SILVER.CUSTOMS_DATA_TRANSFORMED
APPEND_ONLY = TRUE;

CREATE OR REPLACE PROCEDURE CUSTOMS_DATA_TRANSFORMED_STREAM_SPLIT()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
 
    INSERT ALL
 
        -- ERROR RECORDS
        WHEN container_id IS NULL
          OR clearance_time IS NULL
          OR clearance_status IS NULL
          OR UPPER(clearance_status) NOT IN ('CLEARED', 'HOLD', 'SCAN_REQUIRED')
        THEN INTO SILVER.CUSTOMS_DATA_TRANSFORMED_ERROR
        VALUES (
            container_id,
            import_export,
            clearance_status,
            clearance_time,
            officer_id,
            tariff_code,
            CURRENT_TIMESTAMP,
            CASE
                WHEN container_id IS NULL THEN 'MISSING_CONTAINER_ID'
                WHEN clearance_time IS NULL THEN 'INVALID_CLEARANCE_TIME'
                WHEN clearance_status IS NULL THEN 'MISSING_CLEARANCE'
                WHEN UPPER(clearance_status) NOT IN ('CLEARED', 'HOLD', 'SCAN_REQUIRED')
                     THEN 'UNKNOWN_CLEARANCE_STATUS'
                ELSE 'UNKNOWN_ERROR'
            END
        )
 
        -- CLEAN RECORDS
        WHEN container_id IS NOT NULL
         AND clearance_time IS NOT NULL
         AND clearance_status IS NOT NULL
         AND UPPER(clearance_status) IN ('CLEARED', 'HOLD', 'SCAN_REQUIRED')
        THEN INTO SILVER.CUSTOMS_DATA_TRANSFORMED_CLEAN
        VALUES (
            container_id,
            import_export,
            clearance_status,
            clearance_time,
            officer_id,
            tariff_code
        )
 
    SELECT
        container_id,
        import_export,
        clearance_status,
        clearance_time,
        officer_id,
        tariff_code
    FROM CUSTOMS_DATA_TRANSFORMED_STREAM;
 
    RETURN 'SPLIT COMPLETED SUCCESSFULLY';
 
END;
$$;
 



CREATE OR REPLACE TASK CUSTOMS_DATA_TRANSFORMED_SPLIT_TASK
WAREHOUSE = PORTS_WH
AFTER CUSTOMS_DATA_TRANSFORMED_TASK
AS
CALL  CUSTOMS_DATA_TRANSFORMED_STREAM_SPLIT();




-- creating streams on clean silver data

CREATE OR REPLACE STREAM CONTAINER_MOVEMENT_CLEANED
ON TABLE SILVER.CONTAINER_MOVEMENT_TRANSFORMED_CLEAN
APPEND_ONLY = TRUE;


CREATE OR REPLACE STREAM SHIPMENT_DETAILS_CLEANED
ON TABLE SILVER.SHIPMENT_DETAILS_TRANSFORMED_CLEAN
APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM CUSTOMS_DATA_CLEANED
ON TABLE SILVER.CUSTOMS_DATA_TRANSFORMED_CLEAN
APPEND_ONLY = TRUE;

--------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TASK DAILY_CONTAINER_THROUGHPUT_TASK
WAREHOUSE = PORTS_WH
AFTER CONTAINER_MOVEMENT_TRANSFORMED_SPLIT_TASK
AS
MERGE INTO GOLD.DAILY_CONTAINER_THROUGHPUT tgt
USING(
    SELECT
        terminal_id,
        DATE(event_time) AS event_date,
        count(*) AS container_count
    FROM SILVER.CONTAINER_MOVEMENT_TRANSFORMED_CLEAN
    WHERE event_time >= DATEADD('day', -1, CURRENT_DATE)
    GROUP BY terminal_id, DATE(event_time)
) src
ON tgt.terminal_id = src.terminal_id
AND tgt.event_date = src.event_date
WHEN MATCHED THEN
    UPDATE SET
        container_count = src.container_count,
        last_updated_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        terminal_id,
        event_date,
        container_count,
        last_updated_ts
    )
    VALUES (
        src.terminal_id,
        src.event_date,
        src.container_count,
        CURRENT_TIMESTAMP()
    );



 

CREATE OR REPLACE TASK BUILD_SHIPMENT_ON_TIME_PERFORMANCE_TASK
WAREHOUSE = PORTS_WH
AFTER SHIPMENT_DETAILS_TRANSFORMED_SPLIT_TASK
AS
MERGE INTO GOLD.SHIPMENT_ON_TIME_PERFORMANCE tgt
USING (
  SELECT
    DATE(actual_eta) AS shipment_date,
    COUNT(*) AS total_shipments,
    COUNT_IF(actual_eta <= planned_eta) AS on_time_shipments,
    COUNT_IF(actual_eta > planned_eta) AS delayed_shipments,
    ROUND(
      COUNT_IF(actual_eta <= planned_eta) * 100.0 / COUNT(*),
      2
    ) AS on_time_percentage
  FROM SILVER.SHIPMENT_DETAILS_CLEAN
  WHERE actual_eta >= DATEADD('day', -1, CURRENT_DATE)
  GROUP BY DATE(actual_eta)
) src
ON tgt.shipment_date = src.shipment_date
WHEN MATCHED THEN
  UPDATE SET
    total_shipments = src.total_shipments,
    on_time_shipments = src.on_time_shipments,
    delayed_shipments = src.delayed_shipments,
    on_time_percentage = src.on_time_percentage,
    last_updated_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT VALUES (
    src.shipment_date,
    src.total_shipments,
    src.on_time_shipments,
    src.delayed_shipments,
    src.on_time_percentage,
    CURRENT_TIMESTAMP()
  );
 


 

CREATE OR REPLACE TASK BUILD_CUSTOMS_CLEARANCE_METRICS_TASK
WAREHOUSE = PORTS_WH
AFTER CUSTOMS_DATA_TRANSFORMED_SPLIT_TASK
AS
MERGE INTO GOLD.CUSTOMS_CLEARANCE_METRICS tgt
USING (
  SELECT
    DATE(clearance_time) AS clearance_date,
    COUNT(*) AS total_containers,
    AVG(DATEDIFF('hour', arrival_time, clearance_time)) AS avg_clearance_hours,
    COUNT_IF(DATEDIFF('hour', arrival_time, clearance_time) > 24)
      AS delayed_containers
  FROM SILVER.CUSTOMS_DATA_CLEAN
  WHERE clearance_time >= DATEADD('day', -1, CURRENT_DATE)
  GROUP BY DATE(clearance_time)
) src
ON tgt.clearance_date = src.clearance_date
WHEN MATCHED THEN
  UPDATE SET
    total_containers = src.total_containers,
    avg_clearance_hours = src.avg_clearance_hours,
    delayed_containers = src.delayed_containers,
    last_updated_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT VALUES (
    src.clearance_date,
    src.total_containers,
    src.avg_clearance_hours,
    src.delayed_containers,
    CURRENT_TIMESTAMP()
  );

 
---------------------------------------------------------------------------------------------------------------------------------
ALTER TASK CUSTOMS_DATA_RAW_TASK SUSPEND;
ALTER TASK SHIPMENT_DETAILS_RAW_TASK SUSPEND;
ALTER TASK CONTAINER_MOVEMENT_RAW_TASK SUSPEND;


ALTER TASK DAILY_CONTAINER_THROUGHPUT_TASK RESUME;
ALTER TASK CONTAINER_MOVEMENT_TRANSFORMED_SPLIT_TASK RESUME;
ALTER TASK CONTAINER_MOVEMENT_TRANSFORMED_TASK RESUME;
ALTER TASK CONTAINER_MOVEMENT_RAW_TASK RESUME;

ALTER TASK BUILD_CUSTOMS_CLEARANCE_METRICS_TASK RESUME;
ALTER TASK CUSTOMS_DATA_TRANSFORMED_SPLIT_TASK RESUME;
ALTER TASK CUSTOMS_DATA_TRANSFORMED_TASK RESUME;
ALTER TASK CUSTOMS_DATA_RAW_TASK RESUME;


ALTER TASK BUILD_SHIPMENT_ON_TIME_PERFORMANCE_TASK RESUME;
ALTER TASK SHIPMENT_DETAILS_TRANSFORMED_SPLIT_TASK RESUME;
ALTER TASK SHIPMENT_DETAILS_TRANSFORMED_TASK RESUME;
ALTER TASK SHIPMENT_DETAILS_RAW_TASK RESUME;










SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'CONTAINER_MOVEMENT_RAW_TASK'
))
;




