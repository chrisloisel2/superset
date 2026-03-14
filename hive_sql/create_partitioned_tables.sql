-- =============================================================================
-- Hive External Tables for HDFS Session Data
-- Structure: partitioned by session_id
-- Data location: hdfs://namenode:9000/hive/sessions/<table>/session_id=<id>/
-- Note: `timestamp` is backtick-escaped (reserved keyword in Hive)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS robotics;
USE robotics;

-- -----------------------------------------------------------------------------
-- Table 1: tracker_positions
-- Source: tracker_positions.csv (25 columns)
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS robotics.tracker_positions (
    `timestamp`     STRING,
    time_seconds    DOUBLE,
    timestamp_ns    BIGINT,
    frame_number    INT,
    tracker_1_x     DOUBLE,
    tracker_1_y     DOUBLE,
    tracker_1_z     DOUBLE,
    tracker_1_qw    DOUBLE,
    tracker_1_qx    DOUBLE,
    tracker_1_qy    DOUBLE,
    tracker_1_qz    DOUBLE,
    tracker_2_x     DOUBLE,
    tracker_2_y     DOUBLE,
    tracker_2_z     DOUBLE,
    tracker_2_qw    DOUBLE,
    tracker_2_qx    DOUBLE,
    tracker_2_qy    DOUBLE,
    tracker_2_qz    DOUBLE,
    tracker_3_x     DOUBLE,
    tracker_3_y     DOUBLE,
    tracker_3_z     DOUBLE,
    tracker_3_qw    DOUBLE,
    tracker_3_qx    DOUBLE,
    tracker_3_qy    DOUBLE,
    tracker_3_qz    DOUBLE
)
PARTITIONED BY (session_id STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- -----------------------------------------------------------------------------
-- Table 2: pince1_data
-- Source: pince1_data.csv (8 columns)
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS robotics.pince1_data (
    `timestamp`     STRING,
    time_seconds    DOUBLE,
    timestamp_ns    BIGINT,
    t_ms            BIGINT,
    pince_id        STRING,
    sw              STRING,
    ouverture_mm    DOUBLE,
    angle_deg       DOUBLE
)
PARTITIONED BY (session_id STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- -----------------------------------------------------------------------------
-- Table 3: pince2_data
-- Source: pince2_data.csv (8 columns, same schema as pince1_data)
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS robotics.pince2_data (
    `timestamp`     STRING,
    time_seconds    DOUBLE,
    timestamp_ns    BIGINT,
    t_ms            BIGINT,
    pince_id        STRING,
    sw              STRING,
    ouverture_mm    DOUBLE,
    angle_deg       DOUBLE
)
PARTITIONED BY (session_id STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Verify creation
SHOW TABLES IN robotics;
