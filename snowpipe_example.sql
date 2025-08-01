-- Set database and schema context
use database til_data_engineering;
use schema jt_deng3_staging;

-- Create a table to store raw ingested data
CREATE OR REPLACE TABLE snowpipe_test_table 
(
    raw variant
);

-- Create a Snowpipe to automate ingestion from the external stage using defined JSON file format
CREATE OR REPLACE PIPE jt_amplitude_events_pipe AUTO_INGEST = TRUE
AS
COPY INTO snowpipe_test_table
FROM @TIL_DATA_ENGINEERING.JT_DENG3_STAGING.PYTHON_IMPORT_STAGE
FILE_FORMAT = (FORMAT_NAME = jts_json_format);

-- Preview the data ingested by the Snowpipe
select * from snowpipe_test_table;

-- List all pipes in current schema to verify pipe creation and status
show pipes;
