# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a97c725f-e62b-42a6-81be-79df51bc6de5",
# META       "default_lakehouse_name": "lh_ec_bronze",
# META       "default_lakehouse_workspace_id": "24b23f62-544e-4c50-b957-cbf70bb32ba6",
# META       "known_lakehouses": [
# META         {
# META           "id": "a97c725f-e62b-42a6-81be-79df51bc6de5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.conf.set("spark.sql.session.timeZone", "UTC")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_yahoo_api_response (
    batch_id STRING,
    run_id STRING,
    source STRING,
    request_scope STRING,
    request_page INT,
    observed_at TIMESTAMP,
    http_status INT,
    source_updated_at_raw STRING,
    source_period_start_raw STRING,
    source_period_end_raw STRING,
    record_count BIGINT,
    response_body STRING
) USING DELTA
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_rakuten_api_response (
    batch_id STRING,
    run_id STRING,
    source STRING,
    request_scope STRING,
    request_page INT,
    observed_at TIMESTAMP,
    http_status INT,
    source_updated_at_raw STRING,
    source_period_start_raw STRING,
    source_period_end_raw STRING,
    record_count BIGINT,
    response_body STRING
) USING DELTA
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS ops_collection_run (
    batch_id STRING,
    run_id STRING,
    source STRING,
    request_scope STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status STRING,
    last_http_status INT,
    received_pages INT,
    record_count BIGINT,
    error_code STRING,
    bronze_stored BOOLEAN
) USING DELTA
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
