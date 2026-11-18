# =============================================================================
# Delta Live Tables — EV Charging Intelligence Pipeline
# =============================================================================
# Pipeline : dlt_ev_charging_pipeline
# Project  : VoltGrid AU — EV Charging Franchise Intelligence Platform
#
# Layers:
#   Bronze  → raw vehicle battery events from Event Hub (streaming)
#   Silver  → DQ-validated, deduplicated events (streaming)
#   Gold    → 5-minute tumbling window charging progress mart (streaming)
#             + daily revenue-by-station aggregation (batch)
#
# How to run:
#   Databricks UI → Delta Live Tables → Create Pipeline
#   Source code   → this file
#   Pipeline mode → Triggered (batch) or Continuous (streaming)
#   Target schema → ev_catalog.dlt_ev_charging
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType, BooleanType
)

# =============================================================================
# CONFIGURATION
# =============================================================================
# In a real pipeline these come from pipeline parameters set in the UI / DAB
ADLS_ACCOUNT     = spark.conf.get("ev.adls_account", "evdatalakedevnich")
EH_NAMESPACE     = spark.conf.get("ev.eh_namespace",     "evh-ev-intelligence-dev02-nich")
EH_TOPIC         = spark.conf.get("ev.eh_topic",         "vehicle-battery-live")
EH_CONN_STR_KEY  = spark.conf.get("ev.eh_conn_str_key",  "ev-kv-scope:eh-conn-str")

BRONZE_PATH  = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/event-stream/vehicle_battery_live/"
SILVER_PATH  = f"abfss://silver@{ADLS_ACCOUNT}.dfs.core.windows.net/sl_vehicle_battery_live/"

# =============================================================================
# RAW EVENT SCHEMA
# Schema of JSON events produced by send_vehicle_battery_events.py
# =============================================================================
RAW_SCHEMA = StructType([
    StructField("event_id",                   StringType(),  True),
    StructField("vehicle_id",                 StringType(),  True),
    StructField("session_id",                 StringType(),  True),
    StructField("station_id",                 StringType(),  True),
    StructField("charger_id",                 StringType(),  True),
    StructField("battery_pct",                DoubleType(),  True),
    StructField("charging_rate_kw",           DoubleType(),  True),
    StructField("battery_temp_c",             DoubleType(),  True),
    StructField("state_of_charge_target_pct", IntegerType(), True),
    StructField("estimated_minutes_to_full",  IntegerType(), True),
    StructField("event_ts",                   StringType(),  True),
    StructField("_is_corrupt",                BooleanType(), True),
])

# =============================================================================
# LAYER 1 — BRONZE (Streaming)
# Reads raw JSON from the Bronze Delta path written by the Event Hub AutoLoader
# or Kafka consumer from Day 13.
#
# DLT handles:
#   - Checkpointing automatically
#   - Schema enforcement via the declared schema
#   - Quarantine of rows that cannot be parsed (rescued to _rescue_data)
# =============================================================================

@dp.table(
    name="bronze_vehicle_battery_events",
    comment="Raw EV battery telemetry events landed from Event Hub. Append-only. No transforms.",
    table_properties={
        "quality":                   "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def bronze_vehicle_battery_events():
    return (
        spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
    )

