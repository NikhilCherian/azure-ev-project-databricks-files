# =============================================================================
# LAYER 3b — GOLD Batch Mart (read from Silver, not streaming)
# Daily revenue proxy: total kWh delivered per station per day.
# Grain: one row per (station_id, session_date).
# This is a materialized view that DLT refreshes on each pipeline trigger.
#
# kWh proxy formula:
#   charging_rate_kw × (1 second / 3600) × event_count
#   = approximate energy delivered in the sampling window
# =============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(
    name="gold_daily_station_energy",
    comment="Daily kWh delivered per station — batch mart refreshed on each pipeline run.",
    table_properties={
        "quality":   "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_daily_station_energy():
    return (
        # dlt.read() (not read_stream) → batch snapshot of Silver at trigger time
        dp.read("silver_vehicle_battery_events")
        .groupBy("station_id", "session_date")
        .agg(
            F.countDistinct("vehicle_id")                            .alias("unique_vehicles"),
            F.countDistinct("session_id")                           .alias("unique_sessions"),
            F.round(
                F.sum(F.col("charging_rate_kw") / 3600.0), 4
            )                                                       .alias("total_kwh_delivered"),
            F.round(F.avg("charging_rate_kw"), 2)                  .alias("avg_charging_rate_kw"),
            F.round(F.avg("battery_temp_c"), 2)                    .alias("avg_temp_c"),
            F.sum(
                F.when(F.col("battery_temp_c") > 45.0, 1).otherwise(0)
            )                                                       .alias("overtemp_event_count"),
            F.count("*")                                            .alias("total_event_count"),
            F.min("event_ts_ts")                                    .alias("first_event_ts"),
            F.max("event_ts_ts")                                    .alias("last_event_ts"),
        )
        .withColumn("pipeline_updated_at", F.current_timestamp())
    )
