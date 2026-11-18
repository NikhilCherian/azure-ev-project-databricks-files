# =============================================================================
# LAYER 3a — GOLD Streaming Mart
# 5-minute tumbling window aggregation per vehicle + station.
# Grain: one row per (vehicle_id, station_id, 5-minute window).
# MERGEs on (vehicle_id, station_id, window_start) so late-arriving data
# within the window updates rather than duplicates the row.
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
@dp.table(
    name="gold_charging_progress_live_event_hub",
    comment="5-min tumbling window charging progress per vehicle+station. Updates as Silver events arrive.",
    table_properties={
        "quality":                        "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.targetFileSize":           "134217728",   # 128 MB target file size
    },
)
def gold_charging_progress_live_event_hub():
    return (
        dp.read_stream("silver_vehicle_battery_events_dlt_event_hub")
        .groupBy(
            F.col("vehicle_id"),
            F.col("station_id"),
            F.window(F.col("event_ts_ts"), "5 minutes"),
        )
        .agg(
            F.min("battery_pct")                     .alias("battery_pct_start"),
            F.max("battery_pct")                     .alias("battery_pct_end"),
            F.round(F.avg("battery_pct"), 2)         .alias("battery_pct_avg"),
            F.round(F.avg("charging_rate_kw"), 2)    .alias("avg_charging_rate_kw"),
            F.round(F.avg("battery_temp_c"), 2)      .alias("avg_temp_c"),
            F.round(F.max("battery_temp_c"), 2)      .alias("max_temp_c"),
            # Overtemperature: any reading above 45°C in this window
            F.max(F.when(F.col("battery_temp_c") > 45.0, 1).otherwise(0))
             .cast(BooleanType())                    .alias("overtemp_flag"),
            F.min("estimated_minutes_to_full")       .alias("eta_minutes_to_full"),
            F.first("session_id")                    .alias("session_id"),
            F.first("charger_id")                    .alias("charger_id"),
            F.count("*")                             .alias("event_count"),
        )
        .select(
            "vehicle_id", "station_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end")  .alias("window_end"),
            "session_id", "charger_id",
            "battery_pct_start", "battery_pct_end", "battery_pct_avg",
            "avg_charging_rate_kw", "avg_temp_c", "max_temp_c",
            "overtemp_flag", "eta_minutes_to_full", "event_count",
        )
    )
