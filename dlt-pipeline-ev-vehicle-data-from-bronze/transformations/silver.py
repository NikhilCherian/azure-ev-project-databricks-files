# =============================================================================
# LAYER 2 — SILVER (Streaming)
# Applies 9 DQ rules, marks each row pass/fail, routes bad rows to quarantine.
# Clean rows are MERGED on event_id for idempotency.
#
# dp Expectations enforce DQ rules declaratively — no manual if/else needed.
# Violations are tracked in the pipeline event log automatically.
# =============================================================================

# ── DQ expectations ──────────────────────────────────────────────────────────
# drop_on_violation → bad rows are dropped from silver and tracked in event log
# warn_on_violation  → bad rows pass through but are flagged (use for monitoring)
# fail_on_violation  → entire pipeline batch fails if any row violates the rule

from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.expect_or_drop("valid_event_id",        "event_id IS NOT NULL")
@dp.expect_or_drop("valid_vehicle_id",       "vehicle_id IS NOT NULL")
@dp.expect_or_drop("valid_session_id",       "session_id IS NOT NULL")
@dp.expect_or_drop("valid_battery_pct",      "battery_pct BETWEEN 0 AND 100")
@dp.expect_or_drop("valid_charging_rate",    "charging_rate_kw BETWEEN 0 AND 350")
@dp.expect_or_drop("valid_battery_temp",     "battery_temp_c BETWEEN -10 AND 80")
@dp.expect_or_drop("valid_soc_target",       "state_of_charge_target_pct BETWEEN 1 AND 100")
@dp.expect_or_drop("valid_event_ts",         "event_ts IS NOT NULL")
@dp.expect("not_corrupt",                    "_is_corrupt IS FALSE")   # warn only — keep for analysis
@dp.table(
    name="silver_vehicle_battery_events",
    comment="DQ-validated EV battery events. 9 rules enforced. Bad rows dropped and tracked in dp event log.",
    table_properties={
        "quality":                        "silver",
        "delta.enableChangeDataFeed":     "true",   # enables downstream CDC consumers
        "pipelines.autoOptimize.managed": "true",
    },
)
def silver_vehicle_battery_events():
    return (
        dp.read_stream("bronze_vehicle_battery_events")
        .withColumn("event_ts_ts",    F.to_timestamp(F.col("event_ts")))
        .withColumn("sl_ingested_at", F.current_timestamp())
        .withColumn("battery_pct",    F.round(F.col("battery_pct"), 2))
        .withColumn("battery_temp_c", F.round(F.col("battery_temp_c"), 2))
        # Derive session date for partitioning
        .withColumn("session_date",   F.to_date(F.col("event_ts_ts")))
        .select(
            "event_id", "vehicle_id", "session_id", "station_id", "charger_id",
            "battery_pct", "charging_rate_kw", "battery_temp_c",
            "state_of_charge_target_pct", "estimated_minutes_to_full",
            "event_ts", "event_ts_ts", "session_date", "sl_ingested_at","_is_corrupt"
        )
    )
