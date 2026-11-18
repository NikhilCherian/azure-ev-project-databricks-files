# LAYER 3c — GOLD Overtemperature Alert View
# Materialised view: stations with overtemp events in the last 24 hours.
# Downstream: Power BI alert dashboard, Azure Monitor webhook.
# =============================================================================

from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.view(
    name="gold_overtemp_alerts_24h",
    comment="Vehicles with any battery_temp_c > 45°C in the past 24 hours. Feeds alert dashboard.",
)
def gold_overtemp_alerts_24h():
    return (
        dp.read("gold_charging_progress_live")
        .filter(F.col("overtemp_flag") == True)         # noqa: E712
        .filter(
            F.col("window_start") >= F.date_sub(F.current_timestamp().cast("date"), 1)
        )
        .select(
            "vehicle_id", "station_id", "session_id",
            "window_start", "window_end",
            "max_temp_c", "avg_temp_c",
            "battery_pct_start", "battery_pct_end",
            "event_count",
        )
        .orderBy(F.col("max_temp_c").desc())
    )