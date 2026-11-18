
from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

EH_NAMESPACE = "evh-ev-intelligence-dev02-nich"
EH_TOPIC     = "vehicle-battery-live"
EH_BOOTSTRAP = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
EH_CONN_STR  = dbutils.secrets.get(scope="ev-kv-scope", key="eventhub-vehicle-battery-listen-conn-str")
ADLS_ACCOUNT  = dbutils.secrets.get(scope="ev-kv-scope", key="adls-account-name")
conn_str = EH_CONN_STR.strip()

# Databricks uses a shaded Kafka — must use kafkashaded prefix for the LoginModule
SASL_CONFIG = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="$ConnectionString" '
    f'password="{conn_str}";'
)

BRONZE_PATH     = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/event-stream-dlt/vehicle_battery_live_dlt/"
CHECKPOINT_PATH = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/_checkpoints/vehicle-battery-live-dlt/"

EVENT_SCHEMA = StructType([
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
])
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":      EH_BOOTSTRAP,
    "subscribe":                    EH_TOPIC,
    "kafka.security.protocol":      "SASL_SSL",
    "kafka.sasl.mechanism":         "PLAIN",
    "kafka.sasl.jaas.config":       SASL_CONFIG,
    "startingOffsets":              "latest",
    "maxOffsetsPerTrigger":         1000,
    "failOnDataLoss":               "false",
    "kafka.request.timeout.ms":     "60000",
    "kafka.session.timeout.ms":     "60000",
}

@dp.table(name="bronze_delta_table_vehicle_battery_live_dlt")
def bronze_delta_table_vehicle_battery_live_dlt():
    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    parsed = (
    raw_stream
    .withColumn("value_str", F.col("value").cast("string"))
    .withColumn("parsed",    F.from_json(F.col("value_str"), EVENT_SCHEMA))
    .select(
        F.col("parsed.*"),
        F.col("timestamp").alias("eh_enqueued_time"),
        F.col("partition").alias("eh_partition"),
        F.col("offset")   .alias("eh_offset"),
        F.col("topic")    .alias("eh_topic"),
    ))
    

    return(
    parsed
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source",       F.lit("eventhub/vehicle-battery-live-dlt"))
    .withColumn("_is_corrupt",   F.col("event_id").isNull())
    .withColumn("event_date",    F.to_date(F.col("event_ts")))
)