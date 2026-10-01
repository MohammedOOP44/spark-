from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, from_json, lag, unix_timestamp, lead
from pyspark.sql.window import Window

the_spark = (
    SparkSession.builder
    .appName("ClickStreamFunnelAnalysis")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4")
    .getOrCreate() 
) 

the_spark.sparkContext.setLogLevel("WARN")

payload_schema = StructType(
    [
        StructField("product_id",StringType(),True),
        StructField("price",DoubleType(),True),
        StructField("referrer",StringType(),True)
    ]
)

log_schema = StructType(
    [
        StructField("event_id",StringType(),False),
        StructField("user_id",StringType(),False),
        StructField("event_type",StringType(),False),
        StructField("timestamp",StringType(),False),
        StructField("attributes",payload_schema,True)
    ]
)

raw_json_data = [
    ('{"event_id": "E101", "user_id": "U1", "event_type": "view", "timestamp": "2026-09-28 10:00:00", "attributes": {"product_id": "P10", "price": 100.0, "referrer": "google"}}',),
    ('{"event_id": "E102", "user_id": "U1", "event_type": "view", "timestamp": "2026-09-28 10:00:02", "attributes": {"product_id": "P10", "price": 100.0, "referrer": "google"}}',), # Rapid duplicate
    ('{"event_id": "E103", "user_id": "U1", "event_type": "add_to_cart", "timestamp": "2026-09-28 10:02:00", "attributes": {"product_id": "P10", "price": 100.0, "referrer": "direct"}}',),
    ('{"event_id": "E104", "user_id": "U1", "event_type": "purchase", "timestamp": "2026-09-28 10:05:00", "attributes": {"product_id": "P10", "price": 100.0, "referrer": "checkout"}}',),
    ('{"event_id": "E105", "user_id": "U2", "event_type": "view", "timestamp": "2026-09-28 10:10:00", "attributes": {"product_id": "P20", "price": 50.0, "referrer": "facebook"}}',),
    ('{"event_id": "E106", "user_id": "U2", "event_type": "add_to_cart", "timestamp": "2026-09-28 10:12:00", "attributes": {"product_id": "P20", "price": 50.0, "referrer": "direct"}}',), # Drop-off (No Purchase)
    ('{"event_id": "E107", "user_id": "U3", "event_type": "view", "timestamp": "2026-09-28 10:15:00", "attributes": {"product_id": "P30", "price": 300.0, "referrer": "google"}}',) # Drop-off (View only)
]

df_raw = the_spark.createDataFrame(raw_json_data,["raw_value"])

df_parsed = df_raw.withColumn("data", from_json(col("raw_value"),log_schema)) \
    .select(
        col("data.event_id").alias("event_id"),
        col("data.user_id").alias("user_id"),
        col("data.event_type").alias("event_type"),
        col("data.timestamp").cast(TimestampType).alias("event_time"),
        col("data.attributes.product_id").alias("product_id"),
        col("data.attributes.price").alias("price")
    )

print("--- parsed structure log data ---")
df_parsed.show(truncate=False)
 
user_time_window = Window.partitionBy("user_id","event_type","product_id").orderBy("event_time")

df_deduped = df_parsed.withColumn(
    "prev_event_time", 
    lag("event_time",1).over(user_time_window) 
).withColumn(
    "time_delta_sec", 
    unix_timestamp(col("event_time")) - unix_timestamp(col("prev_event_time"))
).filter(
    col("time_delta_sec").isNull() | (col("time_delta_sec") > 5)
).drop("prev_event_time","time_delta_sec")

df_journey_window = Window.partitionBy("user_id").orderBy("event_time")

df_journey = df_deduped.withColumn(
    "next_event_type",
    lead(col("event_type"), 1).over(df_journey_window) 
).withColumn(
    "next_event_time",
    lead(col("event_time"), 1).over(df_journey_window)
).withColumn(
    "time_to_next_action_sec", 
    unix_timestamp(col("next_event_time")) - unix_timestamp(col("event_time"))
)
