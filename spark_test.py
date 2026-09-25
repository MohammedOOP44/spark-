from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum 
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

spark = (
    SparkSession.builder
    .appName("DataEngineeringFoundations")
    .master("local[*]")
    .getOrCreate() 
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("transaction_id",StringType(),False),
        StructField("user_id", StringType(), False),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True)
    ]
)

data = [
    ("TXN_101", "USER_A", 150.50, "COMPLETED"),
    ("TXN_102", "USER_B", 200.00, "COMPLETED"),
    ("TXN_103", "USER_A", 50.25, "CANCELLED"),
    ("TXN_104", "USER_C", 300.00, "COMPLETED"),
    ("TXN_105", "USER_B", 120.00, "COMPLETED"),
    ("TXN_106", "USER_A", 80.00, "COMPLETED")
]

df = spark.createDataFrame(data,schema=schema)

filtered_df = df.filter(col("status")=="COMPLETED")

grouped_df = filtered_df.groupBy("user_id").agg(_sum("amount").alias("total_spent"))

print("--- User Total Spending (Completed Transactions) ---")
grouped_df.show()

print("--- Execution Plan (Catalyst Output) ---")
grouped_df.explain(True)

spark.stop()