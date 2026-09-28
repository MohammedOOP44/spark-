from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.functions import col, sum as _sum 

myspark = (
    SparkSession.builder
    .appName("YO SPARK")
    .master("local[*]")
    .getOrCreate()
)

myspark.sparkContext.setLogLevel("WARN")

myschema = StructType(
    [
        StructField("transaction_id",StringType,False),
        StructField("user_id",StringType,False),
        StructField("amount",DoubleType,True),
        StructField("status",StringType,True)
    ]
)

mydata = [
    ("TXN_101", "USER_A", 150.50, "COMPLETED"),
    ("TXN_102", "USER_B", 200.00, "COMPLETED"),
    ("TXN_103", "USER_A", 50.25, "CANCELLED"),
    ("TXN_104", "USER_C", 300.00, "COMPLETED"),
    ("TXN_105", "USER_B", 120.00, "COMPLETED"),
    ("TXN_106", "USER_A", 80.00, "COMPLETED")
]

df = myspark.createDataFrame(mydata,myschema)

filtered_df = df.filter(col("status")=="COMPLETED")

grouped_df = filtered_df.groupBy("user_id").agg(_sum("amount").alias("total_spent"))

print("--- User Total Spending (Completed Transactions) ---")
grouped_df.show()

print("--- Execution Plan (Catalyst Output) ---")
grouped_df.explain(True)

myspark.stop()

