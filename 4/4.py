from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count, col
import os
os.environ["JAVA_HOME"] = r"C:\Users\SwordLake\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
spark = SparkSession.builder.appName("OrdersByYearMonth").getOrCreate()

orders_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Orders.csv")

result = orders_df.withColumn(
    "Year", year(col("Order_Purchase_Timestamp"))
).withColumn(
    "Month", month(col("Order_Purchase_Timestamp"))
).groupBy(
    "Year", "Month"
).agg(
    count("Order_ID").alias("Total_Orders")
).orderBy(
    col("Year").asc(),
    col("Month").desc()
)

result.toPandas().to_csv("4.csv", index=False)
result.show()
spark.stop()