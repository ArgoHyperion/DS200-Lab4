from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import os
os.environ["JAVA_HOME"] = r"C:\Users\SwordLake\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.19.10-hotspot"

spark = SparkSession.builder.appName("OrdersByCountry").getOrCreate()

customers_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Customer_List.csv")

orders_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Orders.csv")

joined_df = orders_df.join(customers_df, on="Customer_Trx_ID", how="inner")

result = joined_df.groupBy("Customer_Country") \
    .agg(count("Order_ID").alias("Total_Orders")) \
    .orderBy("Total_Orders", ascending=False)

result.toPandas().to_csv("3.csv", index=False)

result.show()
spark.stop()