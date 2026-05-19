from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum, col
import os
os.environ["JAVA_HOME"] = r"C:\Users\SwordLake\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
spark = SparkSession.builder.appName("Revenue2024").getOrCreate()

orders_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Orders.csv")

order_items_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Order_Items.csv")

products_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Products.csv")

orders_2024 = orders_df.filter(
    year(col("Order_Purchase_Timestamp")) == 2024
)

joined_df = orders_2024.join(
    order_items_df,
    on="Order_ID",
    how="inner"
).join(
    products_df,
    on="Product_ID",
    how="inner"
)

revenue_df = joined_df.withColumn(
    "Revenue",
    col("Price") + col("Freight_Value")
)

result = revenue_df.groupBy("Product_Category_Name") \
    .agg(sum("Revenue").alias("Total_Revenue")) \
    .orderBy(col("Total_Revenue").desc())

result.toPandas().to_csv("6.csv", index=False)

result.show()

spark.stop()