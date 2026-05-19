from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
import os
os.environ["JAVA_HOME"] = r"C:\Users\SwordLake\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.19.10-hotspot"

spark = SparkSession.builder.appName("Statistics").getOrCreate()

orders_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Orders.csv")

order_items_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Order_Items.csv")

result = orders_df.agg(
    countDistinct("Order_ID").alias("Total_Orders"),
    countDistinct("Customer_Trx_ID").alias("Total_Customers")
)

seller_count = order_items_df.agg(
    countDistinct("Seller_ID").alias("Total_Sellers")
)

final_df = result.crossJoin(seller_count)
final_df.toPandas().to_csv("2.csv", index=False)
final_df.show()
spark.stop()