from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, countDistinct, dense_rank, col
from pyspark.sql.window import Window
import os
os.environ["JAVA_HOME"] = r"C:\Users\SwordLake\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
spark = SparkSession.builder.appName("SellerRanking").getOrCreate()

order_items_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Order_Items.csv")

revenue_df = order_items_df.withColumn(
    "Revenue",
    col("Price") + col("Freight_Value")
)

seller_stats = revenue_df.groupBy("Seller_ID") \
    .agg(
        sum("Revenue").alias("Total_Revenue"),
        countDistinct("Order_ID").alias("Total_Orders")
    )

window_spec = Window.orderBy(
    col("Total_Revenue").desc(),
    col("Total_Orders").desc()
)

ranked_df = seller_stats.withColumn(
    "Seller_Rank",
    dense_rank().over(window_spec)
)

result = ranked_df.orderBy("Seller_Rank")

result.toPandas().to_csv("10.csv", index=False)

result.show()

spark.stop()
