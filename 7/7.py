from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    avg,
    col,
    expr
)
import os
os.environ["JAVA_HOME"] = r"C:\Users\SwordLake\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
spark = SparkSession.builder.appName("TopProductsAndReviews").getOrCreate()

order_items_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Order_Items.csv")

orders_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Orders.csv")

reviews_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", False) \
    .csv("../Order_Reviews.csv")

products_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Products.csv")

reviews_df = reviews_df.withColumn(
    "Review_Score_Int",
    expr("try_cast(Review_Score as int)")
)

clean_reviews_df = reviews_df.filter(
    col("Review_Score_Int").isNotNull()
).filter(
    (col("Review_Score_Int") >= 1) &
    (col("Review_Score_Int") <= 5)
)

sales_df = order_items_df.groupBy("Product_ID") \
    .agg(
        count("Order_Item_ID").alias("Total_Sold")
    )

product_review_df = order_items_df.join(
    orders_df,
    on="Order_ID",
    how="inner"
).join(
    clean_reviews_df,
    on="Order_ID",
    how="inner"
)

review_stats_df = product_review_df.groupBy("Product_ID") \
    .agg(
        avg("Review_Score_Int").alias("Average_Review_Score")
    )

result = sales_df.join(
    review_stats_df,
    on="Product_ID",
    how="left"
).join(
    products_df.select(
        "Product_ID",
        "Product_Category_Name"
    ),
    on="Product_ID",
    how="left"
).orderBy(
    col("Total_Sold").desc()
)

result.show()

result.toPandas().to_csv("7.csv", index=False)

spark.stop()