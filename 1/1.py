from pyspark.sql import SparkSession
import os

os.environ["JAVA_HOME"] = r"C:\Users\SwordLake\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.19.10-hotspot"

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
spark.sparkContext.setLogLevel("FATAL")

customer_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Customer_List.csv")

order_items_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Order_Items.csv")

order_reviews_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Order_Reviews.csv")

orders_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Orders.csv")

products_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .csv("../Products.csv")

schemas = {
    "Customer_List": customer_df,
    "Order_Items": order_items_df,
    "Order_Reviews": order_reviews_df,
    "Orders": orders_df,
    "Products": products_df
}

with open("1.txt", "w", encoding="utf-8") as f:
    for name, df in schemas.items():
        f.write(f"{name}.csv\n")
        f.write(df._jdf.schema().treeString())
        f.write("\n\n")

spark.stop()