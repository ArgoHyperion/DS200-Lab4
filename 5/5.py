import os
os.environ["JAVA_HOME"] = r"C:\Users\SwordLake\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, expr

spark = SparkSession.builder.appName("ReviewStatistics").getOrCreate()

reviews_df = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .option("inferSchema", False) \
    .csv("../Order_Reviews.csv")

reviews_df = reviews_df.withColumn(
    "Review_Score_Int",
    expr("try_cast(Review_Score as int)")
)

clean_df = reviews_df.filter(
    col("Review_Score_Int").isNotNull()
).filter(
    (col("Review_Score_Int") >= 1) &
    (col("Review_Score_Int") <= 5)
)

avg_score = clean_df.agg(
    avg("Review_Score_Int").alias("Average_Review_Score")
)

score_distribution = clean_df.groupBy("Review_Score_Int") \
    .agg(count("*").alias("Review_Count")) \
    .orderBy("Review_Score_Int")

avg_score.show()
score_distribution.show()
avg_score.toPandas().to_csv("5_average.csv", index=False)
score_distribution.toPandas().to_csv("5.csv", index=False)

spark.stop()