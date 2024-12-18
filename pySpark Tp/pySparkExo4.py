from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col


spark = SparkSession.builder \
    .appName("Produits les plus chers") \
    .getOrCreate()

file_path = "produits.csv"

df = spark.read.option("header", "true") \
    .option("delimiter",",") \
    .option("inferSchema", "true") \
    .csv(file_path)


trois_produits_plus_chers = df.orderBy(col("Prix").desc()).limit(3)

trois_produits_plus_chers.show()

spark.stop()