from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum


spark = SparkSession.builder \
    .appName("Agrégation des produits par catégorie") \
    .getOrCreate()

file_path = "produits.csv"

df = spark.read.option("header", "true") \
    .option("delimiter",",") \
    .option("inferSchema", "true") \
    .csv(file_path)

resultat = df.groupBy("Catégorie").agg(
    avg("Prix").alias("Prix Moyen"),
    sum("Prix").alias("Prix Total")
    )

resultat.show()
spark.stop()