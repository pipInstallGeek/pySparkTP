from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col, sum


spark = SparkSession.builder \
    .appName("Analyse des transactions") \
    .getOrCreate()

file_path = "transactions.csv"

df = spark.read.option("header", "true") \
    .option("delimiter",",") \
    .option("inferSchema", "true") \
    .csv(file_path)

depenses_totales_par_client = df.groupBy("Client").agg(sum("Montant").alias("Dépenses_totales"))

client_max_depense = depenses_totales_par_client.orderBy(col("Dépenses_totales").desc()).limit(1)

print("Dépenses totales par client :")
depenses_totales_par_client.show()

print("Client ayant dépensé le plus :")
client_max_depense.show()

spark.stop()