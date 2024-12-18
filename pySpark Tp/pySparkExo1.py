from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max


spark = SparkSession.builder \
    .appName("Analyse des ventes") \
    .getOrCreate()

file_path = "ventes.csv"
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter",",") \
    .csv(file_path)

df = df.withColumn("Chiffre_d_affaires", col("Quantité") * col("Prix_unitaire"))

df.show()

total_revenue = df.agg(sum("Chiffre_d_affaires").alias("Chiffre_d_affaires_total")).collect()[0]["Chiffre_d_affaires_total"]
print(f"Chiffre d'affaires total : {total_revenue:.2f} €")

most_sold_product = df.groupBy("Produit") \
    .agg(sum("Quantité").alias("Quantité_totale")) \
    .orderBy(col("Quantité_totale").desc()) \
    .first()

print(f"Produit le plus vendu : {most_sold_product['Produit']} ({most_sold_product['Quantité_totale']} unités)")

spark.stop()

