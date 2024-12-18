from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col


spark = SparkSession.builder \
    .appName("Nettoyage des données clients") \
    .getOrCreate()

file_path = "clients.csv"

df = spark.read.option("header", "true") \
    .option("delimiter",",") \
    .csv(file_path)

print("Données Avant Nettoyage :")
df.show()

age_moyen = df.select(mean("Âge").alias("age_moyen")).collect()[0]["age_moyen"]
df = df.fillna({"Âge": age_moyen})
df = df.fillna({"Ville": "Inconnue"})
df = df.filter(df["Revenu"].isNotNull())

print("Données Apres Nettoyage :")
df.show()

spark.stop()