from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, count


spark = SparkSession.builder \
    .appName("Analyse des utilisateurs") \
    .getOrCreate()

file_path ="utilisateurs.json"

df = spark.read.option("inferSchema", "true").json(file_path)
df = df.filter(df["ville"].isNotNull())
age_moyen = df.agg(avg("âge").alias("âge_moyen")).collect()[0]["âge_moyen"]

print(f"Âge moyen des utilisateurs : {age_moyen:.2f} ans")

habitans_par_ville =  df.groupBy("ville").agg(count("id").alias("Nombre_d_utilisateurs"))

print("Nombre d'utilisateurs par ville :")

habitans_par_ville.show()

plus_jeune = df.orderBy("âge").first()

print(f"Plus jeune utilisateur : {plus_jeune["nom"]} ({plus_jeune["âge"]} ans)")

spark.stop()