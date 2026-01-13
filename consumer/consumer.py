from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

# 1️⃣ Créer une session Spark
spark = SparkSession.builder \
    .appName("TicketConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️⃣ Définir le schéma des tickets
ticket_schema = StructType([
    StructField("ticket_id", IntegerType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("request", StringType(), True),
    StructField("request_type", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("padding", StringType(), True)
])

# 3️⃣ Lire les données depuis Redpanda
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4️⃣ Parser le JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), ticket_schema).alias("data")
).select("data.*")

# 5️⃣ Ajouter la colonne assigned_team
transformed_df = parsed_df.withColumn(
    "assigned_team",
    when(col("request_type") == "support", "Team Support")
    .when(col("request_type") == "billing", "Team Finance")
    .when(col("request_type") == "incident", "Team DevOps")
    .otherwise("Team General")
)

# 6️⃣ Fonction pour écrire chaque batch
def write_batch(batch_df, batch_id):
    # Écrire tous les tickets transformés
    if batch_df.count() > 0:
        batch_df.write.mode("append").json("/output/tickets")
        
        # Calculer et écrire les stats par type
        stats_type = batch_df.groupBy("request_type").count()
        stats_type.write.mode("overwrite").json("/output/stats_by_type")
        
        # Calculer et écrire les stats par priorité
        stats_priority = batch_df.groupBy("priority").count()
        stats_priority.write.mode("overwrite").json("/output/stats_by_priority")
        
        print(f"✅ Batch {batch_id}: {batch_df.count()} tickets traités")

# 7️⃣ Écrire avec foreachBatch
query = transformed_df.writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "/output/checkpoint") \
    .start()

# 8️⃣ Garder le script actif
query.awaitTermination()