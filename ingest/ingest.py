from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
import os

datalake_path = 'datalake'

landing_zone_path = f"{datalake_path}/landing"
landing_zone_stream_path = f"{datalake_path}/landing/usage_events_stream"

print(f"Landing zone path: {landing_zone_path}")
print(f"Landing zone stream path: {landing_zone_stream_path}")

bronze_batch_path = f"{datalake_path}/bronze/batch_data"
bronze_stream_path = f"{datalake_path}/bronze/usage_events"
checkpoint_location = f"{datalake_path}/checkpoints/usage_events_cp"
quarantine_path = f"{datalake_path}/quarantine/usage_events_errors"
quarantine_checkpoint = f"{datalake_path}/checkpoints/quarantine_cp"

print(f"Path Landing (CSV): {landing_zone_path}")
print(f"Path Landing (Streaming JSONL): {landing_zone_stream_path}")
print(f"Path destino Bronze (Batch): {bronze_batch_path}")
print(f"Path destino Bronze (Streaming): {bronze_stream_path}")
print(f"Ruta de destino Bronze (Streaming OK): {bronze_stream_path}")
print(f"Ruta de destino Cuarentena (Errores): {quarantine_path}")

spark = SparkSession.builder\
    .appName("Big Data")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("\nSpark Session inicializada.")


#Parseo de todos los archivos csv del directorio montado
files = os.listdir(landing_zone_path)
batch_files = []
for file in files:
  if '.csv' in file:
    batch_files.append(file)
print(batch_files)


# Ingesta Batch a Bronze
# Dedupe por event_id
# Marcas de ingesta (ingest_ts, source_file)
csv_dataframes = {}
for file in batch_files:
  csv_df = spark.read \
      .format("csv") \
      .option("header", "true") \
      .option("inferSchema", "true") \
      .load(f"{landing_zone_path}/{file}")
  columns_map = {
      "ingest_ts": current_timestamp(),
      "source_file": lit(file)
  }
  csv_df = csv_df.withColumns(columns_map) #Añadir marcas de ingesta
  csv_dataframes[file] = csv_df

print(f"\nCSV files loaded successfully. Rows: {csv_df.count()}")
print("Schema del DataFrame Batch (CSV):")
for csv_df in csv_dataframes.values():
  csv_df.printSchema()
  csv_df.select("*").limit(5).show()

#Write to Bronze Parquet Batch
csv_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("ingest_ts") \
    .save(bronze_batch_path)
#Particionado por timestamp -> cada archivo se va a procesar en un timestamp distinto (uniqueness), y si se quiere filtrar eventualmente por fecha para limpieza por ejemplo, se puede

print(f"\nProceso Batch completado: Datos CSV escritos en Bronze Parquet en {bronze_batch_path}")

# Ingesta Streaming a Bronze
# ==========================

stream_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("timestamp", TimestampType(), True),
    StructField("org_id", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("service", StringType(), True),
    StructField("region", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("cost_usd_increment", DoubleType(), True),
    StructField("schema_version", IntegerType(), True),
    StructField("carbon_kg", DoubleType(), True),
    StructField("genai_tokens", IntegerType(), True)
])

jsonl_stream_df = spark.readStream \
    .format("json") \
    .schema(stream_schema) \
    .load(f"{landing_zone_stream_path}")

print("\nStructured Streaming inicializado para leer JSONL.")

processed_stream_df = jsonl_stream_df \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["event_id"]) \
    .withColumns({'ingest_ts': current_timestamp(), 'source_file': input_file_name()})

processed_stream_df = processed_stream_df.withColumn("date", date_format(col("ingest_ts"), "yyyyMMdd"))

print("Schema of processed_stream_df after adding date:")
processed_stream_df.printSchema()

print("Watermark y deduplicación configurados (10 min de latencia).")
processed_stream_df.printSchema()

COST_ANOMALY_THRESHOLD = 5.00
COST_ANOMALY_PERCENTILE = 0.99

quality_rules_df = processed_stream_df.selectExpr("*",
    # [event_id no nulo]
    "event_id IS NOT NULL as is_event_id_valid",

    # [cost_usd_increment >= -0.01] y [flag de anomalía si > umbral]
    "cost_usd_increment >= -0.01 as is_cost_range_valid",
    f"cost_usd_increment > {COST_ANOMALY_THRESHOLD} as is_cost_anomaly",

    # [unit no nulo cuando value no nulo]
    "CASE WHEN value IS NOT NULL THEN unit IS NOT NULL ELSE TRUE END as is_unit_valid",

    # [schema_version manejada y compatibilizada] (asumimos 1 o 2 son válidos)
    "schema_version IN (1, 2) as is_schema_version_valid"
    # Verificar que esten las columnas de la v2 tambien en la v1
)

quarantine_flagged_df = quality_rules_df.withColumn("is_quarantine",
    ~col("is_event_id_valid") | ~col("is_cost_range_valid") | ~col("is_unit_valid") | ~col("is_schema_version_valid")
)

good_data_df = quarantine_flagged_df.filter(col("is_quarantine") == False)
quarantine_df = quarantine_flagged_df.filter(col("is_quarantine") == True)

query_good = good_data_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", bronze_stream_path) \
    .option("checkpointLocation", checkpoint_location) \
    .partitionBy("date") \
    .trigger(once=True) \
    .start()

query_quarantine = quarantine_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", quarantine_path) \
    .option("checkpointLocation", quarantine_checkpoint) \
    .partitionBy("date") \
    .trigger(once=True) \
    .start()

print("Esperando a que finalice la ejecución única de ambos Streams (Parquet Sink)...")
query_good.awaitTermination()
query_quarantine.awaitTermination()

print(f"\nProceso Streaming completado: ")
print(f"- Datos válidos escritos en Bronze Parquet en {bronze_stream_path}")
print(f"- Datos en cuarentena escritos en Parquet en {quarantine_path}")

spark.stop()