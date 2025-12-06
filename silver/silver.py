from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, countDistinct, when, lit, to_date
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os

spark = SparkSession.builder\
    .appName("Big Data")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("\nSpark Session inicializada.")

# Rutas
# datalake_path = '/content/drive/MyDrive/Big Data - Final/datalake'
datalake_path = 'datalake'
landing_zone_path = f"{datalake_path}/landing"
bronze_batch_path = f"{datalake_path}/bronze/batch_data"
bronze_stream_path = f"{datalake_path}/bronze/usage_events"
silver_path = f"{datalake_path}/silver"

files = os.listdir(landing_zone_path)
batch_files = [f for f in files if f.endswith('.csv')]
print(batch_files)

print("Iniciando proceso de creación de capa Silver...")

# --------------------------------------------------------------------------------
# SILVER: Procesar datos BATCH (CSV)
# --------------------------------------------------------------------------------

# Leer batch de Bronze
bronze_dataframes = {}
for filename in batch_files:
    print(f"Processing batch file: {filename}")
    batch_df = spark.read.parquet(f'{bronze_batch_path}/source_file={filename}')
    bronze_dataframes[filename] = batch_df

for df in bronze_dataframes.values():
    df.show(2)
print(f"Batch data loaded: {len(bronze_dataframes)} DataFrames")

# Aquí puedes agregar limpiezas específicas según los CSV que tengas
# Por ejemplo, si tienes billing_monthly.csv, support_tickets.csv, etc.

# Identificar qué archivos tienes en batch y procesarlos
# Asumiendo que necesitas separar por tipo de fuente

# Para usage_events en batch (si los tienes)
# Nota: Los CSV no tienen el mismo esquema que los JSONL, así que solo procesamos JSONL como usage_events

# Para billing (si está en CSV)
if 'billing_monthly' in [f.lower() for f in batch_df.columns] or 'amount_usd' in batch_df.columns:
    billing_df = batch_df.filter(col("amount_usd").isNotNull())

    billing_silver = billing_df \
        .withColumn("billing_date", to_date(col("billing_date"))) \
        .dropDuplicates()

    billing_silver.write \
        .mode("overwrite") \
        .parquet(f"{silver_path}/billing_monthly_clean")

    print("✓ Billing data processed to Silver")


batch_df.show(5)

# Para support_tickets (si está en CSV)
# if 'ticket_id' in batch_df.columns:
    # tickets_df = batch_df.filter(col("ticket_id").isNotNull())

    # tickets_silver = tickets_df \
    #     .withColumn("created_at", to_date(col("created_at"))) \
    #     .withColumn("sla_breached",
    #                 when(col("sla_breached").cast("string").isin(["true", "True", "1"]), True)
    #                 .otherwise(False)) \
    #     .dropDuplicates(["ticket_id"])

    # tickets_silver.write \
    #     .mode("overwrite") \
    #     .parquet(f"{silver_path}/support_tickets_clean")

    # print("✓ Support tickets processed to Silver")


# --------------------------------------------------------------------------------
# SILVER: Procesar datos STREAMING (JSONL)
# --------------------------------------------------------------------------------

# Load dimension tables from batch
orgs_filename = 'customers_orgs.csv'
users_filename = 'users.csv'
resources_filename = 'resources.csv'

#TODO se borran los ingest_ts antes de joinsear con stream data
# nos quedamos unicamente con ingest_ts del streaming
orgs_df = bronze_dataframes[orgs_filename].filter(col("org_id").isNotNull() & col("org_name").isNotNull()).select("*").drop("ingest_ts")
users_df = bronze_dataframes[users_filename].filter(col("user_id").isNotNull()).select("*").drop("ingest_ts")
resources_df = bronze_dataframes[resources_filename].filter(col("resource_id").isNotNull()).select("*").drop("ingest_ts")

print("########sexo1")

# Leer streaming de Bronze
stream_df = spark.read.parquet(bronze_stream_path)

print("Stream schema:")
stream_df.printSchema()
print("Orgs schema:")
orgs_df.printSchema()
print("Resources schema:")
resources_df.printSchema()

# Join events with dimensions
usage_events_enriched = stream_df \
    .join(orgs_df, on="org_id", how="left")
print("joined orgs#################")
usage_events_enriched = usage_events_enriched \
    .join(resources_df, on=["org_id", "resource_id", "region", "service"], how="left")
print("joined resources#################")

#  .join(users_df, on="user_id") \
# TODO join con users
# los eventos no tienen user_id, y cada org_id puede tener varios users asociados

usage_events_enriched.write.mode("overwrite").parquet(f"{silver_path}/usage_events_clean")

print("########sexo2")

print(f"Stream data loaded: {stream_df.count()} rows")
print("Stream schema luego del join mistico!!!!!!!!:")
stream_df.printSchema()
stream_df.select('*').where(col("timestamp").isNull() | col("region").isNull() | col("service").isNull()).show(truncate=False)

# Limpiar y conformar datos de streaming
# completo con defaults las entradas que no tengan valores validos de genai_tokens y carbon_kg
usage_events_silver = stream_df \
    .withColumn("usage_date", to_date(col("timestamp"))) \
    .withColumn("genai_tokens",
                when((col("service") == "genai") & (col("schema_version") == 2),
                     col("genai_tokens"))
                .otherwise(lit(0))) \
    .withColumn("carbon_kg",
                when(col("schema_version") == 2, col("carbon_kg"))
                .otherwise(lit(0.0))) \
    .dropDuplicates(["event_id"]) \
    .filter(col("event_id").isNotNull())

# Guardar en Silver
usage_events_silver.write \
    .mode("overwrite") \
    .partitionBy("usage_date") \
    .parquet(f"{silver_path}/usage_events_clean")

print(f"✓ Usage events processed to Silver: {usage_events_silver.count()} rows")