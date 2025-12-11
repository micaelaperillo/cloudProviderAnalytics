from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, countDistinct, when, lit, to_date, to_timestamp
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import os
import shutil

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
silver_path_usage_events_clean = f"{silver_path}/usage_events_clean"
silver_path_billing_monthly_clean = f"{silver_path}/billing_monthly_clean"
silver_path_support_tickets_clean = f"{silver_path}/support_tickets_clean"
silver_path_batch = f"{silver_path}/batch_data"

files = os.listdir(landing_zone_path)
batch_files = [f for f in files if f.endswith('.csv')]
print(batch_files)


#=============== Clean bronze before writing ==================
silver_paths = [silver_path, silver_path_usage_events_clean, silver_path_billing_monthly_clean, silver_path_support_tickets_clean, silver_path_batch]
for path in silver_paths:
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
            print(f"✓ Cleaned: {path}")
        except Exception as e:
            print(f"✗ Error cleaning {path}: {e}")
    else:
        print(f"- Path does not exist (will be created): {path}")

print("Iniciando proceso de creación de capa Silver...")

# --------------------------------------------------------------------------------
# SILVER: Procesar datos BATCH (CSV)
# --------------------------------------------------------------------------------

# Leer batch de Bronze
bronze_dataframes = {}
for filename in batch_files:
    print(f"Processing batch file: {filename}")
    columns_map = {
      "source_file": lit(filename)
    }
    batch_df = spark.read.parquet(f'{bronze_batch_path}/source_file={filename}') \
        .withColumns(columns_map)
    bronze_dataframes[filename] = batch_df

print(f"Batch data loaded: {len(bronze_dataframes)} DataFrames")

# Aquí puedes agregar limpiezas específicas según los CSV que tengas
# Por ejemplo, si tienes billing_monthly.csv, support_tickets.csv, etc.

# Identificar qué archivos tienes en batch y procesarlos
# Asumiendo que necesitas separar por tipo de fuente

# Para usage_events en batch (si los tienes)
# Nota: Los CSV no tienen el mismo esquema que los JSONL, así que solo procesamos JSONL como usage_events


#TODO estas cosas medio que no hacen nada?????
# osea se le agrega columna bnilling date pero ya existe
# mismo con sla_breached -> ya esxiste la col y se pone en true si estaba en true??

# Para billing (si está en CSV)
billing_df = bronze_dataframes["billing_monthly.csv"]

billing_silver = billing_df \
    .withColumns({ 
        "subtotal_usd": col("subtotal")*col("exchange_rate_to_usd"),
        "credits_usd": col("credits")*col("exchange_rate_to_usd"),
        "taxes_usd": col("taxes")*col("exchange_rate_to_usd")
    }) \
    .dropDuplicates()

billing_silver.write \
    .mode("overwrite") \
    .parquet(silver_path_billing_monthly_clean)

print("✓ Billing data processed to Silver")


# Para support_tickets (si está en CSV)
support_tickets_df = bronze_dataframes['support_tickets.csv'].filter(col("ticket_id").isNotNull())

tickets_silver = support_tickets_df \
    .withColumn("created_at", to_date(col("created_at"))) \
    .dropDuplicates(["ticket_id"])
tickets_silver.show(5)
tickets_silver.write \
    .mode("overwrite") \
    .parquet(f"{silver_path}/support_tickets_clean")

print("✓ Support tickets processed to Silver")


# --------------------------------------------------------------------------------
# SILVER: Procesar datos STREAMING (JSONL)
# --------------------------------------------------------------------------------

# Load dimension tables from batch
orgs_filename = 'customers_orgs.csv'
users_filename = 'users.csv'
resources_filename = 'resources.csv'

#TODO se borran los ingest_ts antes de joinsear con stream data
# Por que aca se borra en lugar de directamente no agregarla en ingest?
# nos quedamos unicamente con ingest_ts del streaming
orgs_df = bronze_dataframes[orgs_filename].filter(col("org_id").isNotNull() & col("org_name").isNotNull()).select("*").drop("ingest_ts").drop("source_file")
resources_df = bronze_dataframes[resources_filename].filter(col("resource_id").isNotNull()).select("*").drop("ingest_ts").drop("source_file")

#TODO que onda el join con users??? los usage_events no tienen nada que ver con users, no tienen user_id
# users_df = bronze_dataframes[users_filename].filter(col("user_id").isNotNull()).select("*").drop("ingest_ts")

# Leer streaming de Bronze
stream_df = spark.read.parquet(bronze_stream_path)

# Join events with dimensions
usage_events_enriched = stream_df \
    .join(orgs_df, on="org_id", how="left") \
    .join(resources_df, on=["org_id", "resource_id", "region", "service"], how="left")


#  .join(users_df, on="user_id") \
# TODO join con users
# los eventos no tienen user_id, y cada org_id puede tener varios users asociados

# Limpiar y conformar datos de streaming
# completo con defaults las entradas que no tengan valores validos de genai_tokens y carbon_kg
usage_events_silver = usage_events_enriched \
    .withColumn("usage_date", to_date(col("timestamp"))) \
    .withColumn("genai_tokens",
                when((col("service") == "genai") & (col("schema_version") == 2),
                     col("genai_tokens"))
                .otherwise(lit(0))) \
    .withColumn("carbon_kg",
                when(col("schema_version") == 2, col("carbon_kg"))
                .otherwise(lit(0.0))) \
    .withColumn("requests",
        when(col("metric") == "requests", col("value")).otherwise(0)
    ) \
    .withColumn("cpu_hours",
        when(col("metric") == "cpu_hours", col("value")).otherwise(0)
    ) \
    .withColumn("storage_gb_hours",
        when(col("metric") == "storage_gb_hours", col("value")).otherwise(0)
    ) \
    .dropDuplicates(["event_id"]) 
    # .filter(col("event_id").isNotNull()) # Lo comento porque ya se valida en bronze/quarentine

# Datos agregados por dia (features calucladas)
daily_usage_silver = usage_events_silver.groupBy(
    "org_id", "service", "usage_date"
).agg(
    sum("cost_usd_increment").alias("daily_cost_usd"),
    sum("requests").alias("total_requests"),
    sum("cpu_hours").alias("total_cpu_hours"),
    sum("storage_gb_hours").alias("total_storage_gb_hours"),
    sum("genai_tokens").alias("total_genai_tokens"),
    sum("carbon_kg").alias("total_carbon_kg"),
    count("*").alias("event_count")
).withColumn("last_updated", F.current_timestamp())

# Guardar en Silver
usage_events_silver.write \
    .mode("overwrite") \
    .partitionBy("usage_date") \
    .parquet(f"{silver_path}/usage_events_clean")

daily_usage_silver.write \
    .mode("overwrite") \
    .partitionBy("usage_date") \
    .parquet(f"{silver_path}/daily_usage")

print(f"✓ Usage events processed to Silver: {usage_events_silver.count()} rows")
usage_events_silver.printSchema()
print(f"✓ Daily usage events processed to Silver: {daily_usage_silver.count()} rows")
daily_usage_silver.printSchema()


# Escribir batch procesados de bronze a silver
#TODO por ahora no les hicimos nada, pero para que se pudan levantar desde silver los dejamos ahi
for filename, df in bronze_dataframes.items():
    
    df.write \
        .mode("append") \
        .format("parquet") \
        .partitionBy("source_file", "ingest_ts") \
        .save(silver_path_batch)

    print(f"✓ {filename} written to Silver")