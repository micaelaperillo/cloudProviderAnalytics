from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, countDistinct, when, lit, to_date
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder\
    .appName("Big Data")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("\nSpark Session inicializada.")

# Rutas
datalake_path = '/content/drive/MyDrive/Big Data - Final/datalake'
bronze_batch_path = f"{datalake_path}/bronze/batch_data"
bronze_stream_path = f"{datalake_path}/bronze/usage_events"
silver_path = f"{datalake_path}/silver"

print("Iniciando proceso de creación de capa Silver...")

# --------------------------------------------------------------------------------
# SILVER: Procesar datos BATCH (CSV)
# --------------------------------------------------------------------------------

# Leer batch de Bronze
batch_df = spark.read.parquet(bronze_batch_path)

print(f"Batch data loaded: {batch_df.count()} rows")
print("Batch schema:")
batch_df.printSchema()

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

# Para support_tickets (si está en CSV)
if 'ticket_id' in batch_df.columns:
    tickets_df = batch_df.filter(col("ticket_id").isNotNull())

    tickets_silver = tickets_df \
        .withColumn("created_at", to_date(col("created_at"))) \
        .withColumn("sla_breached",
                    when(col("sla_breached").isin(["true", "True", "1", 1, True]), True)
                    .otherwise(False)) \
        .dropDuplicates(["ticket_id"])

    tickets_silver.write \
        .mode("overwrite") \
        .parquet(f"{silver_path}/support_tickets_clean")

    print("✓ Support tickets processed to Silver")


# --------------------------------------------------------------------------------
# SILVER: Procesar datos STREAMING (JSONL)
# --------------------------------------------------------------------------------

# Leer streaming de Bronze
stream_df = spark.read.parquet(bronze_stream_path)

print(f"Stream data loaded: {stream_df.count()} rows")
print("Stream schema:")
stream_df.printSchema()

# Limpiar y conformar datos de streaming
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