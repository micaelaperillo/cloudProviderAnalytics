from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, countDistinct, when, lit, to_date, date_format
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName("Big Data")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("\nSpark Session inicializada.")

# Rutas
datalake_path = 'datalake'
silver_path = f"{datalake_path}/silver"
silver_path_usage_events_clean = f"{silver_path}/usage_events_clean"
silver_path_daily_usage = f"{silver_path}/daily_usage"
silver_path_billing_monthly_clean = f"{silver_path}/billing_monthly_clean"
silver_path_support_tickets_clean = f"{silver_path}/support_tickets_clean"
silver_path_batch = f"{silver_path}/batch_data"

# ================================================================================
# REQUERIMIENTO 4: GOLD (MARTS DE NEGOCIO)
# ================================================================================

gold_path = f"{datalake_path}/gold"

# --------------------------------------------------------------------------------
# MART 1: FinOps - org_daily_usage_by_service
# --------------------------------------------------------------------------------

print("\n[1/5] Creando mart: org_daily_usage_by_service...")

daily_usage_silver = spark.read.parquet(silver_path_daily_usage)

org_daily_usage_by_service = daily_usage_silver.select(
    "org_id",
    "service",
    "usage_date",
    "daily_cost_usd",
    "total_requests",
    "total_cpu_hours",
    "total_storage_gb_hours",
    "total_genai_tokens",
    "total_carbon_kg",
    "last_updated"
)

org_daily_usage_by_service.write \
    .mode("overwrite") \
    .partitionBy("usage_date") \
    .parquet(f"{gold_path}/finops/org_daily_usage_by_service")


# print(f"✓ Mart creado: {org_daily_usage_by_service.count()} rows")

# # --------------------------------------------------------------------------------
# # MART 2: FinOps - revenue_by_org_month
# # --------------------------------------------------------------------------------
# print("\n[2/5] Creando mart: revenue_by_org_month...")

billing_silver = spark.read.parquet(f"{silver_path}/billing_monthly_clean")

revenue_by_org_month = billing_silver \
    .withColumn("month_and_year", date_format(col("month"), "yyyy-MM")) \
    .drop("month") \
    .groupBy("org_id", "month_and_year") \
    .agg(
        sum("subtotal_usd").alias("revenue_usd"),
        sum("credits_usd").alias("credits_usd"),
        sum("taxes_usd").alias("tax_usd"),
        avg("exchange_rate_to_usd").alias("fx_applied")
    )

revenue_by_org_month.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/finops/revenue_by_org_month")

print(f"✓ Mart creado: {revenue_by_org_month.count()} rows")

# except Exception as e:
#     print(f"⚠ Billing data no disponible, skipping revenue_by_org_month: {e}")

# --------------------------------------------------------------------------------
#  Query 3: critical_tickets_evolution_sla_rate_daily
# --------------------------------------------------------------------------------
support_tickets_df = spark.read.parquet(silver_path_support_tickets_clean)
# support_tickets_df.select('severity').show(500)


critical_tickets_evolution_sla_rate_daily = support_tickets_df \
    .filter(col("severity") == "critical") \
    .withColumn("date", to_date(col("created_at"))) \
    .withColumn("solved", when(col("resolved_at").isNotNull(), to_date(col("resolved_at")))) \
    .groupBy("date") \
    .agg(
        sum(when(col("sla_breached") == True, 1).otherwise(0)).alias("breach_count"),
        sum(when(col("solved").isNotNull(), 1).otherwise(0)).alias("solved_count"),
        count("*").alias("critical_ticket_count"),
    ) \
    .withColumn(
          "breach_rate",
          col("breach_count") / col("critical_ticket_count")
    ) \
    .orderBy("date")

critical_tickets_evolution_sla_rate_daily.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/finops/critical_tickets_evolution_sla_rate_daily")



# --------------------------------------------------------------------------------
#  Query 5: genai_tokens_cost_daily
# --------------------------------------------------------------------------------

silver_path_events_clean = spark.read.parquet(silver_path_usage_events_clean).select('*') \
.where((col("genai_tokens") > 0) & (col("service") != "genai") ) \
.show(10)
# genai_tokens_cost_daily = silver_path_events_clean \
#     .filter(col("service") == "genai") \
#     .groupBy("usage_date") \
#query3_df.select('*').where(col('breach_count') > 0).show(500)

# --------------------------------------------------------------------------------
#  Query 4: Revenue mensual con créditos/impuestos aplicados (normalizado a USD)
# --------------------------------------------------------------------------------

billing_df = spark.read.parquet(f"{silver_path}/billing_monthly_clean")

query4_df = (
    billing_df
        .groupBy("org_id", "month")
        .agg(
            sum("subtotal_usd").alias("revenue_usd"),
            sum("credits_usd").alias("credits_usd"),
            sum("taxes_usd").alias("tax_usd"),
            avg("exchange_rate_to_usd").alias("fx_applied")
        )
)

query4_df.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/finops/query4_df")

query4_df.select('*').where(col('revenue_usd') > 0).show(500)

# # --------------------------------------------------------------------------------
# # MART 3: FinOps - cost_anomaly_mart
# # --------------------------------------------------------------------------------
# print("\n[3/5] Creando mart: cost_anomaly_mart...")

# window_spec = Window.partitionBy("org_id", "service")

# cost_anomaly_mart = spark.read.parquet(f"{silver_path}/usage_events_clean") \
#     .groupBy("org_id", "usage_date", "service") \
#     .agg(sum("cost_usd_increment").alias("daily_cost")) \
#     .withColumn("mean_cost", F.avg("daily_cost").over(window_spec)) \
#     .withColumn("stddev_cost", F.stddev("daily_cost").over(window_spec)) \
#     .withColumn("z_score",
#                 when(col("stddev_cost") > 0,
#                      (col("daily_cost") - col("mean_cost")) / col("stddev_cost"))
#                 .otherwise(0)) \
#     .withColumn("is_anomaly",
#                 when(F.abs(col("z_score")) > 3, True).otherwise(False)) \
#     .withColumn("anomaly_score", F.abs(col("z_score"))) \
#     .withColumn("last_updated", F.current_timestamp()) \
#     .select("org_id", "usage_date", "service", "daily_cost",
#             "z_score", "anomaly_score", "is_anomaly", "last_updated")

# cost_anomaly_mart.write \
#     .mode("overwrite") \
#     .partitionBy("usage_date") \
#     .parquet(f"{gold_path}/finops/cost_anomaly_mart")

# print(f"✓ Mart creado: {cost_anomaly_mart.count()} rows")


# # --------------------------------------------------------------------------------
# # MART 5: Producto/Usage - genai_tokens_by_org_date
# # --------------------------------------------------------------------------------
# print("\n[5/5] Creando mart: genai_tokens_by_org_date...")

# COST_PER_1K_TOKENS = 0.002

# genai_tokens_by_org_date = spark.read.parquet(f"{silver_path}/usage_events_clean") \
#     .filter((col("service") == "genai") & (col("genai_tokens") > 0)) \
#     .groupBy("org_id", "usage_date") \
#     .agg(
#         sum("genai_tokens").alias("total_genai_tokens"),
#         sum("genai_cost_usd").alias("total_genai_cost_usd"),
#     )


# ================================================================================
# CONSULTAS CQL QUE HAY QUE RESPONDER CON LOS MARTS
# ================================================================================

#costos y request diarios por org y servicio en un rango de fechas
# CREATE TABLE daily_costs_requests_by_org_service (
#     org_id STRING,
#     usage_date DATE,
#     service STRING,
#     daily_cost_usd DOUBLE,
#     total_requests LONG,
#     cpu_hours DOUBLE,
#     storage_gb_hours DOUBLE,
#     genai_tokens LONG,
#     carbon_kg DOUBLE,
#     avg_cost_per_event DOUBLE,
#     last_updated TIMESTAMP
# )   

# CREATE TABLE critical_tickets_evolution_sla_rate_daily_last30days (
#     ticket_id STRING,
#     org_id STRING,
#     category STRING,
#     created_at TIMESTAMP,
#     resolved_at TIMESTAMP NULLABLE,
#     severity STRING,
#     sla_breached BOOLEAN,
# )   