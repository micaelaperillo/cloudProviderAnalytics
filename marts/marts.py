from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, countDistinct, when, lit, to_date
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Rutas
datalake_path = '/content/drive/MyDrive/Big Data - Final/datalake'
bronze_batch_path = f"{datalake_path}/bronze/batch_data"
bronze_stream_path = f"{datalake_path}/bronze/usage_events"
silver_path = f"{datalake_path}/silver"

# ================================================================================
# REQUERIMIENTO 4: GOLD (MARTS DE NEGOCIO)
# ================================================================================

print("\n" + "="*80)
print("CREANDO MARTS GOLD")
print("="*80)

gold_path = f"{datalake_path}/gold"

# --------------------------------------------------------------------------------
# MART 1: FinOps - org_daily_usage_by_service
# --------------------------------------------------------------------------------
print("\n[1/5] Creando mart: org_daily_usage_by_service...")

org_daily_usage_by_service = spark.read.parquet(f"{silver_path}/usage_events_clean") \
    .groupBy("org_id", "usage_date", "service") \
    .agg(
        sum("cost_usd_increment").alias("daily_cost_usd"),
        count("event_id").alias("total_requests"),
        sum(when(col("unit") == "hours", col("value")).otherwise(0)).alias("cpu_hours"),
        sum(when(col("unit") == "gb_hours", col("value")).otherwise(0)).alias("storage_gb_hours"),
        sum("genai_tokens").alias("genai_tokens"),
        sum("carbon_kg").alias("carbon_kg"),
        avg("cost_usd_increment").alias("avg_cost_per_event")
    ) \
    .withColumn("last_updated", F.current_timestamp())

org_daily_usage_by_service.write \
    .mode("overwrite") \
    .partitionBy("usage_date") \
    .parquet(f"{gold_path}/finops/org_daily_usage_by_service")

print(f"✓ Mart creado: {org_daily_usage_by_service.count()} rows")


# --------------------------------------------------------------------------------
# MART 2: FinOps - revenue_by_org_month
# --------------------------------------------------------------------------------
print("\n[2/5] Creando mart: revenue_by_org_month...")

# Verificar si existe billing data
try:
    billing_silver = spark.read.parquet(f"{silver_path}/billing_monthly_clean")

    revenue_by_org_month = billing_silver \
        .withColumn("month", F.date_format(col("billing_date"), "yyyy-MM")) \
        .groupBy("org_id", "month") \
        .agg(
            sum("amount_usd").alias("revenue_usd"),
            sum("credits_applied_usd").alias("total_credits_usd"),
            sum("tax_usd").alias("total_tax_usd"),
            sum("amount_local").alias("revenue_local_currency"),
            F.first("currency").alias("local_currency"),
            F.first("fx_rate_to_usd").alias("fx_rate"),
            (sum("amount_usd") + sum("tax_usd") - sum("credits_applied_usd")).alias("net_revenue_usd")
        ) \
        .withColumn("last_updated", F.current_timestamp())

    revenue_by_org_month.write \
        .mode("overwrite") \
        .partitionBy("month") \
        .parquet(f"{gold_path}/finops/revenue_by_org_month")

    print(f"✓ Mart creado: {revenue_by_org_month.count()} rows")

except Exception as e:
    print(f"⚠ Billing data no disponible, skipping revenue_by_org_month: {e}")


# --------------------------------------------------------------------------------
# MART 3: FinOps - cost_anomaly_mart
# --------------------------------------------------------------------------------
print("\n[3/5] Creando mart: cost_anomaly_mart...")

window_spec = Window.partitionBy("org_id", "service")

cost_anomaly_mart = spark.read.parquet(f"{silver_path}/usage_events_clean") \
    .groupBy("org_id", "usage_date", "service") \
    .agg(sum("cost_usd_increment").alias("daily_cost")) \
    .withColumn("mean_cost", F.avg("daily_cost").over(window_spec)) \
    .withColumn("stddev_cost", F.stddev("daily_cost").over(window_spec)) \
    .withColumn("z_score",
                when(col("stddev_cost") > 0,
                     (col("daily_cost") - col("mean_cost")) / col("stddev_cost"))
                .otherwise(0)) \
    .withColumn("is_anomaly",
                when(F.abs(col("z_score")) > 3, True).otherwise(False)) \
    .withColumn("anomaly_score", F.abs(col("z_score"))) \
    .withColumn("last_updated", F.current_timestamp()) \
    .select("org_id", "usage_date", "service", "daily_cost",
            "z_score", "anomaly_score", "is_anomaly", "last_updated")

cost_anomaly_mart.write \
    .mode("overwrite") \
    .partitionBy("usage_date") \
    .parquet(f"{gold_path}/finops/cost_anomaly_mart")

print(f"✓ Mart creado: {cost_anomaly_mart.count()} rows")


# --------------------------------------------------------------------------------
# MART 4: Soporte - tickets_by_org_date
# --------------------------------------------------------------------------------
print("\n[4/5] Creando mart: tickets_by_org_date...")

try:
    tickets_silver = spark.read.parquet(f"{silver_path}/support_tickets_clean")

    tickets_by_org_date = tickets_silver \
        .withColumn("ticket_date", to_date(col("created_at"))) \
        .groupBy("org_id", "ticket_date", "severity") \
        .agg(
            count("ticket_id").alias("ticket_count"),
            sum(when(col("sla_breached") == True, 1).otherwise(0)).alias("sla_breach_count"),
            avg("csat_score").alias("avg_csat"),
            countDistinct("ticket_id").alias("unique_tickets"),
            avg("resolution_time_hours").alias("avg_resolution_hours")
        ) \
        .withColumn("sla_breach_rate",
                    when(col("ticket_count") > 0, col("sla_breach_count") / col("ticket_count"))
                    .otherwise(0.0)) \
        .withColumn("last_updated", F.current_timestamp())

    tickets_by_org_date.write \
        .mode("overwrite") \
        .partitionBy("ticket_date") \
        .parquet(f"{gold_path}/support/tickets_by_org_date")

    print(f"✓ Mart creado: {tickets_by_org_date.count()} rows")

except Exception as e:
    print(f"⚠ Support tickets data no disponible, skipping tickets_by_org_date: {e}")


# --------------------------------------------------------------------------------
# MART 5: Producto/Usage - genai_tokens_by_org_date
# --------------------------------------------------------------------------------
print("\n[5/5] Creando mart: genai_tokens_by_org_date...")

COST_PER_1K_TOKENS = 0.002

genai_tokens_by_org_date = spark.read.parquet(f"{silver_path}/usage_events_clean") \
    .filter((col("service") == "genai") & (col("genai_tokens") > 0)) \
    .groupBy("org_id", "usage_date") \
    .agg(
        sum("genai_tokens").alias("total_tokens"),
        count("event_id").alias("total_requests"),
        avg("genai_tokens").alias("avg_tokens_per_request")
    ) \
    .withColumn("estimated_cost_usd",
                (col("total_tokens") / 1000) * lit(COST_PER_1K_TOKENS)) \
    .withColumn("last_updated", F.current_timestamp())

genai_tokens_by_org_date.write \
    .mode("overwrite") \
    .partitionBy("usage_date") \
    .parquet(f"{gold_path}/product/genai_tokens_by_org_date")

print(f"✓ Mart creado: {genai_tokens_by_org_date.count()} rows")

print("\n✓ Todos los marts Gold creados exitosamente")

# spark.stop()
