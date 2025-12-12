from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, countDistinct, when, lit, to_date, date_format
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import *

datalake_path = 'datalake'
silver_path = f"{datalake_path}/silver"
silver_path_usage_events_clean = f"{silver_path}/usage_events_clean"
silver_path_daily_usage = f"{silver_path}/daily_usage"
silver_path_billing_monthly_clean = f"{silver_path}/billing_monthly_clean"
silver_path_support_tickets_clean = f"{silver_path}/support_tickets_clean"
silver_path_batch = f"{silver_path}/batch_data"
gold_path = f"{datalake_path}/gold"


spark = SparkSession.builder\
    .appName("Big Data")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def query1(): 
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


def query2():
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


def query3():
    """
    Daily evolution of critical tickets and SLA breach rate
    """
    support_tickets_df = spark.read.parquet(silver_path_support_tickets_clean)

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
    
    critical_tickets_evolution_sla_rate_daily.printSchema()
    return critical_tickets_evolution_sla_rate_daily


def query4():
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


def query5():
    """
    Daily total of GenAI tokens used and their associated cost in USD
    """
    silver_path_events_clean = spark.read.parquet(silver_path_usage_events_clean)
    genai_tokens_cost_daily = silver_path_events_clean \
        .filter(col("service") == "genai") \
        .groupBy("usage_date") \
        .agg(
            sum("genai_tokens").alias("total_genai_tokens"),
            sum("cost_usd_increment").alias("total_genai_cost_usd"),
        ).orderBy("usage_date")

    genai_tokens_cost_daily.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/finops/genai_tokens_cost_daily")
    
    genai_tokens_cost_daily.printSchema()
    return genai_tokens_cost_daily
    

if __name__ == "__main__":
    query1()
    query3()
    query5()