from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, when, to_date, date_format
)
from functools import reduce
import pyspark.sql.functions as F

# =====================================================================
# SPARK FACTORY (NO CREAR SPARK EN IMPORT)
# =====================================================================

def get_spark():
    return (
        SparkSession.builder
        .appName("Big Data Marts")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .getOrCreate()
    )


# =====================================================================
# PATHS
# =====================================================================

datalake_path = "datalake"
silver_path = f"{datalake_path}/silver"
gold_path = f"{datalake_path}/gold"

silver_path_usage_events_clean = f"{silver_path}/usage_events_clean"
silver_path_daily_usage = f"{silver_path}/daily_usage"
silver_path_billing_monthly_clean = f"{silver_path}/billing_monthly_clean"
silver_path_support_tickets_clean = f"{silver_path}/support_tickets_clean"


# =====================================================================
# QUERIES (NO TIENEN SPARK GLOBAL)
# =====================================================================

def query1():
    spark = get_spark()

    df = spark.read.parquet(silver_path_daily_usage)

    out = df.select(
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

    out.write \
        .mode("overwrite") \
        .partitionBy("usage_date") \
        .parquet(f"{gold_path}/finops/org_daily_usage_by_service")

    out.printSchema()
    return out


def query2():
    spark = get_spark()

    billing = spark.read.parquet(silver_path_billing_monthly_clean)

    revenue = (
        billing
        .withColumn("month_and_year", date_format(col("month"), "yyyy-MM"))
        .groupBy("org_id", "month_and_year")
        .agg(
            sum("subtotal_usd").alias("revenue_usd"),
            sum("credits_usd").alias("credits_usd"),
            sum("taxes_usd").alias("tax_usd"),
            avg("exchange_rate_to_usd").alias("fx_applied")
        )
    )

    revenue.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/finops/services_by_cum_cost_by_org")

    revenue.printSchema()
    return revenue


def query3():
    spark = get_spark()

    df = spark.read.parquet(silver_path_support_tickets_clean)

    out = (
        df.filter(col("severity") == "critical")
        .withColumn("date", to_date(col("created_at")))
        .withColumn("solved", when(col("resolved_at").isNotNull(), to_date(col("resolved_at"))))
        .groupBy("date")
        .agg(
            sum(when(col("sla_breached") == True, 1).otherwise(0)).alias("breach_count"),
            sum(when(col("solved").isNotNull(), 1).otherwise(0)).alias("solved_count"),
            count("*").alias("critical_ticket_count"),
        )
        .withColumn("breach_rate", col("breach_count") / col("critical_ticket_count"))
        .orderBy("date")
    )

    out.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/finops/critical_tickets_evolution_sla_rate_daily")

    out.printSchema()
    return out


def query4():
    spark = get_spark()

    df = spark.read.parquet(silver_path_billing_monthly_clean)

    out = (
        df.groupBy("org_id", "month")
        .agg(
            sum("subtotal_usd").alias("revenue_usd"),
            sum("credits_usd").alias("credits_usd"),
            sum("taxes_usd").alias("tax_usd"),
            avg("exchange_rate_to_usd").alias("fx_applied"),
        )
    )

    out.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/finops/revenue_by_org_month_usd")

    out.printSchema()
    return out


def query5():
    spark = get_spark()

    df = spark.read.parquet(silver_path_usage_events_clean)

    out = (
        df.filter(col("service") == "genai")
        .groupBy("usage_date")
        .agg(
            sum("genai_tokens").alias("total_genai_tokens"),
            sum("cost_usd_increment").alias("total_genai_cost_usd"),
        )
        .orderBy("usage_date")
    )

    out.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/finops/genai_tokens_cost_daily")

    out.printSchema()
    return out

def columns_with_nulls(df):
    cols = []
    for c in df.columns:
        if df.filter(col(c).isNull()).count() > 0:
            cols.append(c)
    return cols

if __name__ == "__main__":
    spark = get_spark()
    
    df1 = query1()
    print("query1 tiene NULLs:", columns_with_nulls(df1))
    df1.select('total_requests', 'total_cpu_hours', 'total_storage_gb_hours').where(
        (col('total_requests').isNull()) |
        (col('total_cpu_hours').isNull()) |
        (col('total_storage_gb_hours').isNull())
    ).show()

    df2 = query2()
    print("query2 tiene NULLs:", columns_with_nulls(df2))
    df2.select('credits_usd').where(
        (col('credits_usd').isNull())
    ).show()

    df3 = query3()
    print("query3 tiene NULLs:", columns_with_nulls(df3))

    df4 = query4()
    print("query4 tiene NULLs:", columns_with_nulls(df4))
    df4.select('credits_usd').where(
        (col('credits_usd').isNull())
    ).show()

    df5 = query5()
    print("query5 tiene NULLs:", columns_with_nulls(df5))
    spark.stop()
