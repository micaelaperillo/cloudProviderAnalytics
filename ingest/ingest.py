from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
import os
import shutil

datalake_path = 'datalake'
landing_zone_path = f"{datalake_path}/landing"
landing_zone_stream_path = f"{datalake_path}/landing/usage_events_stream"
bronze_batch_path = f"{datalake_path}/bronze/batch_data"
bronze_stream_path = f"{datalake_path}/bronze/usage_events"
checkpoint_location = f"{datalake_path}/checkpoints/usage_events_cp"
quarantine_path = f"{datalake_path}/quarantine/usage_events_errors"
quarantine_checkpoint = f"{datalake_path}/checkpoints/quarantine_cp"

def ingest_data(): 
    spark = SparkSession.builder\
        .appName("Big Data")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    files = os.listdir(landing_zone_path)
    batch_files = []
    for file in files:
        if '.csv' in file:
            batch_files.append(file)

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

    bronze_paths = [bronze_batch_path, bronze_stream_path, quarantine_path, checkpoint_location, quarantine_checkpoint]
    for path in bronze_paths:
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
            except Exception as e:
                print(f"✗ Error cleaning {path}: {e}")
        else:
            print(f"- Path does not exist (will be created): {path}")

    for file, csv_df in csv_dataframes.items():

        #Write to Bronze Parquet Batch
        #TODO si uso overwrite solo se crea 1 archivo????
        # solo append crea varios, pero no quiero que se appendee contenido a los archivos viejos
        csv_df.write \
            .mode("append") \
            .format("parquet") \
            .partitionBy("source_file", "ingest_ts") \
            .save(bronze_batch_path)

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

    processed_stream_df = jsonl_stream_df \
        .withWatermark("timestamp", "10 minutes") \
        .dropDuplicates(["event_id"]) \
        .withColumns({'ingest_ts': current_timestamp(), 'source_file': input_file_name()})

    processed_stream_df = processed_stream_df.withColumn("date", date_format(col("ingest_ts"), "yyyyMMdd"))

    processed_stream_df.printSchema()
    processed_stream_df.printSchema()


    quality_rules_df = processed_stream_df.selectExpr("*",
        # event_id no nulo
        "event_id IS NOT NULL as is_event_id_valid",

        # timestamp valido
        "timestamp IS NOT NULL as is_valid_timestamp",

        # unit no nulo cuando value no nulo
        "CASE WHEN value IS NOT NULL THEN unit IS NOT NULL ELSE TRUE END as is_unit_valid",

        # verificar que la version sea 1 o 2
        "schema_version IN (1, 2) as is_schema_version_valid",

        # unidad correcta según la métrica
        """
        CASE
            WHEN metric = 'requests' THEN unit = 'count'
            WHEN metric = 'cpu_hours' THEN unit = 'hours'
            WHEN metric = 'storage_gb_hours' THEN unit = 'gb_hours'
            ELSE TRUE
        END as is_correct_unit
        """
    )

    quarantine_flagged_df = quality_rules_df.withColumn("is_quarantine",
        ~col("is_event_id_valid") | \
        ~col("is_valid_timestamp") | \
        ~col("is_unit_valid") | \
        ~col("is_schema_version_valid") | \
        ~col("is_correct_unit")
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

    query_good.awaitTermination()
    query_quarantine.awaitTermination()

    spark.stop()
