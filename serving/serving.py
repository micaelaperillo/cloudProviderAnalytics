import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from astrapy import DataAPIClient

# =====================================================================
# SPARK FACTORY (NO INICIALIZAR SPARK EN IMPORT)
# =====================================================================

def get_spark():
    return (
        SparkSession.builder
        .appName("Big Data - Cassandra Integration")
        .master("local[*]")
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
        .getOrCreate()
    )

# =====================================================================
# LOAD ENV + ASTRA DB CLIENT
# =====================================================================

load_dotenv()

ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("KEYSPACE", "datalake")

client = DataAPIClient(ASTRA_TOKEN)
db = client.get_database_by_api_endpoint(
    "https://585a4a70-5434-4747-950c-595c987cbc1d-eu-west-1.apps.astra.datastax.com",
    keyspace=KEYSPACE
)

# =====================================================================
# FETCH HELPERS (USED BY FASTAPI)
# =====================================================================

def fetch_all_docs(collection_name):
    col = db[collection_name]
    return [doc for doc in col.find({})]

def retrieve_query3_results():
    return fetch_all_docs("critical_tickets_evolution_sla_rate_daily")

def retrieve_query5_results():
    return fetch_all_docs("genai_tokens_cost_daily")

# =====================================================================
# SERVING PIPELINE (ONLY RUN WHEN EXPLICITLY CALLED)
# =====================================================================

def run_serving_pipeline():
    """
    Ejecuta la carga Gold → Cassandra.
    Llamar manualmente, NO en imports.
    """
    spark = get_spark()

    datalake_path = "datalake"
    gold_path = f"{datalake_path}/gold"

    critical_path = f"{gold_path}/finops/critical_tickets_evolution_sla_rate_daily"
    genai_path = f"{gold_path}/finops/genai_tokens_cost_daily"

    # definimos las funciones de mapeo
    def map_critical_tickets(row):
        return {
            "date": str(row["date"]),
            "breach_count": int(row["breach_count"]),
            "solved_count": int(row["solved_count"]),
            "critical_ticket_count": int(row["critical_ticket_count"]),
            "breach_rate": float(row["breach_rate"]),
        }

    def map_genai_tokens(row):
        return {
            "usage_date": str(row["usage_date"]),
            "total_genai_tokens": int(row["total_genai_tokens"]),
            "total_genai_cost_usd": float(row["total_genai_cost_usd"]),
        }

    # helper para carga
    def load_to_cassandra(path, table_name, mapping_fn):
        print(f"Cargando {table_name} desde {path} ...")
        df = spark.read.parquet(path)
        docs = [mapping_fn(r) for r in df.collect()]
        col = db[table_name]
        col.insert_many(docs)
        print(f"OK: {len(docs)} registros")

    # crear colecciones si no existen
    if "critical_tickets_evolution_sla_rate_daily" not in db.list_collection_names():
        db.create_collection("critical_tickets_evolution_sla_rate_daily")

    if "genai_tokens_cost_daily" not in db.list_collection_names():
        db.create_collection("genai_tokens_cost_daily")

    # cargar datos
    load_to_cassandra(critical_path, "critical_tickets_evolution_sla_rate_daily", map_critical_tickets)
    load_to_cassandra(genai_path, "genai_tokens_cost_daily", map_genai_tokens)

    spark.stop()


if __name__ == "__main__":
    # ejecutar pipeline manualmente
    run_serving_pipeline()
    print(retrieve_query3_results())
    print(retrieve_query5_results())
