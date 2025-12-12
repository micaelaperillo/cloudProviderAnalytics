
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from astrapy import DataAPIClient

# --------------------------------------------------------------------------------
# Instalar y configurar Spark Cassandra Connector
# --------------------------------------------------------------------------------

print("Instalando Spark Cassandra Connector...")

# Reinstalar Spark con el conector de Cassandra

spark = SparkSession.builder \
    .appName("Big Data - Cassandra Integration") \
    .config("spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# paths
datalake_path = 'datalake'
gold_path = f"{datalake_path}/gold"
critical_tickets_evolution_sla_rate_daily_path = f"{gold_path}/finops/critical_tickets_evolution_sla_rate_daily"
genai_tokens_cost_daily_path = f"{gold_path}/finops/genai_tokens_cost_daily"


# --------------------------------------------------------------------------------
# Configuración de conexión a AstraDB
# --------------------------------------------------------------------------------

load_dotenv()

ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("KEYSPACE", "datalake")

client = DataAPIClient(ASTRA_TOKEN)
db = client.get_database_by_api_endpoint(
    'https://585a4a70-5434-4747-950c-595c987cbc1d-eu-west-1.apps.astra.datastax.com',
    keyspace=KEYSPACE
)

# --------------------------------------------------------------------------------
# Crear tablas en Cassandra usando CQL
# --------------------------------------------------------------------------------

# TABLA 1: org_daily_usage_by_service

create_table_1 = """
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id text,
    usage_date date,
    service text,
    daily_cost_usd double,
    total_requests double,
    total_cpu_hours double,
    total_storage_gb_hours double,
    total_genai_tokens int,
    total_carbon_kg double,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
"""

# create_table_3 = "critical_tickets_evolution_sla_rate_daily"
table_names = ['critical_tickets_evolution_sla_rate_daily', 'genai_tokens_cost_daily']
astra_tables = db.list_collection_names()
for col_name in table_names:
    if col_name in astra_tables:
        db.drop_collection(col_name)

db.create_collection('critical_tickets_evolution_sla_rate_daily')
def map_critical_tickets(row):
    return {
        "date": str(row["date"]),  # convertimos a texto
        "breach_count": int(row["breach_count"]),
        "solved_count": int(row["solved_count"]),
        "critical_ticket_count": int(row["critical_ticket_count"]),
        "breach_rate": float(row["breach_rate"])
    }

# TABLA 5: genai_tokens_cost_daily
db.create_collection('genai_tokens_cost_daily')
def map_genai_tokens(row):
    return {
        "usage_date": str(row["usage_date"]),
        "total_genai_tokens": int(row["total_genai_tokens"]),
        "total_genai_cost_usd": float(row["total_genai_cost_usd"])
    }



# --------------------------------------------------------------------------------
# Cargar datos desde Gold (Parquet) a Cassandra usando Spark Connector
# --------------------------------------------------------------------------------

# Función helper para cargar con manejo de errores
def load_to_cassandra(parquet_path, table_name, description, mapping_function):
    """
    Carga datos desde Parquet a Cassandra usando DataAPIClient
    Args: 
        parquet_path (str): Ruta al archivo Parquet
        table_name (str): Nombre de la tabla en Cassandra
        description (str): Descripción para logs
        mapping_function (func): Función para mapear filas a diccionarios con models
    """
    try:
        print(f"\n[{description}] Cargando datos desde {parquet_path} a tabla {table_name}...")

        # Leer desde Gold
        df = spark.read.parquet(parquet_path)
        df.show(5, truncate=False)
        row_count = df.count()
        print(f"  Filas leídas: {row_count}")

        # Convertir a lista de diccionarios
        records = df.collect()
        docs = [mapping_function(r) for r in records]

        # Obtener la tabla de Cassandra
        table = db[table_name]
        table.insert_many(docs)

        print(f"✓ {row_count} filas cargadas en {table_name}")
        return True

    except Exception as e:
        print(f"✗ Error cargando {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return False


# CARGAR Query 3: critical_tickets_evolution_sla_rate_daily
load_to_cassandra(
    critical_tickets_evolution_sla_rate_daily_path,
    "critical_tickets_evolution_sla_rate_daily",
    "Query 3 - Critical Tickets Evolution & SLA Rate",
    map_critical_tickets
)

# CARGAR MART 5: genai_tokens_by_org_date
load_to_cassandra(
    genai_tokens_cost_daily_path,
    "genai_tokens_cost_daily",
    "MART 5 - GenAI Usage",
    map_genai_tokens
)


def fetch_all_docs(collection_name):
    col = db[collection_name]
    results = col.find({})
    docs = [doc for doc in results]  # cada doc es el JSON plano
    return docs

def retrieve_query3_results():
    return fetch_all_docs('critical_tickets_evolution_sla_rate_daily')

def retrieve_query5_results():
    return fetch_all_docs('genai_tokens_cost_daily')

spark.stop()

if __name__ == "__main__":
    result_query3 = retrieve_query3_results()
    result_query5 = retrieve_query5_results()
    for result in result_query3:
        print(result)
    for result in result_query5:
        print(result)