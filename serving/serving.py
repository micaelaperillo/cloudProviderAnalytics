import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from astrapy import DataAPIClient
# from connect_database import db_session
from cassandra import ConsistencyLevel
from api.models import *

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

# session = db_session()

# KEYSPACE = "datalake"
# def keyspaces():
#     rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
#     print("KEYSPACES:")
#     for row in rows:
#         print(row.keyspace_name)

# =====================================================================
# FETCH HELPERS (USED BY FASTAPI)
# =====================================================================

def fetch_all_docs(collection_name):
    col = db[collection_name]
    return [doc for doc in col.find({})]

def retrieve_query1_results(req: Query1Request):
    result = fetch_all_docs("org_daily_usage_by_service")

    #filter by org_id, services, date range
    filtered = []
    for doc in result:
        if doc["org_id"] == req.organization and \
            doc["service"] in req.service and \
            doc["usage_date"] >= req.start_date and doc["usage_date"] <= req.end_date:
            filtered.append(doc)

    return filtered

def retrieve_query2_results(req: Query2Request):
    result = fetch_all_docs("services_by_cum_cost_by_org")

    #filter by org_id, top_n, last 14 days from reference_date
    #TODO filtrar por ultimos 14 dias
    filtered = []
    for doc in result:
        if doc["org_id"] == req.organization:
            filtered.append(doc)
    filtered.sort(key=lambda x: x["revenue_usd"], reverse=True)
    return filtered[:req.top_n]

def retrieve_query3_results(req: Query3Request):
    result = fetch_all_docs("critical_tickets_evolution_sla_rate_daily")

    #filter by date range
    filtered = []
    for doc in result:
        if doc['date'] >= req.start_date and doc['date'] <= req.end_date:
            filtered.append(doc)
    return filtered

def retrieve_query4_results(req: Query4Request):
    return fetch_all_docs("revenue_by_org_month_usd")

def retrieve_query5_results(req: Query5Request):
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

    org_daily_usage_by_service_path = f"{gold_path}/finops/org_daily_usage_by_service"
    services_by_cum_cost_by_org_path = f"{gold_path}/finops/services_by_cum_cost_by_org"
    critical_path = f"{gold_path}/finops/critical_tickets_evolution_sla_rate_daily"
    revenue_by_org_month_usd_path = f"{gold_path}/finops/revenue_by_org_month_usd"
    genai_path = f"{gold_path}/finops/genai_tokens_cost_daily"

    

    # crear colecciones si no existen

    #Tabla query 1
    #costos y requests diarios por org y servicio
    if "org_daily_usage_by_service" in db.list_collection_names():
        db["org_daily_usage_by_service"].drop()
    #Tabla query 2
    #TOP N servicios por costo acumulkado en los ultimos 14 dias por ORG
    if 'services_by_cum_cost_by_org' in db.list_collection_names():
        db["services_by_cum_cost_by_org"].drop()

    #Tabla query 3
    #Evolucion tickets criticos y sla breach rate diario
    if "critical_tickets_evolution_sla_rate_daily" in db.list_collection_names():
        db["critical_tickets_evolution_sla_rate_daily"].drop()
    #Tabla query 4
    #Revenue mensual con creditos/impuestos aplucados en USD
    if 'revenue_by_org_month_usd' in db.list_collection_names():
        db["revenue_by_org_month_usd"].drop()
    #Tabla query 5
    #Tokens y costo diario de GenAI
    if "genai_tokens_cost_daily" in db.list_collection_names():
        db["genai_tokens_cost_daily"].drop()

    db.create_collection("org_daily_usage_by_service")
    db.create_collection('services_by_cum_cost_by_org')
    db.create_collection("critical_tickets_evolution_sla_rate_daily")
    db.create_collection('revenue_by_org_month_usd')
    db.create_collection("genai_tokens_cost_daily")

    # definimos las funciones de mapeo
    def map_org_daily_usage_by_service(row):
        return {
            "org_id": str(row["org_id"]),
            "service": str(row["service"]),
            "usage_date": str(row["usage_date"]),
            "daily_cost_usd": float(row["daily_cost_usd"]),
            "total_requests": float(row["total_requests"]),
            "total_cpu_hours": float(row["total_cpu_hours"]),
            "total_storage_gb_hours": float(row["total_storage_gb_hours"]),
            "total_genai_tokens": int(row["total_genai_tokens"]),
            "total_carbon_kg": float(row["total_carbon_kg"]),
            "last_updated": str(row["last_updated"]),
        }

    def map_services_by_cum_cost_by_org(row):
        return {
            "org_id": str(row["org_id"]),
            "month_and_year": str(row["month_and_year"]),
            "revenue_usd": float(row["revenue_usd"]),
            "credits_usd": float(row["credits_usd"]),
            "tax_usd": float(row["tax_usd"]),
            "fx_applied": float(row["fx_applied"]),
        }

    def map_critical_tickets(row):
        return {
            "date": str(row["date"]),
            "breach_count": int(row["breach_count"]),
            "solved_count": int(row["solved_count"]),
            "critical_ticket_count": int(row["critical_ticket_count"]),
            "breach_rate": float(row["breach_rate"]),
        }

    def map_revenue_by_org_month_usd(row):
        return {
            "org_id": str(row["org_id"]),
            "month": str(row["month"]),
            "revenue_usd": float(row["revenue_usd"]),
            "credits_usd": float(row["credits_usd"]),
            "tax_usd": float(row["tax_usd"]),
            "fx_applied": float(row["fx_applied"]),
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

    # cargar datos
    load_to_cassandra(org_daily_usage_by_service_path, "org_daily_usage_by_service", map_org_daily_usage_by_service)
    load_to_cassandra(services_by_cum_cost_by_org_path, "services_by_cum_cost_by_org", map_services_by_cum_cost_by_org)
    load_to_cassandra(critical_path, "critical_tickets_evolution_sla_rate_daily", map_critical_tickets)
    load_to_cassandra(revenue_by_org_month_usd_path, "revenue_by_org_month_usd", map_revenue_by_org_month_usd)
    load_to_cassandra(genai_path, "genai_tokens_cost_daily", map_genai_tokens)

    spark.stop()
        
if __name__ == "__main__":
    # ejecutar pipeline manualmente
    run_serving_pipeline()
    print(retrieve_query3_results())
    print(retrieve_query5_results())
