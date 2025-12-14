from fastapi import FastAPI
from .models import (
    Query1Request,
    Query2Request,
    Query3Request,
    Query4Request,
    Query5Request
)

# IMPORTS RENOMBRADOS PARA EVITAR COLISIONES
from serving.serving import *  # si hay más funciones, importarlas con alias
import os
from ingest.ingest import ingest_data
from silver.silver import silver as run_silver

from astrapy import DataAPIClient
from dotenv import load_dotenv

app = FastAPI()

# cassandra connection
load_dotenv()

ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("KEYSPACE", "datalake")

client = DataAPIClient(ASTRA_TOKEN)
db = client.get_database_by_api_endpoint(
    'https://585a4a70-5434-4747-950c-595c987cbc1d-eu-west-1.apps.astra.datastax.com',
    keyspace=KEYSPACE
)


# ============================================================
# INGEST
# ============================================================

@app.post("/ingest")
def ingest_endpoint():
    try:
        ingest_data()
        return {"status": "Ingestion finished successfully"}
    except Exception as e:
        return {"status": "Ingestion failed", "error": str(e)}


# ============================================================
# SILVER
# ============================================================

@app.post("/silver")
def silver_endpoint():
    try:
        run_silver()
        return {"status": "Silver layer processing finished successfully"}
    except Exception as e:
        return {"status": "Silver processing failed", "error": str(e)}


# ============================================================
# SERVING
# ============================================================

@app.post("/serving")
def serving_endpoint():
    try:
        run_serving_pipeline()
        return {"status": "Serving layer processing finished successfully"}
    except Exception as e:
        return {"status": "Serving processing failed", "error": str(e)}


# ============================================================
# QUERIES
# ============================================================

@app.post("/query/costs-daily")
def query_costs_daily_endpoint(req: Query1Request):
    result = retrieve_query1_results(req)
    return result


@app.post("/query/top-services")
def query_top_services_endpoint(req: Query2Request):
    result = retrieve_query2_results(req)
    return result


@app.post("/query/sla-evolution")
def query_sla_evolution_endpoint(req: Query3Request):
    result = retrieve_query3_results(req)
    return result


@app.post("/query/monthly-revenue")
def query_monthly_revenue_endpoint(req: Query4Request):
    result = retrieve_query4_results(req)
    return result


@app.post("/query/genai-tokens")
def query_genai_tokens_endpoint(req: Query5Request):
    result = retrieve_query5_results(req)
    return result
