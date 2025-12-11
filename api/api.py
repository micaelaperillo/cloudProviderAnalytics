from fastapi import FastAPI
from .models import (
    Query1Request,
    Query2Request,
    Query3Request,
    Query4Request,
    Query5Request
)

from ingest import ingest_data
from silver import silver
from marts import query1, query2, query3, query4, query5

app = FastAPI()

@app.post("/ingest")
def ingest():
    try:
        ingest_data()
        return {"status": "Ingestion started"}
    except Exception as e:
        return {"status": "Ingestion failed", "error": str(e)}


@app.post("/silver")
def silver():
    try:
        silver()
        return {"status": "Silver layer processing started"}
    except Exception as e:
        return {"status": "Silver processing failed", "error": str(e)}


@app.post("/serving")
def serving():
    return {"status": "Serving layer processing started"}


@app.post("/query/costs-daily")
def query_costs_daily(req: Query1Request):
    query1()


@app.post("/query/top-services")
def query_top_services(req: Query2Request):
    query2()


@app.post("/query/sla-evolution")
def query_sla_evolution(req: Query3Request):
    query3()


@app.post("/query/monthly-revenue")
def query_monthly_revenue(req: Query4Request):
    query4()


@app.post("/query/genai-tokens")
def query_genai_tokens(req: Query5Request):
    query5()