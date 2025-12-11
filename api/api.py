from fastapi import FastAPI
from .models import (
    Query1Request,
    Query2Request,
    Query3Request,
    Query4Request,
    Query5Request
)

from ingest import ingest_data

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
    return {"status": "Silver layer processing started"}

@app.post("/serving")
def serving():
    return {"status": "Serving layer processing started"}

@app.post("/query/costs-daily")
def query_costs_daily(req: Query1Request):
    return {
        "organization": req.organization,
        "service": req.service,
        "range": [req.start_date, req.end_date],
        "daily": [
            {"date": "2025-01-01", "cost_usd": 12.4, "requests": 183},
            {"date": "2025-01-02", "cost_usd": 14.9, "requests": 201},
            {"date": "2025-01-03", "cost_usd": 11.2, "requests": 152},
        ]
    }


@app.post("/query/top-services")
def query_top_services(req: Query2Request):
    return {
        "organization": req.organization,
        "top_n": req.top_n,
        "services": [
            {"service": "Compute Engine", "total_cost_usd": 230.55},
            {"service": "Cloud Storage", "total_cost_usd": 180.12},
            {"service": "GenAI Embeddings", "total_cost_usd": 95.44},
        ][:req.top_n]
    }


@app.post("/query/sla-evolution")
def query_sla_evolution(req: Query3Request):
    return {
        "organization": req.organization,
        "days": [
            {"date": "2025-01-01", "critical_tickets": 3, "sla_breach_rate": 0.02},
            {"date": "2025-01-02", "critical_tickets": 1, "sla_breach_rate": 0.00},
            {"date": "2025-01-03", "critical_tickets": 5, "sla_breach_rate": 0.04},
        ]
    }


@app.post("/query/monthly-revenue")
def query_monthly_revenue(req: Query4Request):
    return {
        "organization": req.organization,
        "month": f"{req.year}-{req.month:02d}",
        "revenue_usd_normalized": 15432.77,
        "credits_applied_usd": 230.00,
        "taxes_usd": 493.40,
        "final_revenue_usd": 15432.77 - 230 + 493.40
    }


@app.post("/query/genai-tokens")
def query_genai_tokens(req: Query5Request):
    return {
        "organization": req.organization,
        "range": [req.start_date, req.end_date],
        "daily": [
            {"date": "2025-01-01", "tokens_input": 120000, "tokens_output": 54000, "cost_usd": 4.32},
            {"date": "2025-01-02", "tokens_input": 96000, "tokens_output": 42000, "cost_usd": 3.10},
            {"date": "2025-01-03", "tokens_input": 150000, "tokens_output": 61000, "cost_usd": 5.05},
        ]
    }