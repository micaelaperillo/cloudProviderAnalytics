# Cloud Provider Analytics

ETL + Streaming + Serving en Cassandra

## Execute

To run a package
```bash
uv run ingest/ingest.py
```

To run the API 
```bash
uv run uvicorn api.api:app --reload
```

To run the Frontend
```bash
uv run streamlit run frontend/app.py
```