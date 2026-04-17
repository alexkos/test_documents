from fastapi import FastAPI

from app.routes_api import router as api_router

app = FastAPI(
    title="Document intake",
    description="Mini data platform: ingestion, normalization, enrichment, query API",
    version="0.1.0",
)

app.include_router(api_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
