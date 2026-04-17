from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import documents as documents_routes
from app.api.routes import ingestions as ingestions_routes
from app.api.routes import stats as stats_routes
from app.config import elasticsearch_enabled, elasticsearch_startup_summary
from app.search.index import ensure_elasticsearch_index
from app.utils.logger import logger


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("Document intake API starting")
    logger.info(elasticsearch_startup_summary())
    if elasticsearch_enabled() and ensure_elasticsearch_index() is not None:
        logger.info("Elasticsearch index ready")
    yield
    logger.info("Document intake API shutting down")


app = FastAPI(
    title="Document intake",
    description="Mini data platform: ingestion, normalization, enrichment, query API",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(documents_routes.router, prefix="/documents", tags=["documents"])
app.include_router(ingestions_routes.router, prefix="/ingestions", tags=["ingestions"])
app.include_router(stats_routes.router, prefix="/stats", tags=["stats"])


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
