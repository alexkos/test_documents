from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.routes_api import router as api_router
from app.utils.logger import logger


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("Document intake API starting")
    yield
    logger.info("Document intake API shutting down")


app = FastAPI(
    title="Document intake",
    description="Mini data platform: ingestion, normalization, enrichment, query API",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(api_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
