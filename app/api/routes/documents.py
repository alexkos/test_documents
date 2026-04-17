from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.document_service import format_search_query_for_log, get_document, list_documents
from app.utils.logger import logger

router = APIRouter()


@router.get("")
def documents(
    request: Request,
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
    date_from: str | None = None,
    date_to: str | None = None,
    tag: str | None = None,
    organization: str | None = None,
    status: str | None = None,
    search: str | None = None,
) -> dict:
    search_stripped = (search or "").strip()
    if search_stripped:
        base = f"{request.url.scheme}://{request.url.netloc}{request.url.path}"
        logger.info(
            f"GET {base} search={format_search_query_for_log(search_stripped)!r} "
            f"skip={skip} limit={limit}"
        )
    return list_documents(
        db,
        skip=skip,
        limit=limit,
        date_from=date_from,
        date_to=date_to,
        tag=tag,
        organization=organization,
        status=status,
        search=search,
    )


@router.get("/{doc_id}")
def document(doc_id: int, db: Session = Depends(get_db)) -> dict:
    row = get_document(db, doc_id)
    if row is None:
        raise HTTPException(status_code=404, detail="document not found")
    return row
