from __future__ import annotations

from typing import TYPE_CHECKING

from app.models import Document
from app.processing.classification import classify_document
from app.processing.keywords import extract_keywords
from app.processing.scoring import compute_score
from app.processing.summary import summarize_body_or_abstract
from app.utils.logger import logger

if TYPE_CHECKING:
    from app.ingestion.normalizer import NormalizedRecord


def apply_processing(doc: Document, rec: NormalizedRecord) -> None:
    logger.debug(
        "apply_processing document_id=%s external_id=%r",
        doc.id,
        rec.external_id,
    )
    blob = " ".join(filter(None, [rec.title, rec.abstract, rec.body]))
    doc.keywords = extract_keywords(blob)
    doc.classification = classify_document(rec.title, rec.body)
    doc.score = compute_score(doc)
    doc.summary = summarize_body_or_abstract(rec.body, rec.abstract)
