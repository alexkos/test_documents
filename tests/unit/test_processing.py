"""Unit tests for ``app.processing`` helpers (classification, keywords, scoring, summary)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.processing.classification import classify_document
from app.processing.keywords import extract_keywords
from app.processing.scoring import compute_score
from app.processing.summary import summarize_body_or_abstract


def test_classify_policy_from_title() -> None:
    assert classify_document(None, "Energy Policy Today", None) == "policy"


def test_classify_policy_from_body() -> None:
    assert classify_document(None, None, "Discussion of climate policy measures.") == "policy"


def test_classify_report_from_body() -> None:
    assert classify_document(None, "Annual", "This quarterly report covers Q3.") == "report"


def test_classify_policy_checked_before_report() -> None:
    assert classify_document(None, "Policy report", None) == "policy"


def test_classify_data_from_text() -> None:
    assert classify_document(None, "Dataset title", "We release the raw data today.") == "data"


def test_classify_structured_document_type_takes_priority() -> None:
    assert classify_document("report", "climate policy overview", "policy debate continues") == "report"


def test_classify_document_type_mapping_journal_article() -> None:
    assert classify_document("journal_article", "Short", "Body") == "research"


def test_classify_document_type_mapping_press_release() -> None:
    assert classify_document("press_release", None, None) == "news"


def test_classify_other_when_no_match() -> None:
    assert classify_document(None, "Notes", "Short memo without magic words.") == "other"


def test_classify_other_for_empty_inputs() -> None:
    assert classify_document(None, None, None) == "other"
    assert classify_document(None, "", "") == "other"


def test_extract_keywords_empty_returns_empty() -> None:
    assert extract_keywords(None) == []
    assert extract_keywords("") == []


def test_extract_keywords_uses_tfidf_and_english_stopwords() -> None:
    text = "machine learning models learn from data; machine data"
    out = extract_keywords(text, top_n=5)
    assert "machine" in out
    assert "learning" in out
    assert "models" in out
    assert "data" in out
    assert "from" not in out


def test_extract_keywords_energy_market_example() -> None:
    text = (
        "The global energy market is experiencing rapid growth due to increasing demand "
        "for renewable energy and rising oil prices. Energy companies are investing "
        "heavily in solar and wind power."
    )
    # Single-document TF-IDF ranks rarer terms above one-off mentions; need enough slots
    # for late-ranked terms like "solar" / "wind".
    out = extract_keywords(text, top_n=18)
    assert "energy" in out
    assert "renewable" in out
    assert "solar" in out
    assert "wind" in out
    assert "the" not in out
    assert "is" not in out


def test_extract_keywords_hyphenated_words() -> None:
    out = extract_keywords("state-of-the-art results are strong", top_n=10)
    assert "state-of-the-art" in out


def test_extract_keywords_respects_top_n() -> None:
    text = "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu"
    out = extract_keywords(text, top_n=3)
    assert len(out) == 3


def test_compute_score_formula() -> None:
    doc = SimpleNamespace(citation_count=10, relevance_score=0.5, word_count=100)
    assert compute_score(doc) == pytest.approx(23.25)


def test_compute_score_none_counts_as_zero() -> None:
    doc = SimpleNamespace(citation_count=None, relevance_score=None, word_count=None)
    assert compute_score(doc) == 0.0


def test_summarize_prefers_body_when_present() -> None:
    assert summarize_body_or_abstract("First line. Second line. Third.", "Abstract only.") == (
        "First line. Second line."
    )


def test_summarize_falls_back_to_abstract() -> None:
    assert summarize_body_or_abstract(None, "One sentence. Two sentences. Rest.") == (
        "One sentence. Two sentences."
    )


def test_summarize_empty_body_uses_abstract() -> None:
    assert summarize_body_or_abstract("", "Abstract here. More here.") == (
        "Abstract here. More here."
    )


def test_summarize_none_returns_none() -> None:
    assert summarize_body_or_abstract(None, None) is None
