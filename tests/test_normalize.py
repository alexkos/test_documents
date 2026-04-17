from app.ingestion.normalizer import normalize_raw_record


def test_normalize_tags_comma_string() -> None:
    rec = normalize_raw_record({"external_id": "x", "tags": "water,energy"})
    assert rec.tags == ["water", "energy"]


def test_normalize_tags_lowercase_dedupe() -> None:
    rec = normalize_raw_record({"external_id": "x", "tags": ["Water", "water", "Energy"]})
    assert rec.tags == ["water", "energy"]


def test_normalize_tags_list_with_null() -> None:
    rec = normalize_raw_record({"external_id": "x", "tags": ["a", None, "b"]})
    assert rec.tags == ["a", "b"]


def test_normalize_open_access_coercion() -> None:
    rec = normalize_raw_record({"external_id": "x", "open_access": "yes"})
    assert rec.open_access is True
    rec2 = normalize_raw_record({"external_id": "y", "open_access": 0})
    assert rec2.open_access is False


def test_normalize_document_type_lowercase() -> None:
    rec = normalize_raw_record({"external_id": "x", "document_type": "REPORT"})
    assert rec.document_type == "report"


def test_external_id_int_coerced() -> None:
    rec = normalize_raw_record({"external_id": 16, "title": "t"})
    assert rec.external_id == "16"
