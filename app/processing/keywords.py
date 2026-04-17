from __future__ import annotations

from sklearn.feature_extraction.text import TfidfVectorizer

# Match prior tokenizer: word-like tokens including hyphenated compounds (e.g. state-of-the-art).
_TOKEN_PATTERN = r"(?u)[a-zA-Z][a-zA-Z\-]{2,}"


def extract_keywords(text: str | None, top_n: int = 12) -> list[str]:
    if not text or not text.strip():
        return []
    vectorizer = TfidfVectorizer(stop_words="english", token_pattern=_TOKEN_PATTERN)
    matrix = vectorizer.fit_transform([text])
    row = matrix.getrow(0)
    scores = row.data
    names = vectorizer.get_feature_names_out()[row.indices]
    ranked = sorted(zip(scores, names, strict=True), key=lambda x: x[0], reverse=True)
    return [name for score, name in ranked[:top_n] if score > 0]
