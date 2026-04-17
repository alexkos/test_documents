from __future__ import annotations

import re
from collections import Counter

_STOPWORDS = frozenset(
    """
    the a an and or of to in for on with as at by from into through during before after
    above below between under again further then once here there when where why how all
    each both few more most other some such no nor not only own same so than too very
    can will just should now longer body text short abstract
    """.split()
)


def extract_keywords(text: str | None, top_n: int = 12) -> list[str]:
    if not text:
        return []
    words = re.findall(r"[a-zA-Z][a-zA-Z\-]{2,}", text.lower())
    filtered = [w for w in words if w not in _STOPWORDS]
    counts = Counter(filtered)
    return [w for w, _ in counts.most_common(top_n)]
