"""Keyword retrieval (BM25) over stored articles, accent-insensitive.

A deliberately simple baseline for the prototype: Vietnamese diacritics are removed on both sides
so "doi tuyen" matches "đội tuyển", and a few aliases expand the query ("VN" -> "viet nam").
Phase 4 replaces it with hybrid BM25 + multilingual vectors (see the Phase 1 assessment).
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass

from sportsbot_agent.store import Article

_NON_WORD = re.compile(r"[^0-9a-z]+")

STOPWORDS = {
    # Vietnamese (accent-stripped)
    "la",
    "va",
    "cua",
    "co",
    "cac",
    "nhung",
    "cho",
    "voi",
    "trong",
    "mot",
    "nao",
    "gi",
    "ve",
    "the",
    "khong",
    "duoc",
    "nhat",
    "moi",
    "nay",
    "do",
    "da",
    "dang",
    "se",
    "thi",
    "ma",
    "o",
    "tu",
    "den",
    "ra",
    "vao",
    "khi",
    "nhu",
    "hay",
    "bao",
    "nhieu",
    "ai",
    "sao",
    "tin",
    "tuc",
    "toi",
    "biet",
    "hoi",
    "gan",
    "day",
    "hien",
    # English
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "that",
    "to",
    "was",
    "were",
    "what",
    "which",
    "who",
    "will",
    "with",
    "about",
    "latest",
    "news",
    "me",
    "tell",
    "show",
    "any",
    "recent",
    "today",
}
# "da" is also half of "bong da"; keep it when it follows "bong".
_KEEP_AFTER = {("bong", "da")}

ALIASES = {
    "vn": ["viet", "nam"],
    "vietnam": ["viet", "nam"],
    "indo": ["indonesia"],
    "mu": ["manchester", "united"],
    "football": ["bong", "da"],
    "soccer": ["bong", "da", "football"],
    "volleyball": ["bong", "chuyen"],
    "basketball": ["bong", "ro"],
    "badminton": ["cau", "long"],
    "tennis": ["quan", "vot"],
}


def normalize(text: str) -> str:
    text = text.lower().replace("đ", "d")
    text = "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")
    return _NON_WORD.sub(" ", text).strip()


def tokenize(text: str) -> list[str]:
    words = normalize(text).split()
    out = []
    for i, w in enumerate(words):
        prev = words[i - 1] if i else ""
        if w in STOPWORDS and (prev, w) not in _KEEP_AFTER:
            continue
        out.append(w)
    return out


def query_terms(question: str) -> list[str]:
    terms: list[str] = []
    for t in tokenize(question):
        terms.append(t)
        terms.extend(ALIASES.get(t, []))
    return list(dict.fromkeys(terms))  # unique, order kept


@dataclass(frozen=True)
class Hit:
    article: Article
    score: float
    matched_terms: int


class BM25Index:
    def __init__(self, articles: list[Article], k1: float = 1.5, b: float = 0.75) -> None:
        self.articles = articles
        self.k1, self.b = k1, b
        self.docs = [Counter(tokenize(f"{a.title} {a.title} {a.summary}")) for a in articles]
        self.lengths = [sum(d.values()) for d in self.docs]
        self.avg_len = (sum(self.lengths) / len(self.lengths)) if self.lengths else 0.0
        df: Counter[str] = Counter()
        for d in self.docs:
            df.update(d.keys())
        n = len(self.docs)
        self.idf = {t: math.log(1 + (n - f + 0.5) / (f + 0.5)) for t, f in df.items()}

    def search(self, question: str, k: int = 5, min_matched_terms: int = 1) -> list[Hit]:
        terms = query_terms(question)
        if not terms or not self.docs:
            return []
        # Long questions must match more than one stray word.
        needed = max(min_matched_terms, math.ceil(0.34 * len(terms)) if len(terms) >= 4 else 1)
        hits = []
        for doc, length, article in zip(self.docs, self.lengths, self.articles, strict=True):
            matched = [t for t in terms if t in doc]
            if len(matched) < needed:
                continue
            score = 0.0
            for t in matched:
                tf = doc[t]
                denom = tf + self.k1 * (1 - self.b + self.b * length / (self.avg_len or 1))
                score += self.idf[t] * tf * (self.k1 + 1) / denom
            hits.append(Hit(article, score, len(matched)))
        # Best score first; newer articles first on ties.
        hits.sort(key=lambda h: (h.score, h.article.published_at or ""), reverse=True)
        return hits[:k]
