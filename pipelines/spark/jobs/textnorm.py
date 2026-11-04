"""Pure-Python text helpers for the Spark Silver job (Task 6, docs/handoff/VNP.md).

Importable without Spark so the rules can be unit-tested (pipelines/tests/unit/test_textnorm.py)
and shipped unchanged to the executors (spark-submit --py-files).
Must stay compatible with Python 3.10 (the Spark image's Python): no 3.11-only syntax.

Rules fixed BEFORE any run (do not tune them after seeing results):
- Clean text: Unicode NFC, whitespace collapsed to single spaces, stripped. Case, accents
  and punctuation are kept, so Silver stays readable.
- Shingles: lower-cased, accent-preserving word tokens (runs of Unicode letters/digits, so
  punctuation is dropped and "2-1" gives "2", "1"); sliding windows of SHINGLE_SIZE words,
  de-duplicated and sorted. A text shorter than SHINGLE_SIZE words gives one shingle holding
  all its words; a text without words gives none.
- Near duplicates: Jaccard similarity of the shingle sets >= NEAR_DUPLICATE_JACCARD, found
  with MinHashLSH using MINHASH_NUM_HASH_TABLES hash tables.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable

SHINGLE_SIZE = 3
NEAR_DUPLICATE_JACCARD = 0.8
MINHASH_NUM_HASH_TABLES = 5

_WHITESPACE = re.compile(r"\s+")
# \w is Unicode-aware for str patterns: letters (with their accents, once NFC-composed), digits
# and "_". Underscores are not words in news text, so they are split off too.
_WORD = re.compile(r"[^\W_]+")


def normalize_text(value: str | None) -> str:
    """NFC-compose, collapse whitespace and strip; None or blank input gives ""."""
    if not value:
        return ""
    return _WHITESPACE.sub(" ", unicodedata.normalize("NFC", value)).strip()


def tokens(value: str | None) -> list[str]:
    """Lower-cased, accent-preserving word tokens of the normalised text."""
    return _WORD.findall(normalize_text(value).lower())


def word_shingles(value: str | None, n: int = SHINGLE_SIZE) -> list[str]:
    """Sorted, unique word n-grams (joined by one space) of the normalised, lower-cased text."""
    if n < 1:
        raise ValueError(f"shingle size must be >= 1, got {n}")
    words = tokens(value)
    if not words:
        return []
    if len(words) < n:
        return [" ".join(words)]
    return sorted({" ".join(words[i : i + n]) for i in range(len(words) - n + 1)})


def strip_leading_title(text: str | None, title: str | None) -> str:
    """Normalised text without a copy of the title at its start, compared word by word.

    Some extractors (VnExpress pages) repeat the title as the first words of the text; that copy
    would add the title's shingles twice. Words are compared lower-cased after NFC, so case,
    spacing and punctuation do not matter, and a partial last word never matches. When the text
    is only the title, it is returned unchanged so it still has words to shingle.
    """
    clean = normalize_text(text)
    title_words = tokens(title)
    if not title_words:
        return clean
    matches = list(_WORD.finditer(clean))
    if len(matches) <= len(title_words):
        return clean
    head = [m.group(0).lower() for m in matches[: len(title_words)]]
    if head != title_words:
        return clean
    return clean[matches[len(title_words) - 1].end() :].strip()


def jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    """|A ∩ B| / |A ∪ B| of two shingle collections; two empty collections give 0.0."""
    set_a, set_b = set(a), set(b)
    union = set_a | set_b
    if not union:
        return 0.0
    return len(set_a & set_b) / len(union)
