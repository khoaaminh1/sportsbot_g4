"""Main-text extraction from an article page (trafilatura) plus text normalisation helpers."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass

_SPACE = re.compile(r"[ \t ]+")
_BLANKS = re.compile(r"\n{3,}")
_WORD = re.compile(r"\w+", re.UNICODE)


@dataclass(frozen=True)
class ExtractedPage:
    title: str | None
    text: str
    author: str | None
    published: str | None  # as found in the page metadata; parsed by the caller
    language: str | None


def clean_text(text: str) -> str:
    text = unicodedata.normalize("NFC", text)
    lines = [_SPACE.sub(" ", line).strip() for line in text.splitlines()]
    return _BLANKS.sub("\n\n", "\n".join(lines)).strip()


def content_hash(text: str) -> str:
    """Hash of the text ignoring case, spacing and punctuation: catches exact re-posts."""
    words = _WORD.findall(unicodedata.normalize("NFC", text).lower())
    return hashlib.sha256(" ".join(words).encode("utf-8")).hexdigest()


def word_count(text: str) -> int:
    return len(_WORD.findall(text))


def extract_page(html: bytes | str, url: str) -> ExtractedPage | None:
    """Return the main article text and metadata, or None when no main text is found."""
    import trafilatura  # imported here: heavy import, only the extractor needs it

    doc = trafilatura.bare_extraction(
        html,
        url=url,
        with_metadata=True,
        include_comments=False,
        include_tables=False,
        favor_precision=True,
    )
    if doc is None:
        return None
    data = doc.as_dict() if hasattr(doc, "as_dict") else dict(doc)
    text = clean_text(data.get("text") or "")
    if not text:
        return None
    return ExtractedPage(
        title=(data.get("title") or None),
        text=text,
        author=(data.get("author") or None),
        published=(data.get("date") or None),
        language=(data.get("language") or None),
    )
