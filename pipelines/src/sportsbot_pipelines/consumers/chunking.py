"""Split article text into overlapping chunks on paragraph and sentence boundaries.

Sizes are in characters (no tokenizer needed): ~1200 characters is roughly 250-350 tokens for
Vietnamese and English, well inside BGE-M3's window and small enough to cite precisely.
"""

from __future__ import annotations

import re

_SENTENCE = re.compile(r"(?<=[.!?…])\s+")


def _units(text: str, max_chars: int) -> list[str]:
    units: list[str] = []
    for para in (p.strip() for p in text.split("\n")):
        if not para:
            continue
        if len(para) <= max_chars:
            units.append(para)
            continue
        for sentence in _SENTENCE.split(para):
            while len(sentence) > max_chars:  # a very long "sentence": hard split
                units.append(sentence[:max_chars])
                sentence = sentence[max_chars:]
            if sentence.strip():
                units.append(sentence.strip())
    return units


def chunk_text(text: str, max_chars: int = 1200, overlap_chars: int = 200) -> list[str]:
    units = _units(text, max_chars)
    chunks: list[str] = []
    current: list[str] = []
    size = 0
    for unit in units:
        if current and size + len(unit) + 1 > max_chars:
            chunks.append("\n".join(current))
            # carry the tail of the previous chunk forward as overlap
            tail: list[str] = []
            tail_size = 0
            for prev in reversed(current):
                if tail_size + len(prev) > overlap_chars:
                    break
                tail.insert(0, prev)
                tail_size += len(prev) + 1
            current, size = tail, tail_size
        current.append(unit)
        size += len(unit) + 1
    if current:
        chunks.append("\n".join(current))
    return chunks
