"""Weak sport labels and classification metrics for the sport classifier (plan task 7).

Pure Python, no Spark: shipped with the job and unit-tested on its own.

RULES, fixed before the first run (never tuned after seeing results). Labels come from metadata
(feed and URL), never from the article text the classifier learns from, so a model cannot score
well by copying its own labelling rule. First matching rule wins:

1. Feed: the source_id of a per-sport feed (guardian_tennis, bbc_sport_formula1, espn_nba, ...).
2. URL path: a sport segment in the article URL (Dân Trí /the-thao/bong-da/, Guardian /football/).
3. Title keyword, only for volleyball and badminton: no feed covers them, so without this rule the
   training set would hold none. These two classes are the weakest; the report says so.
Anything else (general sports feeds with flat URLs) has no weak label and is not used for training.

Classes are the API's sport categories (docs/contracts/api.openapi.yaml):
football, volleyball, basketball, tennis, badminton, other_sport. American football (NFL),
F1, cricket, rugby, golf, cycling, athletics, boxing, snooker, swimming, baseball and ice hockey
are other_sport.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from urllib.parse import urlsplit

CLASSES = ("football", "volleyball", "basketball", "tennis", "badminton", "other_sport")

# 1. per-sport feeds: substring of source_id -> class (checked in this order)
_FEED_RULES: tuple[tuple[str, str], ...] = (
    ("tennis", "tennis"),
    ("nba", "basketball"),
    ("basketball", "basketball"),
    ("nfl", "other_sport"),  # before "football": American football
    ("football", "football"),
    ("soccer", "football"),
    ("futbol", "football"),
    ("formula1", "other_sport"),
    ("formulaone", "other_sport"),
    ("rpm", "other_sport"),
    ("cricket", "other_sport"),
    ("rugby", "other_sport"),
    ("golf", "other_sport"),
    ("cycling", "other_sport"),
    ("athletics", "other_sport"),
    ("boxing", "other_sport"),
    ("snooker", "other_sport"),
    ("swimming", "other_sport"),
    ("mlb", "other_sport"),
    ("nhl", "other_sport"),
)

# 2. URL path segments -> class
_URL_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"/(bong-chuyen|volleyball)(/|-|$)"), "volleyball"),
    (re.compile(r"/(cau-long|badminton)(/|-|$)"), "badminton"),
    (re.compile(r"/(tennis)(/|-|$)"), "tennis"),
    (re.compile(r"/(bong-ro|basketball|nba)(/|-|$)"), "basketball"),
    (
        re.compile(
            r"/(nfl|formula1|formulaone|f1|cricket|rugby|golf|cycling|boxing|athletics)(/|$)"
        ),
        "other_sport",
    ),
    (re.compile(r"/(bong-da|bongda|football|soccer|futbol)(/|-|$)"), "football"),
)

# 3. title keywords, only for the classes without a feed
_TITLE_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b(bóng chuyền|volleyball|voleibol)\b", re.IGNORECASE), "volleyball"),
    (re.compile(r"\b(cầu lông|badminton|bádminton)\b", re.IGNORECASE), "badminton"),
)


def weak_label(source_id: str | None, url: str | None, title: str | None) -> tuple[str, str] | None:
    """(class, rule) for an article, or None when no rule applies."""
    sid = (source_id or "").lower()
    for needle, label in _FEED_RULES:
        if needle in sid:
            return label, "feed"
    path = urlsplit(url or "").path.lower()
    for pattern, label in _URL_RULES:
        if pattern.search(path):
            return label, "url"
    for pattern, label in _TITLE_RULES:
        if pattern.search(title or ""):
            return label, "title"
    return None


# ------------------------------------------------------------------- metrics -----


def confusion(pairs: Iterable[tuple[str, str]]) -> dict[str, dict[str, int]]:
    """{true: {predicted: count}} over the classes that occur."""
    table: dict[str, Counter[str]] = {}
    for true, pred in pairs:
        table.setdefault(true, Counter())[pred] += 1
    return {t: dict(c) for t, c in sorted(table.items())}


def scores(pairs: Iterable[tuple[str, str]]) -> dict[str, object]:
    """Accuracy, macro-F1 over the true classes present, and per-class precision/recall/F1."""
    pairs = list(pairs)
    if not pairs:
        return {"n": 0}
    labels = sorted({t for t, _ in pairs})
    per_class = {}
    for c in labels:
        tp = sum(1 for t, p in pairs if t == c and p == c)
        fp = sum(1 for t, p in pairs if t != c and p == c)
        fn = sum(1 for t, p in pairs if t == c and p != c)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        per_class[c] = {
            "support": tp + fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
        }
    return {
        "n": len(pairs),
        "accuracy": round(sum(1 for t, p in pairs if t == p) / len(pairs), 4),
        "macro_f1": round(sum(v["f1"] for v in per_class.values()) / len(per_class), 4),
        "per_class": per_class,
        "confusion": confusion(pairs),
    }


def scores_by(rows: Iterable[tuple[str, str, str]]) -> dict[str, object]:
    """scores() per group for (group, true, predicted) rows, e.g. per language."""
    groups: dict[str, list[tuple[str, str]]] = {}
    for group, true, pred in rows:
        groups.setdefault(group, []).append((true, pred))
    return {
        g: {k: v for k, v in scores(p).items() if k in {"n", "accuracy", "macro_f1"}}
        for g, p in sorted(groups.items())
    }
