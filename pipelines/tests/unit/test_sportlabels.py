"""Weak sport labels and metrics for the sport classifier (pipelines/spark/jobs/sportlabels.py)."""

from __future__ import annotations

import sys
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[2] / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import sportlabels  # noqa: E402


def test_feed_rules_cover_every_per_sport_feed():
    import json

    sources = json.loads((JOBS_DIR.parents[1] / "sources.json").read_text(encoding="utf-8"))
    per_sport = {
        "guardian_tennis": "tennis",
        "guardian_formulaone": "other_sport",
        "guardian_nba": "basketball",
        "guardian_nfl": "other_sport",
        "guardian_football": "football",
        "espn_soccer": "football",
        "marca_futbol": "football",
        "bbc_sport_snooker": "other_sport",
        "espn_mlb": "other_sport",
        "espn_nhl": "other_sport",
        "espn_rpm": "other_sport",
        "bbc_sport_basketball": "basketball",
    }
    ids = {s["id"] for s in sources["sources"]}
    for sid, expected in per_sport.items():
        assert sid in ids
        assert sportlabels.weak_label(sid, "", "") == (expected, "feed")


def test_general_feeds_fall_back_to_url_then_title():
    assert sportlabels.weak_label(
        "vov_the_thao", "https://vov.vn/the-thao/bong-da-viet-nam/x-1.vov", ""
    ) == (
        "football",
        "url",
    )
    assert sportlabels.weak_label(
        "dantri_the_thao", "https://dantri.com.vn/the-thao/tennis/a.htm", ""
    ) == (
        "tennis",
        "url",
    )
    assert sportlabels.weak_label(
        "vnexpress_the_thao", "https://vnexpress.net/abc-123.html", "Bóng chuyền nữ Việt Nam thắng"
    ) == ("volleyball", "title")
    assert sportlabels.weak_label(
        "vnexpress_the_thao",
        "https://vnexpress.net/abc-123.html",
        "Nguyễn Thùy Linh vào tứ kết cầu lông",
    ) == ("badminton", "title")
    # no rule: general feed, flat URL, no keyword
    assert (
        sportlabels.weak_label("vnexpress_the_thao", "https://vnexpress.net/abc-123.html", "Messi")
        is None
    )
    # football words in the title are not a rule (labels come from metadata)
    assert (
        sportlabels.weak_label("thanhnien_the_thao", "https://thanhnien.vn/a-1.htm", "Bóng đá")
        is None
    )


def test_nfl_is_not_football_and_classes_are_the_api_ones():
    assert sportlabels.weak_label("espn_nfl", "", "")[0] == "other_sport"
    assert (
        sportlabels.weak_label("x", "https://www.theguardian.com/sport/nfl/2026/x", "")[0]
        == "other_sport"
    )
    assert set(sportlabels.CLASSES) == {
        "football",
        "volleyball",
        "basketball",
        "tennis",
        "badminton",
        "other_sport",
    }


def test_scores_on_a_hand_computed_example():
    pairs = [("football", "football")] * 3 + [("football", "tennis"), ("tennis", "tennis")]
    s = sportlabels.scores(pairs)
    assert s["n"] == 5 and s["accuracy"] == 0.8
    assert s["per_class"]["football"] == {
        "support": 4,
        "precision": 1.0,
        "recall": 0.75,
        "f1": 0.8571,
    }
    assert s["per_class"]["tennis"] == {"support": 1, "precision": 0.5, "recall": 1.0, "f1": 0.6667}
    assert s["macro_f1"] == round((0.8571 + 0.6667) / 2, 4)
    assert s["confusion"] == {"football": {"football": 3, "tennis": 1}, "tennis": {"tennis": 1}}
    assert sportlabels.scores([]) == {"n": 0}


def test_scores_by_language():
    rows = [
        ("vi", "football", "football"),
        ("vi", "tennis", "football"),
        ("en", "tennis", "tennis"),
    ]
    by = sportlabels.scores_by(rows)
    assert by["en"] == {"n": 1, "accuracy": 1.0, "macro_f1": 1.0}
    assert by["vi"]["n"] == 2 and by["vi"]["accuracy"] == 0.5
