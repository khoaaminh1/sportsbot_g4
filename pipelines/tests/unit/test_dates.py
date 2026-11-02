from __future__ import annotations

import pytest

from sportsbot_pipelines.collectors.dates import parse_date

VN = "Asia/Ho_Chi_Minh"


@pytest.mark.parametrize(
    ("raw", "tz", "expected_utc", "assumed"),
    [
        # RFC 822 with an offset: exact, no assumption.
        ("Mon, 02 Nov 2026 16:30:00 +0700", VN, "2026-11-02T09:30:00+00:00", False),
        ("Mon, 02 Nov 2026 09:30:00 GMT", VN, "2026-11-02T09:30:00+00:00", False),
        # "GMT+7" is not RFC 822; feedparser returns nothing for it.
        ("Mon, 02 Nov 2026 16:30:00 GMT+7", VN, "2026-11-02T09:30:00+00:00", False),
        ("Mon, 02 Nov 2026 16:30:00 UTC+07:00", VN, "2026-11-02T09:30:00+00:00", False),
        # ISO 8601.
        ("2026-11-02T16:30:00+07:00", VN, "2026-11-02T09:30:00+00:00", False),
        ("2026-11-02T09:30:00Z", VN, "2026-11-02T09:30:00+00:00", False),
        # No offset: the source's timezone is applied and flagged (feedparser would say 16:30 UTC).
        ("2026-11-02 16:30:00", VN, "2026-11-02T09:30:00+00:00", True),
        ("02/11/2026 16:30:00", VN, "2026-11-02T09:30:00+00:00", True),
        ("Thứ hai, 02/11/2026 16:30", VN, "2026-11-02T09:30:00+00:00", True),
        ("Mon, 02 Nov 2026 16:30:00 -0000", VN, "2026-11-02T09:30:00+00:00", True),
        # London is UTC+0 in November.
        ("2026-11-02 09:30:00", "Europe/London", "2026-11-02T09:30:00+00:00", True),
    ],
)
def test_parse_date(raw, tz, expected_utc, assumed):
    parsed = parse_date(raw, tz)
    assert parsed is not None
    assert parsed.iso() == expected_utc
    assert parsed.tz_assumed is assumed


@pytest.mark.parametrize("raw", [None, "", "   ", "yesterday", "32/13/2026 25:61"])
def test_unparseable_dates_return_none(raw):
    assert parse_date(raw, VN) is None
