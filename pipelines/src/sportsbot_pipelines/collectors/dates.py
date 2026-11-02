"""Parse publication dates from feeds, without guessing silently.

feedparser returns a UTC struct and treats dates without an offset as UTC, which is wrong for
sources that publish local time (a Vietnamese site at 16:30 means 09:30 UTC). So the raw string
is parsed here first; when it carries no offset, the source's own timezone is applied and the
result says so (`tz_assumed=True`), so Silver and reports can tell exact from assumed times.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

_GMT_OFFSET = re.compile(r"\b(?:GMT|UTC)\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?\b", re.IGNORECASE)
_NUMERIC = re.compile(
    r"(?P<a>\d{1,2})/(?P<b>\d{1,2})/(?P<y>\d{4})[ ,T]+(?P<H>\d{1,2}):(?P<M>\d{2})"
    r"(?::(?P<S>\d{2}))?(?:\s*(?P<ampm>[AaPp][Mm]))?"
)


@dataclass(frozen=True)
class ParsedDate:
    utc: datetime
    tz_assumed: bool

    def iso(self) -> str:
        return self.utc.isoformat()


def _finish(value: datetime, default_tz: ZoneInfo) -> ParsedDate:
    if value.tzinfo is None:
        return ParsedDate(value.replace(tzinfo=default_tz).astimezone(UTC), True)
    return ParsedDate(value.astimezone(UTC), False)


def parse_date(raw: str | None, default_tz: str = "UTC") -> ParsedDate | None:
    if not raw or not raw.strip():
        return None
    tz = ZoneInfo(default_tz)
    text = raw.strip()

    # "GMT+7", "UTC+07:00" -> "+0700" (RFC 822 parsers do not understand these).
    def _offset(match: re.Match[str]) -> str:
        sign, hours, minutes = match.group(1), int(match.group(2)), int(match.group(3) or 0)
        return f"{sign}{hours:02d}{minutes:02d}"

    normalized = _GMT_OFFSET.sub(_offset, text)

    # RFC 822 / RFC 2822 (RSS pubDate). "-0000" means "unknown offset" -> naive -> assumed.
    try:
        value = parsedate_to_datetime(normalized)
    except (TypeError, ValueError, IndexError):
        value = None
    if value is not None:
        if "-0000" in normalized:
            value = value.replace(tzinfo=None)
        return _finish(value, tz)

    # ISO 8601 (Atom), with or without offset; Python 3.11 accepts a space separator too.
    try:
        return _finish(datetime.fromisoformat(normalized.replace("Z", "+00:00")), tz)
    except ValueError:
        pass

    # Numeric dates. Vietnamese sites write day-first with a 24-hour clock ("02/11/2026 16:30");
    # a 12-hour clock with AM/PM means US month-first ("11/2/2026 1:35:00 PM", seen on Tuổi Trẻ).
    # A part above 12 settles the order regardless.
    match = _NUMERIC.search(normalized)
    if match:
        a, b = int(match["a"]), int(match["b"])
        month_first = bool(match["ampm"])
        if a > 12:
            month_first = False
        elif b > 12:
            month_first = True
        month, day = (a, b) if month_first else (b, a)
        hour = int(match["H"])
        if match["ampm"]:
            if not 1 <= hour <= 12:
                return None
            hour = hour % 12 + (12 if match["ampm"].lower() == "pm" else 0)
        try:
            value = datetime(
                int(match["y"]), month, day, hour, int(match["M"]), int(match["S"] or 0)
            )
        except ValueError:
            return None
        return _finish(value, tz)
    return None
