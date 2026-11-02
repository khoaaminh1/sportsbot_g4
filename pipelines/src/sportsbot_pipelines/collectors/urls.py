"""URL canonicalization and domain allow-listing.

Canonical URLs are the identity of an article across runs (the article contract's `source_url`
and the basis of `article_id`), so tracking parameters and fragments are removed while the
parameters that select content are kept. Unsafe schemes are rejected (from the prototype, PR #1).
"""

from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_PREFIXES = ("utm_", "at_")
_TRACKING_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "cmpid", "ref", "rss", "rss_source"}


def canonical_url(url: str | None) -> str | None:
    """Return the canonical http(s) URL, or None if it is not a usable article link."""
    if not url:
        return None
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return None
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return None
    query = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not k.lower().startswith(_TRACKING_PREFIXES) and k.lower() not in _TRACKING_KEYS
    ]
    netloc = parts.hostname.lower()
    if parts.port and not (
        (parts.scheme == "http" and parts.port == 80)
        or (parts.scheme == "https" and parts.port == 443)
    ):
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path or "/", urlencode(query), ""))


def host_allowed(url: str, domains: list[str]) -> bool:
    host = (urlsplit(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in domains)


def article_id(canonical: str) -> str:
    """Stable article id: hex SHA-256 of the canonical URL (article contract)."""
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
