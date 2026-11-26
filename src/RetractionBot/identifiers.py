"""Validation shared by the Retraction Watch import and citation matching."""

import re
from urllib.parse import urlsplit, urlunsplit


def is_doi(value):
    return bool(re.fullmatch(r'10\.[0-9]{4,}(?:\.[0-9]+)*/[^\s"<>\\]+', value))


def is_pmid(value):
    return bool(re.fullmatch(r"[1-9][0-9]*", value))


def normalize_notice_url(value):
    """Accept a plain HTTP(S) URL safe to put in a wikitext parameter."""
    value = value.strip()
    if not value or re.search(r"[\s\x00-\x1f\x7f\[\]{}|<>\\]", value):
        return ""
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        port = parsed.port
    except ValueError:
        return ""
    if parsed.scheme not in ("http", "https") or not host or parsed.username is not None:
        return ""
    if parsed.scheme == "http" and host in ("retractionwatch.com", "www.retractionwatch.com") and port is None:
        return urlunsplit(parsed._replace(scheme="https"))
    return value
