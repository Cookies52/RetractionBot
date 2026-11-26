"""Deterministic status policy shared by every citation-processing path."""

from datetime import date

PRIORITY = {"correction": 1, "expression of concern": 2, "retraction": 3}


def event_kind(record):
    return " ".join((record.retraction_nature or "").split()).casefold()


def event_key(record):
    # Prefer the most recent notice of a given kind, then a useful identifier,
    # then stable content rather than whichever row MariaDB returned first.
    return (
        record.notice_date or date.min,
        bool(record.retraction_doi or record.retraction_pubmed),
        bool(record.url),
        record.retraction_doi,
        record.retraction_pubmed,
        record.url,
        record.original_doi,
        record.original_pubmed,
        record.origin,
    )


def select_record(records):
    """Return the selected notice, including reinstatement, or None to abstain.

    Retraction outranks concern and correction until a later reinstatement.
    Only notices strictly after the last reinstatement can establish a new
    status. Missing dates or conflicting same-day events around a reinstatement
    are ambiguous: do not create, change or remove a wiki tag in that case.
    Without a reinstatement, severity still works for undated legacy records.
    """
    relevant = [r for r in records if event_kind(r) in (*PRIORITY, "reinstatement")]
    reinstatements = [r for r in relevant if event_kind(r) == "reinstatement"]
    if reinstatements:
        if any(r.notice_date is None for r in relevant):
            return None
        last = max(reinstatements, key=event_key)
        active = [r for r in relevant if event_kind(r) != "reinstatement"]
        if any(r.notice_date == last.notice_date for r in active):
            return None
        relevant = [r for r in active if r.notice_date > last.notice_date]
        if not relevant:
            return last
    return max(relevant, key=lambda r: (PRIORITY[event_kind(r)], event_key(r)), default=None)
