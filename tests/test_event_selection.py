from datetime import date
from itertools import permutations
from types import SimpleNamespace

import pytest

from src.RetractionBot.events import event_kind, select_record


def notice(kind, day=None, suffix="n"):
    return SimpleNamespace(
        origin="Crossref", original_doi="10.1234/paper", original_pubmed="1234",
        retraction_nature=kind, notice_date=date(2026, 1, day) if day else None,
        retraction_doi="10.1234/" + suffix, retraction_pubmed="", url="",
    )


@pytest.mark.parametrize("events, expected", [
    ([notice("Correction", 1), notice("Retraction", 2)], "retraction"),
    ([notice("Expression of concern", 1), notice("Retraction", 2)], "retraction"),
    ([notice("Retraction", 1), notice("Correction", 2)], "retraction"),
    ([notice("Retraction", 1), notice("Reinstatement", 2)], "reinstatement"),
    ([notice("Retraction", 1), notice("Reinstatement", 2), notice("Retraction", 3)], "retraction"),
    ([notice("Retraction", 1), notice("Reinstatement", 2), notice("Correction", 3)], "correction"),
    ([notice("Retraction", 2), notice("Reinstatement", 2)], None),
    ([notice("Retraction"), notice("Reinstatement", 2)], None),
    ([notice("Correction"), notice("RETRACTION")], "retraction"),
    ([notice("Unrecognized", 1)], None),
    ([], None),
])
def test_event_priority_is_independent_of_row_order(events, expected):
    for ordering in permutations(events):
        result = select_record(ordering)
        assert (event_kind(result) if result else None) == expected


def test_multiple_same_type_notices_have_stable_selection():
    records = [notice("Retraction", 1, "z"), notice("Retraction", 2, "a"),
               notice("Retraction", 2, "b")]
    for ordering in permutations(records):
        assert select_record(ordering).retraction_doi == "10.1234/b"
