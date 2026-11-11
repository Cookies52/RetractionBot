from datetime import date

import pytest

from test_page_edits import CITE, FLAG, edit, notice


def test_reinstatement_removes_only_its_adjacent_tag_and_is_idempotent():
    original = f"<ref>{CITE}{FLAG}</ref><ref>{FLAG}</ref>"
    records = [
        notice(notice_date=date(2020, 8, 1)),
        notice(nature="Reinstatement", notice_date=date(2020, 9, 1)),
    ]
    changed, changes = edit(original, records)
    assert changed == f"<ref>{CITE}</ref><ref>{FLAG}</ref>"
    assert [change.action for change in changes] == ["remove"]
    assert edit(changed, records) == (changed, [])


def test_retraction_after_a_reinstatement_is_added_again():
    original = f"<ref>{CITE}</ref>"
    records = [
        notice(nature="Reinstatement", notice_date=date(2020, 9, 1)),
        notice(notice_date=date(2021, 1, 1)),
    ]
    assert edit(original, records)[0] == f"<ref>{CITE}{FLAG}</ref>"


@pytest.mark.parametrize("reinstated", [None, date(2020, 8, 1)])
def test_ambiguous_histories_are_left_for_review(reinstated):
    original = f"<ref>{CITE}{FLAG}</ref>"
    records = [
        notice(nature="Reinstatement", notice_date=reinstated),
        notice(notice_date=date(2020, 8, 1)),
    ]
    assert edit(original, records) == (original, [])
