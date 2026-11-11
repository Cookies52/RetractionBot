import pytest

from src.RetractionBot.page_edits import process_page

from test_page_edits import notice


@pytest.mark.parametrize("nature, expected", [("Retraction", "Rétracté"), ("Expression of concern", "Expression de préoccupation")])
def test_french_uses_configured_tag_names_and_is_idempotent(nature, expected):
    mapping = {"retraction": "Rétracté", "eoc": "Expression de préoccupation"}
    text = "<ref>{{Article|doi=10.1234/a}}</ref>"
    lookup = lambda identifier: [notice(nature=nature)]
    changed, changes = process_page(text, lookup, mapping, {}, ["Article"])
    assert len(changes) == 1
    assert f"{{{{{expected}|doi=10.1234/notice}}}}" in changed
    assert process_page(changed, lookup, mapping, {}, ["Article"]) == (changed, [])


def test_french_updates_existing_english_fallback():
    mapping = {"retraction": "Rétracté"}
    text = "<ref>{{Article|doi=10.1234/a}}{{Retracted|doi=10.1234/notice}}</ref>"
    changed, changes = process_page(text, lambda identifier: [notice()], mapping, {}, ["Article"])
    assert changed == text.replace("Retracted", "Rétracté")
    assert [change.action for change in changes] == ["update"]
