from types import SimpleNamespace

import pytest

from src.RetractionBot.page_edits import process_page


def notice(doi="10.1234/a", pmid="123", nature="Retraction", notice_doi="10.1234/notice", notice_date=None):
    return SimpleNamespace(
        original_doi=doi, original_pubmed=pmid, retraction_nature=nature,
        retraction_doi=notice_doi, retraction_pubmed="", url="", notice_date=notice_date,
    )


def edit(text, records):
    def lookup(identifier):
        assert identifier not in {"", "0", "unavailable"}
        return [r for r in records if identifier in (r.original_doi, r.original_pubmed)]

    return process_page(
        text, lookup, {"retraction": "Retracted"}, {}, ["Cite journal"]
    )


FLAG = "{{Retracted|doi=10.1234/notice}}"
CITE = "{{Cite journal|doi=10.1234/a}}"


def test_tag_belongs_only_to_its_citation_container():
    text = f"<ref>{CITE}</ref> prose <ref>{{{{Retracted|doi=10.1234/other}}}}</ref>"
    updated, changes = edit(text, [notice()])
    assert updated == text.replace(CITE, CITE + FLAG)
    assert [c.action for c in changes] == ["add"]


def test_identical_citations_and_nested_templates_are_idempotent():
    citation = "{{Cite journal|doi=10.1234/a|title=H{{sub|2}}O}}"
    original = f"<ref>{citation}{FLAG}</ref><ref>{citation}</ref>"
    expected = f"<ref>{citation}{FLAG}</ref><ref>{citation}{FLAG}</ref>"
    assert edit(original, [notice()])[0] == expected
    assert edit(expected, [notice()]) == (expected, [])


def test_two_different_templates_for_one_paper_both_get_flags():
    text = f"<ref>{CITE}</ref><ref>{{{{Cite journal|doi=10.1234/a|title=Title}}}}</ref>"
    assert edit(text, [notice()])[0].count(FLAG) == 2


@pytest.mark.parametrize("value", ["{{DOI}}", "{{PMID|1=}}", "{{PMID|0}}", "{{Cite journal|doi=unavailable}}"])
def test_empty_and_placeholder_ids_cannot_reuse_a_notice(value):
    text = f"<ref>{value}</ref><ref>{CITE}</ref><ref>{value}</ref>"
    assert edit(text, [notice(), notice("unavailable", "0")])[0] == text.replace(CITE, CITE + FLAG)


def test_pmid_only_page_and_placeholder_doi_fallback():
    text = "<ref>{{Cite journal|doi=unavailable|pmid=123}}</ref><ref>{{Cite journal|pmid=456}}</ref>"
    second = notice("", "456", notice_doi="10.1234/second")
    updated, changes = edit(text, [notice("", "123"), second])
    assert len(changes) == 2
    assert FLAG in updated and "{{Retracted|doi=10.1234/second}}" in updated


def test_conflicting_valid_doi_prevents_a_false_pmid_match():
    text = "<ref>{{Cite journal|doi=10.1234/other|pmid=123}}</ref>"
    assert edit(text, [notice()]) == (text, [])


@pytest.mark.parametrize("citation", ["{{DOI|10.1234/a}}", "{{PMID|123}}"])
def test_standalone_citation_tags_are_updated_in_place(citation):
    text = f"<ref>{citation} <!-- keep --> {{{{Retracted|doi=10.1234/old|pmc=234}}}}</ref>"
    updated, changes = edit(text, [notice()])
    assert "10.1234/notice|pmc=234" in updated and "<!-- keep -->" in updated
    assert [c.action for c in changes] == ["update"]


def test_unrelated_template_cannot_change_a_notice():
    text = "<ref>{{Unrelated|doi=10.1234/a}}{{Retracted|doi=10.1234/old}}</ref>"
    assert edit(text, [notice()]) == (text, [])
