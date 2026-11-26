from datetime import date
from types import SimpleNamespace

import mwparserfromhell
import pytest

from src.RetractionBot.db import Retraction
from src.RetractionBot import wiki_edit as edit

MAP = {"retraction": "Retracted", "eoc": "Expression of Concern", "erratum": "Erratum"}
FIELDS = {"doi": "doi", "pmid": "pmid"}
CITE = "{{cite journal|doi=10.1234/paper}}"
TAG = "{{Retracted|doi=10.1234/notice}}"


def notice(kind="Retraction", day=2, original="10.1234/paper", pmid="1234", url="", suffix="notice"):
    return Retraction(b"Crossref", original.encode(), ("10.1234/" + suffix).encode(),
                      pmid.encode(), b"", kind.encode(), url.encode(), date(2026, 1, day))


def db(*records):
    return SimpleNamespace(retrieve_retracted_identifier=lambda key: [
        r for r in records if key in (r.original_doi, r.original_pubmed)
    ])


def apply(text, *records, fields=FIELDS, names=MAP, listed=()):
    return edit.edit_page(text, db(*records), listed, names, fields)


@pytest.mark.parametrize("name, yes", [
    ("Cite journal", True), ("cite journal", True), ("CITE_JOURNAL", True),
    ("Cite  arXiv", True), ("citeweb", True), ("#invoke:cite", True),
    ("Lien_web", True), ("Lien  web", True), ("Cited by", False),
    ("Recite", False), ("ChemSpiderCite", False), ("Book cite statistics", False),
    ("Cite needed", False),
])
def test_template_recognition(name, yes):
    assert edit.is_citation_template(name, {"lien web"}) is yes


@pytest.mark.parametrize("cite", [CITE, "{{doi|10.1234/paper}}", "{{PMID|1234}}",
                                  "{{cite journal|pmid=1234}}", "{{citeweb|doi=10.1234/PAPER}}"])
def test_every_citation_path_uses_event_selection(cite):
    text = "<ref>" + cite + "</ref>"
    result, changes = apply(text, notice("Correction", 1), notice())
    assert result == "<ref>" + cite + TAG + "</ref>"
    assert len(changes) == 1
    assert apply(result, notice(), notice("Correction", 1)) == (result, [])


def test_replaces_erratum_in_place():
    text = "<ref>" + CITE + " {{Erratum|doi=10.1234/correction}}</ref>"
    result, changes = apply(text, notice("Correction", 1, suffix="correction"), notice())
    assert result == "<ref>" + CITE + " " + TAG + "</ref>"
    assert len(changes) == 1


def test_reinstatement_removes_tag_and_later_retraction_restores_it():
    text = "<ref>" + CITE + TAG + "</ref>"
    records = [notice(day=1), notice("Reinstatement", 2)]
    result, changes = apply(text, *records)
    assert result == "<ref>" + CITE + "</ref>"
    assert changes[0].retraction_nature == "Reinstatement"
    assert apply(result, *records) == (result, [])
    assert apply(result, *records, notice(day=3))[0] == text


def test_repeated_citations_are_all_tagged_and_remain_idempotent():
    text = "<ref>" + CITE + "</ref> <ref>" + CITE + "</ref>"
    result, changes = apply(text, notice())
    assert result.count(TAG) == 2 and len(changes) == 2
    assert apply(result, notice()) == (result, [])


def test_tags_are_not_associated_across_references_or_nested_templates():
    text = "<ref>" + CITE + "</ref><ref>" + TAG + "</ref>{{box|" + CITE + "}}"
    result, changes = apply(text, notice())
    assert result == "<ref>" + CITE + TAG + "</ref><ref>" + TAG + "</ref>{{box|" + CITE + "}}"
    assert len(changes) == 1


def test_missing_standalone_parameter_does_not_reuse_previous_record():
    text = "<ref>{{doi|10.1234/paper}}{{doi}}{{PMID}}</ref>"
    result, changes = apply(text, notice())
    assert result == "<ref>{{doi|10.1234/paper}}" + TAG + "{{doi}}{{PMID}}</ref>"
    assert len(changes) == 1


def test_pmid_fallback_when_doi_has_no_record():
    cite = "{{cite journal|doi=10.1234/unknown|pmid=1234}}"
    result, changes = apply(cite, notice(original=""))
    assert result == cite + TAG and len(changes) == 1


def test_conflicting_identifiers_are_not_combined():
    cite = "{{cite journal|doi=10.1234/paper|pmid=9999}}"
    assert apply(cite, notice(), notice(original="10.1234/other", pmid="9999")) == (cite, [])


def test_doi_suffixes_and_local_template_names():
    cite = "{{Lien__web|doi=10.1234/abc(1):x;y_z}}"
    result, _ = apply(cite, notice(original="10.1234/abc(1):x;y_z"),
                      names={"retraction": "Rétracté"}, listed=["Lien web"])
    assert result == cite + "{{Rétracté|doi=10.1234/notice}}"


def test_cosmetic_differences_and_editor_fields_do_not_cause_edits():
    text = CITE + " {{retracted | checked=yes | doi= 10.1234/NOTICE |intentional=yes}}"
    assert apply(text, notice()) == (text, [])


def test_explicit_parameter_aliases_but_not_arbitrary_case_folding():
    text = CITE + "{{Retracted|DOI=10.1234/NOTICE}}"
    assert apply(text, notice(), fields={**FIELDS, "doi_aliases": ["DOI"]}) == (text, [])
    result, _ = apply(text, notice())
    assert "DOI=10.1234/NOTICE" in result and "doi=10.1234/notice" in result


def test_duplicate_managed_dois_are_repaired():
    text = CITE + "{{Retracted|doi=10.1234/old|doi=10.1234/notice}}"
    result, _ = apply(text, notice())
    assert result == CITE + TAG
    assert apply(result, notice()) == (result, [])


def test_duplicate_retwatch_numbered_links_and_https_upgrade():
    record = notice(url="http://retractionwatch.com/a")
    text = CITE + "{{Retracted|doi=10.1234/notice|1=http://retractionwatch.com/b ''Retraction Watch''|1=https://retractionwatch.com/a ''Retraction Watch''|2=editor link}}"
    result, changes = apply(text, record)
    assert len(changes) == 1
    tag = mwparserfromhell.parse(result).filter_templates()[-1]
    assert len([p for p in tag.params if str(p.name).strip() == "1"]) == 1
    assert str(tag.get("1").value).strip() == "https://retractionwatch.com/a ''Retraction Watch''"
    assert str(tag.get("2").value).strip() == "editor link"
    assert apply(result, record) == (result, [])
    old_http = CITE + "{{Retracted|doi=10.1234/notice|http://retractionwatch.com/a ''Retraction Watch''}}"
    assert "https://retractionwatch.com/a" in apply(old_http, record)[0]


def test_mixed_duplicate_numbered_fields_and_unowned_type_change_abstain():
    text = CITE + "{{Retracted|1=editor|1=https://retractionwatch.com/a ''Retraction Watch''}}"
    assert apply(text, notice()) == (text, [])
    text = CITE + "{{Erratum|editor-field=keep}}"
    assert apply(text, notice()) == (text, [])


def test_two_adjacent_tags_abstain():
    text = CITE + TAG + "{{Erratum}}"
    assert apply(text, notice()) == (text, [])


def test_conflicting_duplicate_citation_fields_prevent_edit():
    text = "{{cite journal|doi=10.1234/paper|doi=10.1234/other|pmid=1234}}"
    assert apply(text, notice()) == (text, [])
