"""Exercise the job entry point with no wiki or database credentials."""

from datetime import date
import socket
from types import SimpleNamespace

import pytest

from src.RetractionBot import retraction_bot as bot
from src.RetractionBot.db import Retraction


@pytest.fixture(autouse=True)
def prohibit_network(monkeypatch):
    def deny(*args, **kwargs):
        pytest.fail("The offline page-edit fixture attempted to use the network")

    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket, "getaddrinfo", deny)


class Page:
    def __init__(self, text):
        self.text = text
        self.saved = []

    def save(self, *args, **kwargs):
        self.saved.append(self.text)

    def title(self):
        return "Offline fixture"


@pytest.fixture
def run_bot(monkeypatch):
    record = Retraction(
        b"Crossref", b"10.1234/paper", b"10.1234/notice", b"1234", b"",
        b"Retraction", b"", date(2026, 1, 1),
    )
    settings = {
        "template_name_map": {"en": {"retraction": "Retracted"}},
        "template_field_names": {"en": {"doi": "doi", "pmid": "pmid"}},
        "summary_map": {"en": "Test retraction notice"},
        "citation_templates": {"en": ["Cite journal"]},
        "db": {},
    }
    wiki = SimpleNamespace(login=lambda: None)
    switch = SimpleNamespace(text="yes", get=lambda **kwargs: "yes", title=lambda: "run")
    database = SimpleNamespace(
        load_retracted_identifiers=lambda: [(b"10.1234/paper", b"1234")],
        retrieve_retracted_identifier=lambda identifier: [record] if identifier == "10.1234/paper" else [],
        log_retraction_edit=lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(bot, "load_bot_settings", lambda: settings)
    monkeypatch.setattr(bot, "Database", lambda *args: database)
    monkeypatch.setattr(bot.pywikibot, "Site", lambda *args: wiki)
    monkeypatch.setattr(bot.pywikibot, "Page", lambda *args: switch)
    monkeypatch.setattr(bot.time, "sleep", lambda seconds: None)

    def run(page):
        monkeypatch.setattr(bot.pagegenerators, "SearchPageGenerator", lambda *args, **kwargs: iter([page]))
        bot.run_bot()

    return run


def test_retracted_citation_is_tagged_once(run_bot):
    page = Page("<ref>{{Cite journal|doi=10.1234/paper}}</ref>")
    run_bot(page)
    assert page.saved == ["<ref>{{Cite journal|doi=10.1234/paper}}{{Retracted|doi=10.1234/notice}}</ref>"]
    run_bot(page)
    assert len(page.saved) == 1


def test_unrelated_paper_does_not_trigger_a_save(run_bot):
    page = Page("<ref>{{Cite journal|doi=10.1234/other}}</ref>")
    run_bot(page)
    assert not page.saved


@pytest.mark.parametrize("identifier, expected", [
    ((b"10.1234/a", b""), '"10.1234/a"'),
    ((b"10.1234/a", b"0"), '"10.1234/a"'),
    ((b"unavailable", b"1234"), '"1234"'),
    ((b"", b""), ""),
    ((b"10.1234/A(1):b", b"1234"), '"10.1234/a(1):b" OR "1234"'),
])
def test_search_query_uses_valid_identifiers(identifier, expected):
    assert bot.search_query(identifier) == expected


def test_killswitch_checked_again_before_save(run_bot, monkeypatch):
    values = iter([True, False])
    monkeypatch.setattr(bot, "check_bot_killswitches", lambda site: next(values))
    page = Page("<ref>{{Cite journal|doi=10.1234/paper}}</ref>")
    run_bot(page)
    assert not page.saved
