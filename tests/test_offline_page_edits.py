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
def run_bot(monkeypatch, tmp_path):
    record = Retraction(
        b"Crossref", b"10.1234/paper", b"10.1234/notice", b"1234", b"",
        b"Retraction", b"",
    )
    record.notice_date = date(2026, 1, 1)
    settings = {
        "template_name_map": {"en": {"retraction": "Retracted"}},
        "template_field_names": {"en": {"doi": "doi", "pmid": "pmid"}},
        "summary_map": {"en": "Test retraction notice"},
        "citation_templates": {"en": ["Cite journal"]},
        "db": {},
        "scan": {"state_file": str(tmp_path / "offline.sqlite3")},
    }
    wiki = SimpleNamespace(login=lambda: None)
    switch = SimpleNamespace(text="yes", get=lambda **kwargs: "yes", title=lambda: "run")
    database = SimpleNamespace(
        load_retracted_identifiers=lambda: [(b"10.1234/paper", b"1234")],
        load_scan_rows=lambda: [
            (b"10.1234/paper", b"1234", b"10.1234/notice", b"", b"Retraction", b"", record.notice_date)
        ],
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
