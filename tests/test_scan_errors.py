from types import SimpleNamespace

from pywikibot.exceptions import APIError, EditConflictError, OtherPageSaveError

from src.RetractionBot import retraction_bot as bot


class Page:
    def __init__(self, error=None):
        self.text = "initial"
        self.error = error
        self.saved = []

    def title(self, *args, **kwargs):
        return "Example"

    def get(self, *, force):
        assert force
        return "latest"

    def save(self, **kwargs):
        assert kwargs["nocreate"]
        if self.error:
            error, self.error = self.error, None
            raise error(self)
        self.saved.append(self.text)


def test_conflict_recomputes_from_current_revision():
    page = Page(EditConflictError)
    switch = SimpleNamespace(enabled=lambda **kwargs: True)
    assert bot.edit_page(page, switch, "summary", lambda s: (s + " tag", []), lambda *_: None)
    assert page.saved == ["latest tag"]


def test_opt_out_skips_one_page_and_search_continues(monkeypatch):
    opted_out = Page(lambda page: OtherPageSaveError(page, "opted out"))
    allowed = Page()
    monkeypatch.setattr(bot.pagegenerators, "SearchPageGenerator", lambda *args, **kwargs: iter((opted_out, allowed)))
    switch = SimpleNamespace(enabled=lambda **kwargs: True)
    assert bot.scan_identifier("site", "query", switch, "summary", lambda s: (s + " tag", []), lambda *_: None)
    assert not opted_out.saved
    assert allowed.saved == ["initial tag"]


def test_lazy_search_error_does_not_escape(monkeypatch):
    def pages(*args, **kwargs):
        yield Page()
        raise APIError("timeout", "simulated")

    monkeypatch.setattr(bot.pagegenerators, "SearchPageGenerator", pages)
    monkeypatch.setattr(bot.time, "sleep", lambda _: None)
    switch = SimpleNamespace(enabled=lambda **kwargs: True)
    assert not bot.scan_identifier("site", "query", switch, "summary", lambda s: (s, []), lambda *_: None)
