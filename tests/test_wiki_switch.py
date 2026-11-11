from types import SimpleNamespace

from src.RetractionBot import retraction_bot


def test_switch_reads_fresh_state_before_save(monkeypatch):
    state = SimpleNamespace(text="yes", reads=0)

    def get(*, force):
        assert force
        state.reads += 1
        return state.text

    monkeypatch.setattr(retraction_bot.pywikibot, "Page", lambda *args: SimpleNamespace(get=get, title=lambda: "run"))
    switch = retraction_bot.WikiSwitch("enwiki")
    assert switch.enabled()
    state.text = "no"
    assert switch.enabled()
    assert not switch.enabled(fresh=True)
    assert not switch.enabled()
    assert state.reads == 2


def test_missing_switch_denies_edits(monkeypatch):
    class MissingSwitch:
        def title(self, *args, **kwargs):
            return "run"

        def get(self, *, force):
            raise retraction_bot.pywikibot.exceptions.NoPageError(self)

    monkeypatch.setattr(retraction_bot.pywikibot, "Page", lambda *args: MissingSwitch())
    assert not retraction_bot.WikiSwitch("enwiki").enabled()
