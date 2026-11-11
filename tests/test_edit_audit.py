from types import SimpleNamespace

from pymysql import OperationalError

from src.RetractionBot import retraction_bot as bot
from src.RetractionBot.page_edits import PageChange
from test_page_edits import notice


def test_logs_real_title_identifiers_and_action_after_a_save():
    calls = []
    db = SimpleNamespace(log_retraction_edit=lambda *args, **kwargs: calls.append((args, kwargs)))
    page = SimpleNamespace(title=lambda: "A real page title")
    change = PageChange(notice("", "234"), "remove")
    bot.log_page_changes(db, "fr", page, [change])
    [(args, details)] = calls
    assert args[1:] == ("fr.wikipedia.org", "A real page title", "", "234")
    assert details == {"notice_doi": "10.1234/notice", "notice_pmid": "", "action": "remove"}


def test_log_failure_leaves_the_successful_wiki_save_alone(caplog):
    def fail(*args, **kwargs):
        raise OperationalError("fixture")

    db = SimpleNamespace(log_retraction_edit=fail)
    page = SimpleNamespace(title=lambda: "Already saved")
    bot.log_page_changes(db, "en", page, [PageChange(notice(), "add")])
    assert "Wiki edit saved" in caplog.text
