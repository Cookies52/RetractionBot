"""Which notice wins when one paper has several (a correction, then a retraction).

These tests run the real run_bot() and the real db.py queries. Wikipedia is
stubbed out and MySQL is replaced by an in-memory SQLite database, so the
ORDER BY in retrieve_retracted_identifier() is exercised, not mocked away.
Without an ORDER BY, SQLite returns rows in insertion order, as the bot's
own table does, and the bot inserts rows in Retraction Watch file order.

The data is the real Retraction Watch history of Kotz et al. 2024, which the
bot tagged {{Erratum}} on three English Wikipedia articles in October 2026,
before replacing the tag with {{Retracted}} on a later visit.
"""

import datetime
import itertools
import os
import socket
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

# Importing pywikibot looks for a user-config.py. These tests never create a
# Site, so tell pywikibot not to look.
os.environ.setdefault("PYWIKIBOT_NO_USER_CONFIG", "2")

from src.RetractionBot import db, retraction_bot  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

PAPER_DOI = "10.1038/s41586-024-07219-0"
PAPER_PMID = "38632481"
RW_STORY = (
    "https://retractionwatch.com/2025/12/03/"
    "authors-retract-nature-paper-projecting-high-costs-of-climate-change/"
)
RW_FOLLOW_UP = (
    "https://retractionwatch.com/2026/09/02/"
    "oxford-researchers-retract-nature-paper-on-atmospheric-thirst-for-calculation-errors/"
)

# Notices as Retraction Watch lists them. Correction and Retraction are the
# paper's real rows. The expression of concern is made up, to test the middle
# rank.
NOTICES = {
    "Correction": ("10.1038/s41586-024-07732-2", "38914865", RW_STORY),
    "Retraction": (
        "10.1038/s41586-025-09726-0",
        "41339560",
        RW_STORY + ";" + RW_FOLLOW_UP,
    ),
    "Expression of concern": ("10.5555/example-concern", "0", ""),
}
TEMPLATE_NAMES = {
    "Correction": "Erratum",
    "Retraction": "Retracted",
    "Expression of concern": "Expression of Concern",
}

CITE_DOI = (
    "{{Cite journal |last1=Kotz |title=The economic commitment of climate change "
    f"|journal=Nature |date=2024 |doi={PAPER_DOI} |pmid={PAPER_PMID}}}}}"
)
CITE_PMID_ONLY = (
    "{{Cite journal |last1=Kotz |title=The economic commitment of climate change "
    f"|journal=Nature |date=2024 |pmid={PAPER_PMID}}}}}"
)
DOI_TEMPLATE = "{{doi|" + PAPER_DOI + "}}"
PMID_TEMPLATE = "{{pmid|" + PAPER_PMID + "}}"

# The bot skips a page unless some <ref> on it contains a DOI, so every test
# page carries this unrelated reference as well.
OTHER_REF = "<ref>{{Cite journal |title=Unrelated |doi=10.1000/unrelated}}</ref>"


def tag(nature, **editor_fields):
    """The status template the bot writes for a notice, as wikitext."""
    notice_doi, notice_pmid, urls = NOTICES[nature]
    parts = [TEMPLATE_NAMES[nature], "doi=" + notice_doi]
    if notice_pmid != "0":
        parts.append("pmid=" + notice_pmid)
    parts += [url + " ''Retraction Watch''" for url in urls.split(";") if url]
    parts += [f"{name}={value}" for name, value in editor_fields.items()]
    return "{{" + "|".join(parts) + "}}"


def article(citation):
    return f'Lead sentence.<ref name="Kotz2024">{citation}</ref> More.{OTHER_REF}'


# --- A stand-in for pymysql, backed by SQLite --------------------------------

# The columns of schema.sql's tables, in order. TEXT, so that comparisons in
# SQLite behave like MariaDB's on varbinary.
SCHEMA = """
CREATE TABLE retractions (
    timestamp TEXT, origin TEXT, original_doi TEXT, retraction_doi TEXT,
    original_pmid TEXT, retraction_pmid TEXT, retraction_nature TEXT, url TEXT
);
CREATE TABLE edit_log (
    timestamp TEXT, domain TEXT, page_title TEXT, original_doi TEXT,
    retraction_doi TEXT, original_pmid TEXT, retraction_pmid TEXT
);
"""


def _sql_param(value):
    if value is None or isinstance(value, (str, bytes, int, float)):
        return value
    if isinstance(value, datetime.datetime):
        return value.isoformat(sep=" ")
    return str(value)  # pymysql also sends unknown objects as their str()


class _Cursor:
    def __init__(self, connection):
        self._cursor = connection.cursor()

    def execute(self, query, params=()):
        query = query.replace("%s", "?").replace("RAND()", "RANDOM()")
        self._cursor.execute(query, tuple(_sql_param(p) for p in params))

    def fetchall(self):
        # The bot's columns are varbinary, so pymysql hands back bytes.
        return [
            tuple(v.encode() if isinstance(v, str) else v for v in row)
            for row in self._cursor.fetchall()
        ]

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else None


class _Connection:
    def __init__(self, connection):
        self._connection = connection

    def cursor(self):
        return _Cursor(self._connection)

    def ping(self, reconnect=False):
        pass

    def commit(self):
        self._connection.commit()


# --- Fixtures -----------------------------------------------------------------


class Page:
    def __init__(self, text):
        self.text = text
        self.saved = []

    def save(self, *args, **kwargs):
        self.saved.append(self.text)

    def title(self):
        return "Offline fixture"


@pytest.fixture(autouse=True)
def prohibit_network(monkeypatch):
    def deny(*args, **kwargs):
        pytest.fail("A test attempted to use the network")

    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket, "getaddrinfo", deny)


@pytest.fixture
def run_pass(monkeypatch):
    """Run one full bot pass over `page`, with `notices` in the database.

    `notices` are loaded in the given order, which stands for the order of
    Retraction Watch's file. The bot visits the page once per notice.
    """
    connection = sqlite3.connect(":memory:")
    connection.executescript(SCHEMA)
    monkeypatch.setattr(db.pymysql, "connect", lambda **kwargs: _Connection(connection))

    with open(ROOT / "bot_settings.yml") as settings_file:
        settings = yaml.safe_load(settings_file)
    settings["template_name_map"] = {"en": settings["template_name_map"]["en"]}
    monkeypatch.setattr(retraction_bot, "load_bot_settings", lambda: settings)

    wiki = SimpleNamespace(login=lambda: None)
    switch = SimpleNamespace(text="yes")
    monkeypatch.setattr(retraction_bot.pywikibot, "Site", lambda *args, **kwargs: wiki)
    monkeypatch.setattr(
        retraction_bot.pywikibot, "Page", lambda *args, **kwargs: switch
    )
    monkeypatch.setattr(retraction_bot.time, "sleep", lambda seconds: None)

    def run(page, notices):
        connection.execute("DELETE FROM retractions")
        database = db.Database(settings["db"])
        for nature in notices:
            notice_doi, notice_pmid, urls = NOTICES[nature]
            database.save_retraction_to_db(
                timestamp=datetime.datetime(2024, 4, 17),
                origin="Crossref",
                original_doi=PAPER_DOI,
                retraction_doi=notice_doi,
                original_pmid=PAPER_PMID,
                retraction_pmid=notice_pmid,
                retraction_nature=nature,
                url=urls,
            )
        monkeypatch.setattr(
            retraction_bot.pagegenerators,
            "SearchPageGenerator",
            lambda *args, **kwargs: iter([page]),
        )
        retraction_bot.run_bot()
        return page

    return run


def orders(*natures):
    return [
        pytest.param(list(order), id="-".join(n.split()[0] for n in order))
        for order in itertools.permutations(natures)
    ]


# --- First tag on an untagged citation ---------------------------------------


@pytest.mark.parametrize(
    "citation",
    [
        pytest.param(CITE_DOI, id="cite-template-doi"),
        pytest.param(CITE_PMID_ONLY, id="cite-template-pmid"),
        pytest.param(DOI_TEMPLATE, id="doi-template"),
        pytest.param(PMID_TEMPLATE, id="pmid-template"),
    ],
)
def test_first_tag_is_retracted_when_correction_is_listed_first(run_pass, citation):
    page = run_pass(Page(article(citation)), ["Correction", "Retraction"])
    # One edit, straight to {{Retracted}}: no {{Erratum}} first, and no
    # second tag next to it.
    assert page.saved == [article(citation + tag("Retraction"))]


def case(notices, expected):
    return pytest.param(notices, expected, id="-".join(n.split()[0] for n in notices))


@pytest.mark.parametrize(
    "notices, expected",
    [
        case(["Correction", "Retraction"], "Retraction"),
        case(["Retraction", "Correction"], "Retraction"),
        case(["Expression of concern", "Retraction"], "Retraction"),
        case(["Correction", "Expression of concern"], "Expression of concern"),
        case(["Correction", "Expression of concern", "Retraction"], "Retraction"),
    ],
)
def test_first_tag_follows_notice_priority(run_pass, notices, expected):
    page = run_pass(Page(article(CITE_DOI)), notices)
    assert page.saved == [article(CITE_DOI + tag(expected))]


# --- A citation that already has a tag ---------------------------------------


@pytest.mark.parametrize(
    "notices",
    orders("Correction", "Retraction")
    + orders("Correction", "Expression of concern", "Retraction"),
)
def test_existing_erratum_is_replaced_by_retracted(run_pass, notices):
    page = run_pass(Page(article(CITE_DOI + tag("Correction"))), notices)
    assert page.saved == [article(CITE_DOI + tag("Retraction"))]


@pytest.mark.parametrize("field", ["checked", "intentional"])
def test_replacing_erratum_keeps_editor_fields(run_pass, field):
    # Effects of climate change had {{Erratum|...|checked=yes}}. The bot
    # replaced it on 16 November 2026 and kept the editor's field:
    # https://en.wikipedia.org/w/index.php?diff=1385925540&oldid=1383746048
    before = article(CITE_DOI + tag("Correction", **{field: "yes"}))
    page = run_pass(Page(before), ["Correction", "Retraction"])
    assert page.saved == [article(CITE_DOI + tag("Retraction", **{field: "yes"}))]


@pytest.mark.parametrize("notices", orders("Correction", "Retraction"))
def test_existing_retracted_is_left_alone(run_pass, notices):
    page = run_pass(Page(article(CITE_DOI + tag("Retraction"))), notices)
    assert page.saved == []


# --- Correction first, retraction listed later --------------------------------


def test_correction_then_later_retraction(run_pass):
    page = Page(article(CITE_DOI))

    run_pass(page, ["Correction"])
    assert page.text == article(CITE_DOI + tag("Correction"))

    run_pass(page, ["Correction", "Retraction"])
    assert page.text == article(CITE_DOI + tag("Retraction"))
    assert len(page.saved) == 2
