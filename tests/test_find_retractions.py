"""
Tests for the Retraction Watch loader. The download is a fake session and the
database a fake that records what it is asked to load.
"""

import csv
import datetime
import io

import pytest
import requests

from src.RetractionBot import db, find_retractions

COLUMNS = find_retractions.REQUIRED_COLUMNS + ("Title",)

DEFAULTS = {
    "Record ID": "1",
    "OriginalPaperDate": "1/2/2020 0:00",
    "RetractionDate": "2/3/2021 0:00",
    "OriginalPaperDOI": "10.1234/abc",
    "OriginalPaperPubMedID": "0",
    "RetractionDOI": "10.1234/abc.r",
    "RetractionPubMedID": "0",
    "RetractionNature": "Retraction",
    "URLS": "https://example.org/a;",
    "Title": "A title",
}


def make_csv(*overrides):
    """CSV text with one record per dict of overrides to DEFAULTS."""
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=COLUMNS, lineterminator="\r\n")
    writer.writeheader()
    for override in overrides:
        writer.writerow({**DEFAULTS, **override})
    return out.getvalue()


class FakeResponse:
    def __init__(self, content=b"", error=None):
        self.content = content
        self.error = error

    def raise_for_status(self):
        if self.error:
            raise self.error


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


class FakeDatabase:
    def __init__(self):
        self.loaded = None

    def replace_retractions(self, rows):
        self.loaded = list(rows)
        return len(self.loaded)


# --- parsing -----------------------------------------------------------------


def test_row_layout_matches_the_table():
    (row,) = find_retractions.parse_retractions(make_csv({}))

    assert len(row) == len(db.RETRACTION_COLUMNS)
    assert row == (
        datetime.datetime(2020, 1, 2, 0, 0),
        "Crossref",
        "10.1234/abc",
        "10.1234/abc.r",
        "",
        "",
        "Retraction",
        "https://example.org/a",
        datetime.date(2021, 2, 3),
    )


def test_every_record_of_a_paper_is_kept_but_not_repeats():
    # select_record() needs to see the correction *and* the retraction.
    text = make_csv(
        {
            "Record ID": "1",
            "RetractionNature": "Correction",
            "RetractionDOI": "10.1234/c",
        },
        {
            "Record ID": "2",
            "RetractionNature": "Retraction",
            "RetractionDOI": "10.1234/r",
        },
        {
            "Record ID": "3",
            "RetractionNature": "Retraction",
            "RetractionDOI": "10.1234/r",
        },
    )

    rows = find_retractions.parse_retractions(text)

    assert [row[6] for row in rows] == ["Correction", "Retraction"]


def test_only_the_original_doi_is_lowercased():
    text = make_csv({"OriginalPaperDOI": "10.1234/ABC", "RetractionDOI": "10.1234/R"})

    (row,) = find_retractions.parse_retractions(text)

    assert row[2] == "10.1234/abc"
    assert row[3] == "10.1234/R"


def test_fields_with_line_breaks_do_not_disturb_the_next_record():
    text = make_csv(
        {"Record ID": "1", "Title": "one\nline break and a separator"},
        {"Record ID": "2", "OriginalPaperDOI": "10.1234/second"},
    )

    rows = find_retractions.parse_retractions(text)

    assert [row[2] for row in rows] == ["10.1234/abc", "10.1234/second"]


def test_pmid_only_record_is_kept():
    text = make_csv({"OriginalPaperDOI": "unavailable", "OriginalPaperPubMedID": "123"})

    (row,) = find_retractions.parse_retractions(text)

    assert row[4] == "123"


@pytest.mark.parametrize("nature", ["Retraction", "Reinstatement"])
def test_missing_notice_identifiers_keep_the_paper_status(nature):
    (row,) = find_retractions.parse_retractions(make_csv({
        "RetractionNature": nature, "RetractionDOI": "unavailable",
        "RetractionPubMedID": "0",
    }))
    assert row[2:8] == ("10.1234/abc", "", "", "", nature, "")


@pytest.mark.parametrize(
    "bad",
    [
        {"OriginalPaperDOI": "unavailable", "OriginalPaperPubMedID": "0"},
        {"OriginalPaperDOI": "", "OriginalPaperPubMedID": ""},
        {"RetractionNature": ""},
        {"OriginalPaperDOI": "not a DOI", "OriginalPaperPubMedID": "not a PMID"},
    ],
)
def test_unusable_records_are_skipped(bad):
    text = make_csv(bad, {"OriginalPaperDOI": "10.1234/good"})

    rows = find_retractions.parse_retractions(text)

    assert [row[2] for row in rows] == ["10.1234/good"]


def test_truncated_record_rejects_snapshot():
    text = make_csv({}) + "7,1/2/2020 0:00,10.1234/short\r\n"

    with pytest.raises(ValueError, match="Malformed Retraction Watch row"):
        find_retractions.parse_retractions(text)


def test_malformed_quotes_reject_snapshot():
    with pytest.raises(csv.Error):
        find_retractions.parse_retractions(make_csv({}) + '"unterminated')


def test_unknown_nature_does_not_prevent_refresh():
    (row,) = find_retractions.parse_retractions(
        make_csv({"RetractionNature": "Unknown"})
    )
    assert row[6] == "Unknown"


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1/2/2020 0:00", datetime.datetime(2020, 1, 2)),
        ("01/02/2020 13:45", datetime.datetime(2020, 1, 2, 13, 45)),
        ("1/2/2020", datetime.datetime(2020, 1, 2)),
        ("", db.PLACEHOLDER_TIMESTAMP),
        ("not a date", db.PLACEHOLDER_TIMESTAMP),
        ("1/1/1900 0:00", db.PLACEHOLDER_TIMESTAMP),
    ],
)
def test_parse_timestamp(value, expected):
    assert find_retractions.parse_timestamp(value) == expected


def test_record_with_an_unreadable_date_is_still_loaded():
    # The whole record used to be dropped, DOI and all.
    (row,) = find_retractions.parse_retractions(make_csv({"OriginalPaperDate": ""}))

    assert row[0] == db.PLACEHOLDER_TIMESTAMP


def test_urls_are_cut_between_urls_not_inside_one():
    assert find_retractions.fit_urls("http://a;http://b;http://c", 18) == (
        "http://a;http://b"
    )
    assert find_retractions.fit_urls("http://a;;", 100) == "http://a"
    assert find_retractions.fit_urls("http://a-very-long-url", 5) == ""


def test_overlong_url_field_is_shortened_to_fit_the_column():
    urls = ";".join("https://example.org/%04d" % n for n in range(400))
    assert len(urls) > db.COLUMN_LIMITS["url"]

    (row,) = find_retractions.parse_retractions(make_csv({"URLS": urls}))

    assert len(row[7].encode("utf-8")) <= db.COLUMN_LIMITS["url"]
    assert row[7].startswith("https://example.org/0000;https://example.org/0001")
    db.check_lengths([row])


@pytest.mark.parametrize("text", ["", "<html>Not Found</html>", "a,b,c\r\n1,2,3\r\n"])
def test_other_files_are_rejected(text):
    with pytest.raises(ValueError, match="no column"):
        find_retractions.parse_retractions(text)


# --- reloading ---------------------------------------------------------------


@pytest.fixture
def few_records_are_enough(monkeypatch):
    monkeypatch.setattr(find_retractions, "MIN_RETRACTIONS", 2)


def test_download_sends_a_user_agent_and_a_timeout(few_records_are_enough):
    csv_bytes = make_csv({}, {"OriginalPaperDOI": "10.1234/two"}).encode("utf-8")
    session = FakeSession(FakeResponse(csv_bytes))

    find_retractions.get_crossref_retractions(FakeDatabase(), session)

    [(url, kwargs)] = session.calls
    assert url == find_retractions.RETRACTION_WATCH_URL
    assert kwargs["headers"]["User-Agent"] == find_retractions.user_agent
    assert kwargs["timeout"]


def test_reload_hands_the_rows_to_the_database(few_records_are_enough):
    csv_bytes = make_csv({}, {"OriginalPaperDOI": "10.1234/two"}).encode("utf-8")
    database = FakeDatabase()

    count = find_retractions.get_crossref_retractions(
        database, FakeSession(FakeResponse(csv_bytes))
    )

    assert count == 2
    assert [row[2] for row in database.loaded] == ["10.1234/abc", "10.1234/two"]


def test_byte_order_mark_does_not_break_the_first_column(few_records_are_enough):
    body = make_csv({}, {"OriginalPaperDOI": "10.1234/two"}).encode("utf-8")
    csv_bytes = b"\xef\xbb\xbf" + body
    database = FakeDatabase()

    find_retractions.get_crossref_retractions(
        database, FakeSession(FakeResponse(csv_bytes))
    )

    assert len(database.loaded) == 2


def test_http_error_keeps_the_existing_table():
    # The old script emptied the table first and only then downloaded.
    error = requests.HTTPError("503 Server Error")
    database = FakeDatabase()

    with pytest.raises(requests.HTTPError):
        find_retractions.get_crossref_retractions(
            database, FakeSession(FakeResponse(b"", error))
        )

    assert database.loaded is None


def test_error_page_keeps_the_existing_table():
    page = b"<html><body>Rate limited</body></html>"
    database = FakeDatabase()

    with pytest.raises(ValueError):
        find_retractions.get_crossref_retractions(
            database, FakeSession(FakeResponse(page))
        )

    assert database.loaded is None


def test_invalid_utf8_keeps_the_existing_table():
    database = FakeDatabase()
    with pytest.raises(UnicodeDecodeError):
        find_retractions.get_crossref_retractions(
            database, FakeSession(FakeResponse(b"\xff"))
        )
    assert database.loaded is None


def test_suspiciously_small_file_keeps_the_existing_table():
    csv_bytes = make_csv({}, {"OriginalPaperDOI": "10.1234/two"}).encode("utf-8")
    database = FakeDatabase()

    with pytest.raises(RuntimeError, match="keeping the existing table"):
        find_retractions.get_crossref_retractions(
            database, FakeSession(FakeResponse(csv_bytes))
        )

    assert database.loaded is None


# --- entry point ---------------------------------------------------------------


def test_main_reports_failure_with_a_nonzero_status(monkeypatch):
    def fail():
        raise OSError("bot_settings.yml not found")

    monkeypatch.setattr(find_retractions, "load_bot_settings", fail)

    assert find_retractions.main() == 1


def test_main_returns_zero_after_a_reload(monkeypatch):
    reloaded = []
    monkeypatch.setattr(find_retractions, "load_bot_settings", lambda: {"db": {}})
    monkeypatch.setattr(find_retractions, "Database", lambda settings: FakeDatabase())
    monkeypatch.setattr(
        find_retractions,
        "get_crossref_retractions",
        lambda database: reloaded.append(database),
    )

    assert find_retractions.main() == 0
    assert len(reloaded) == 1
