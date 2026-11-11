"""
Tests for db.Database against a fake pymysql connection that records the SQL
it is given. No database server is needed.
"""

import datetime

import pytest

from src.RetractionBot import db

TIMESTAMP = datetime.datetime(2020, 1, 2, 3, 4)


def squash(sql):
    return " ".join(sql.split())


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.lock_query = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def execute(self, sql, params=None):
        self.lock_query = sql.startswith("SELECT GET_LOCK")
        self.connection.log.append(("execute", squash(sql), params))
        if self.connection.fail_on and self.connection.fail_on in sql:
            raise RuntimeError("simulated failure")

    def executemany(self, sql, seq_of_params):
        self.connection.log.append(("executemany", squash(sql), list(seq_of_params)))
        if self.connection.fail_on == "executemany":
            raise RuntimeError("simulated failure")
        return len(seq_of_params) - self.connection.lost_rows

    def fetchall(self):
        return self.connection.result

    def fetchone(self):
        if self.lock_query:
            return (self.connection.lock_result,)
        return self.connection.result[0] if self.connection.result else None


class FakeConnection:
    def __init__(self):
        self.log = []
        self.result = []
        self.fail_on = None
        self.open = True
        self.lock_result = 1
        self.lost_rows = 0

    def cursor(self):
        return FakeCursor(self)

    def ping(self, reconnect=False):
        pass

    def commit(self):
        self.log.append(("commit",))

    def statements(self):
        """The SQL text of everything executed, in order."""
        return [entry[1] for entry in self.log if entry[0] != "commit"]


@pytest.fixture
def connection(monkeypatch):
    fake = FakeConnection()
    monkeypatch.setattr(db.pymysql, "connect", lambda **kwargs: fake)
    return fake


@pytest.fixture
def database(connection):
    return db.Database({"host": "localhost", "name": "test"})


def make_row(**overrides):
    values = {
        "timestamp": TIMESTAMP,
        "origin": "Crossref",
        "original_doi": "10.1234/abc",
        "retraction_doi": "10.1234/abc.r",
        "original_pmid": "0",
        "retraction_pmid": "0",
        "retraction_nature": "Retraction",
        "url": "",
        "notice_date": None,
    }
    values.update(overrides)
    return tuple(values[column] for column in db.RETRACTION_COLUMNS)


# --- lookups -----------------------------------------------------------------


@pytest.mark.parametrize(
    "identifier", ["", " ", "0", " 0 ", "unavailable", "Unavailable", "-1", "１２３"]
)
def test_empty_identifiers_are_not_looked_up(database, connection, identifier):
    # "original_pmid = '0'" matches nearly every record, so a citation with
    # |pmid=0 used to be flagged with whichever paper came first.
    assert database.retrieve_retracted_identifier(identifier) == []
    assert connection.log == []


def test_lookup_folds_the_doi_and_names_its_columns(database, connection):
    connection.result = [
        (b"Crossref", b"10.1234/abc", b"10.1234/r", b"0", b"0", b"Retraction", b"")
    ]

    records = database.retrieve_retracted_identifier(" 10.1234/ABC ")

    [(_, sql, params)] = connection.log
    assert "SELECT *" not in sql
    assert sql.startswith("SELECT origin, original_doi, retraction_doi, original_pmid")
    assert "WHERE original_doi=%s" in sql
    assert params == ("10.1234/abc",)
    assert len(records) == 1
    assert records[0].original_doi == "10.1234/abc"
    assert records[0].retraction_nature == "Retraction"
    assert records[0].original_pubmed == "0"


def test_pmid_uses_its_own_indexed_column(database, connection):
    database.retrieve_retracted_identifier(" 12345 ")
    [(_, sql, params)] = connection.log
    assert "WHERE original_pmid=%s" in sql
    assert params == ("12345",)


def test_each_paper_is_listed_once(database, connection):
    connection.result = [(b"10.1/a", b"0")]

    assert database.load_retracted_identifiers() == [(b"10.1/a", b"0")]

    [(_, sql, _)] = connection.log
    assert "GROUP BY original_doi, original_pmid" in sql


# --- edit log ------------------------------------------------------------------


def test_edit_log_insert_names_its_columns(database, connection):
    database.log_retraction_edit(
        TIMESTAMP,
        "en.wikipedia.org",
        "Some page",
        "10.1234/abc",
        "12345",
        notice_doi="10.1234/notice",
        notice_pmid="67890",
        action="update",
    )

    [(_, sql, params), (commit,)] = connection.log
    assert commit == "commit"
    assert (
        "(timestamp, domain, page_title, original_doi, retraction_doi, "
        "original_pmid, retraction_pmid, action)"
    ) in sql
    # The PMID used to land in the retraction_doi column.
    assert params == (
        TIMESTAMP,
        "en.wikipedia.org",
        "Some page",
        "10.1234/abc",
        "10.1234/notice",
        "12345",
        "67890",
        "update",
    )


# --- reloading -----------------------------------------------------------------


def test_reload_builds_a_new_table_and_swaps_it_in(database, connection):
    rows = [make_row(), make_row(original_doi="10.1234/def")]

    database.replace_retractions(rows)

    statements = connection.statements()
    assert statements[0].startswith("SELECT GET_LOCK")
    assert statements[1:3] == [
        "DROP TABLE IF EXISTS retractions_new, retractions_old",
        "CREATE TABLE retractions_new LIKE retractions",
    ]
    assert "STRICT_ALL_TABLES" in statements[3]
    assert statements[4].startswith(
        "INSERT INTO retractions_new (timestamp, origin, original_doi,"
    )
    assert statements[5:7] == [
        "RENAME TABLE retractions TO retractions_old, retractions_new TO retractions",
        "DROP TABLE retractions_old",
    ]
    assert statements[-1].startswith("DO RELEASE_LOCK")
    inserted = [entry for entry in connection.log if entry[0] == "executemany"]
    assert inserted[0][2] == rows
    # The new table is committed before it replaces the live one.
    commit_at = connection.log.index(("commit",))
    rename_at = next(
        i
        for i, entry in enumerate(connection.log)
        if entry[0] == "execute" and entry[1].startswith("RENAME")
    )
    assert commit_at < rename_at
    # Nothing ever empties the live table.
    assert not any("TRUNCATE" in s or "DELETE" in s for s in connection.statements())


def test_reload_inserts_in_batches(database, connection):
    rows = [make_row(original_doi="10.1234/%d" % n) for n in range(2500)]

    database.replace_retractions(rows)

    sizes = [len(entry[2]) for entry in connection.log if entry[0] == "executemany"]
    assert sizes == [1000, 1000, 500]


def test_failed_insert_leaves_the_live_table_alone(database, connection):
    connection.fail_on = "executemany"

    with pytest.raises(RuntimeError):
        database.replace_retractions([make_row()])

    statements = connection.statements()
    assert not any("RENAME" in s for s in statements)
    assert statements[-1].startswith("DO RELEASE_LOCK")


def test_failed_swap_leaves_the_live_table_alone(database, connection):
    connection.fail_on = "RENAME"

    with pytest.raises(RuntimeError):
        database.replace_retractions([make_row()])

    assert connection.statements()[-1].startswith("DO RELEASE_LOCK")


@pytest.mark.parametrize(
    "row, error",
    [
        (make_row(original_doi="x" * 201), "original_doi"),
        (make_row()[:-1], "expected 9"),
    ],
)
def test_bad_rows_are_rejected_before_staging(database, connection, row, error):
    with pytest.raises(ValueError, match=error):
        database.replace_retractions([row])
    assert len(connection.statements()) == 2
    assert connection.statements()[-1].startswith("DO RELEASE_LOCK")


def test_empty_snapshot_cannot_replace_live_table(database, connection):
    with pytest.raises(ValueError, match="no usable rows"):
        database.replace_retractions([])
    assert len(connection.statements()) == 2


def test_partial_insert_cannot_publish(database, connection):
    connection.lost_rows = 1
    with pytest.raises(RuntimeError, match="insert count"):
        database.replace_retractions([make_row()])
    assert not any("RENAME" in sql for sql in connection.statements())


def test_concurrent_refresh_does_not_fetch_or_touch_staging(database, connection):
    connection.lock_result = 0

    def rows():
        pytest.fail("competing refresh fetched rows")
        yield

    with pytest.raises(RuntimeError, match="refresh lock"):
        database.replace_retractions(rows())
    assert len(connection.statements()) == 1


def test_lengths_are_counted_in_bytes():
    db.check_lengths([make_row(original_doi="a" * 200)])  # fits exactly

    with pytest.raises(ValueError):
        # 101 characters, 202 bytes
        db.check_lengths([make_row(original_doi="é" * 101)])


# --- timestamps ------------------------------------------------------------------


@pytest.mark.parametrize(
    "timestamp, expected",
    [
        (datetime.datetime(2020, 5, 6, 7, 8), datetime.datetime(2020, 5, 6, 7, 8)),
        (datetime.datetime(1971, 1, 1), datetime.datetime(1971, 1, 1)),
        (datetime.datetime(1960, 1, 1), db.PLACEHOLDER_TIMESTAMP),
        (datetime.datetime(2050, 1, 1), db.PLACEHOLDER_TIMESTAMP),
    ],
)
def test_clamp_timestamp(timestamp, expected):
    assert db.clamp_timestamp(timestamp) == expected
