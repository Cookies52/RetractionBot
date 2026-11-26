"""Opt-in real MariaDB tests, using a fresh disposable database per test."""

import datetime
import os
import threading
import uuid
from pathlib import Path

import pymysql
import pytest

from src.RetractionBot.db import Database

pytestmark = pytest.mark.skipif(
    os.environ.get("RETBOT_MARIADB_TEST") != "1",
    reason="Requires a disposable MariaDB server; opt in explicitly",
)


@pytest.fixture
def connections():
    options = {
        "host": os.environ.get("RETBOT_MARIADB_HOST", "127.0.0.1"),
        "port": int(os.environ.get("RETBOT_MARIADB_PORT", "3306")),
        "user": "root",
        "password": os.environ["RETBOT_MARIADB_PASSWORD"],
        "autocommit": True,
        "connect_timeout": 10,
        "read_timeout": 15,
        "write_timeout": 15,
    }
    name = "retbot_test_" + uuid.uuid4().hex
    admin = pymysql.connect(**options)
    db_connection = observer = None
    try:
        with admin.cursor() as cur:
            cur.execute(f"CREATE DATABASE `{name}`")
        db_connection = pymysql.connect(database=name, **options)
        observer = pymysql.connect(database=name, **options)
        schema = (Path(__file__).resolve().parents[1] / "schema.sql").read_text()
        # Only the first CREATE TABLE statement is required for this test.
        with db_connection.cursor() as cur:
            cur.execute(schema.split(";", 1)[0])
        database = Database.__new__(Database)
        database._db = db_connection
        database.allow_large_snapshot_shrink = False
        yield database, observer
    finally:
        if observer is not None:
            observer.close()
        if db_connection is not None:
            db_connection.close()
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS `{name}`")
        admin.close()


def row(index=0, timestamp=None):
    return (timestamp if timestamp is not None else datetime.datetime(2020, 1, 1),
            "Crossref", f"10.1234/paper{index}", "10.1234/notice", "", "",
            "Retraction", "", datetime.date(2021, 2, 3))


def count(connection):
    with connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM retractions")
        return cur.fetchone()[0]


def test_real_swap_and_notice_date(connections):
    database, observer = connections
    assert database.replace_retractions([row()]) == 1
    assert count(observer) == 1
    [record] = database.retrieve_retracted_identifier("10.1234/PAPER0")
    assert record.notice_date == datetime.date(2021, 2, 3)
    assert database.replace_retractions([row(i) for i in range(5)]) == 5
    assert count(observer) == 5
    with observer.cursor() as cur:
        cur.execute("SHOW TABLES LIKE 'retractions_%'")
        assert cur.fetchall() == ()


def test_real_lock_and_failed_stage_leave_live_snapshot(connections):
    database, observer = connections
    database.replace_retractions([row()])
    with observer.cursor() as cur:
        cur.execute("SELECT GET_LOCK(CONCAT(DATABASE(), '.retractions.refresh'), 0)")
        assert cur.fetchone()[0] == 1
        try:
            with pytest.raises(RuntimeError, match="refresh lock"):
                database.replace_retractions([row(1)])
        finally:
            cur.execute("DO RELEASE_LOCK(CONCAT(DATABASE(), '.retractions.refresh'))")
    with pytest.raises(pymysql.MySQLError):
        database.replace_retractions([row(1, timestamp="not a timestamp")])
    assert count(observer) == 1
    assert database.retrieve_retracted_identifier("10.1234/paper0")
    assert not database.retrieve_retracted_identifier("10.1234/paper1")
    assert database.replace_retractions([row(1)]) == 1
    assert database.retrieve_retracted_identifier("10.1234/paper1")


def test_real_reader_observes_only_complete_snapshots(connections):
    database, observer = connections
    database.replace_retractions([row()])
    stop = threading.Event()
    started = threading.Event()
    observations = []
    errors = []

    def read():
        try:
            while not stop.is_set():
                observations.append(count(observer))
                started.set()
        except Exception as exc:
            errors.append(exc)
            started.set()

    thread = threading.Thread(target=read)
    thread.start()
    try:
        assert started.wait(10)
        database.replace_retractions([row(i) for i in range(5000)])
    finally:
        stop.set()
        thread.join(20)
    assert not thread.is_alive()
    assert not errors
    observations.append(count(observer))
    assert {1, 5000} <= set(observations) <= {1, 5000}
