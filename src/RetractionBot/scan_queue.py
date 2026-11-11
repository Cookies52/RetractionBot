"""Store per-wiki progress across scheduled scans."""

from contextlib import contextmanager, closing
from dataclasses import dataclass
import fcntl
import hashlib
from pathlib import Path
import sqlite3
import time

from .db import fold_doi
from .identifiers import is_doi, is_pmid


@dataclass(frozen=True)
class ScanItem:
    doi: str
    pmid: str
    fingerprint: str
    latest: str


def build_snapshot(rows):
    grouped = {}
    for doi, pmid, notice_doi, notice_pmid, nature, url, notice_date in rows:
        doi = doi.decode("utf-8").strip()
        pmid = pmid.decode("utf-8").strip()
        doi = fold_doi(doi) if is_doi(doi) else ""
        pmid = pmid if is_pmid(pmid) else ""
        if not (doi or pmid):
            continue
        date_text = notice_date.isoformat() if notice_date else ""
        grouped.setdefault((doi, pmid), set()).add(
            (date_text, notice_doi, notice_pmid, nature, url)
        )

    items = []
    for (doi, pmid), notices in grouped.items():
        records = sorted(notices)
        fingerprint = hashlib.sha256(repr(records).encode("utf-8")).hexdigest()
        items.append(ScanItem(doi, pmid, fingerprint, records[-1][0]))
    return items


@contextmanager
def open_scan_queue(filename):
    path = Path(filename).expanduser()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.with_name(path.name + ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield None
            return
        with closing(sqlite3.connect(path)) as connection:
            yield ScanQueue(connection)


class ScanQueue:
    def __init__(self, connection):
        self.connection = connection
        with connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS scan_queue (
                    wiki TEXT NOT NULL,
                    doi TEXT NOT NULL,
                    pmid TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    latest TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    checked_at INTEGER NOT NULL DEFAULT 0,
                    attempted_at INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (wiki, doi, pmid)
                )
            """)

    def sync(self, wiki, items):
        generation = time.time_ns()
        with self.connection:
            self.connection.executemany("""
                INSERT INTO scan_queue (wiki, doi, pmid, fingerprint, latest, generation)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(wiki, doi, pmid) DO UPDATE SET
                    checked_at = CASE WHEN fingerprint = excluded.fingerprint THEN checked_at ELSE 0 END,
                    attempted_at = CASE WHEN fingerprint = excluded.fingerprint THEN attempted_at ELSE 0 END,
                    fingerprint = excluded.fingerprint,
                    latest = excluded.latest,
                    generation = excluded.generation
            """, ((wiki, item.doi, item.pmid, item.fingerprint, item.latest, generation)
                  for item in items))
            self.connection.execute(
                "DELETE FROM scan_queue WHERE wiki = ? AND generation != ?",
                (wiki, generation),
            )

    def work(self, wiki, limit):
        return self.connection.execute("""
            SELECT doi, pmid FROM scan_queue WHERE wiki = ?
            ORDER BY (checked_at != 0), MAX(checked_at, attempted_at), latest DESC, doi, pmid
            LIMIT ?
        """, (wiki, limit)).fetchall()

    def started(self, wiki, doi, pmid):
        with self.connection:
            self.connection.execute("""
                UPDATE scan_queue SET attempted_at = ?
                WHERE wiki = ? AND doi = ? AND pmid = ?
            """, (time.time_ns(), wiki, doi, pmid))

    def finished(self, wiki, doi, pmid):
        with self.connection:
            self.connection.execute("""
                UPDATE scan_queue SET checked_at = ?
                WHERE wiki = ? AND doi = ? AND pmid = ?
            """, (time.time_ns(), wiki, doi, pmid))
