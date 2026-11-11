import csv
import datetime
import io
import logging
import sys

import requests

from .db import (
    COLUMN_LIMITS,
    PLACEHOLDER_TIMESTAMP,
    Database,
    clamp_timestamp,
    fold_doi,
)
from .identifiers import is_doi, is_pmid
from .settings import configure_logging, load_bot_settings

logger = logging.getLogger(__name__)

user_agent = "RetractionBot (https://github.com/cookies52/RetractionBot; mailto:matthewdann52@gmail.com)"

RETRACTION_WATCH_URL = "https://gitlab.com/crossref/retraction-watch-data/-/raw/main/retraction_watch.csv?ref_type=heads"

# Columns this script reads. A file without them (an error page, a renamed
# column) must not replace the data we have.
REQUIRED_COLUMNS = (
    "Record ID",
    "OriginalPaperDate",
    "RetractionDate",
    "OriginalPaperDOI",
    "OriginalPaperPubMedID",
    "RetractionDOI",
    "RetractionPubMedID",
    "RetractionNature",
    "URLS",
)

# The real file has tens of thousands of records. Far fewer means the download
# or the parsing went wrong, and the existing table is kept.
MIN_RETRACTIONS = 1000


def fit_urls(urls, limit):
    """
    Returns the ";"-separated ``urls`` without the last ones until the text is
    at most ``limit`` bytes long. Cutting in the middle would leave a broken URL.
    """
    parts = [part.strip() for part in urls.split(";") if part.strip()]
    while parts and len(";".join(parts).encode("utf-8")) > limit:
        parts.pop()
    return ";".join(parts)


def parse_csv_date(value):
    for date_format in ("%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(value.strip(), date_format)
        except ValueError:
            continue
    return None


def parse_timestamp(value):
    """An unreadable publication date is not a reason to drop the row."""
    timestamp = parse_csv_date(value)
    return clamp_timestamp(timestamp) if timestamp is not None else PLACEHOLDER_TIMESTAMP


def row_from_record(record):
    """
    Turns a CSV record into a row for Database.replace_retractions(), in the
    order of db.RETRACTION_COLUMNS. Returns None for a record the bot cannot use.
    """
    original_doi = fold_doi(record["OriginalPaperDOI"].strip())
    original_pmid = record["OriginalPaperPubMedID"].strip()
    retraction_doi = record["RetractionDOI"].strip()
    retraction_pmid = record["RetractionPubMedID"].strip()
    nature = record["RetractionNature"].strip()

    if not nature or not (is_doi(original_doi) or is_pmid(original_pmid)):
        return None
    # A notice can have no DOI or PMID. Keep the status for a known paper,
    # while omitting unverified links from rows without a notice identifier.
    notice_doi = retraction_doi if is_doi(retraction_doi) else ""
    notice_pmid = retraction_pmid if is_pmid(retraction_pmid) else ""
    urls = record["URLS"] if notice_doi or notice_pmid else ""

    notice_date = parse_csv_date(record["RetractionDate"])
    return (
        parse_timestamp(record["OriginalPaperDate"]),
        "Crossref",
        original_doi if is_doi(original_doi) else "",
        notice_doi,
        original_pmid if is_pmid(original_pmid) else "",
        notice_pmid,
        nature,
        fit_urls(urls, COLUMN_LIMITS["url"]),
        notice_date.date() if notice_date else None,
    )


def parse_retractions(text):
    """
    Parses the Retraction Watch CSV into rows for Database.replace_retractions().

    Raises ValueError if the text is not that file. Records that cannot be
    used are skipped. A paper may have several records (a
    correction and later a retraction, say) and all of them are kept, but
    exact repeats are not.
    """
    # newline="" lets the csv module see the line breaks inside quoted fields.
    reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
    missing = [
        name for name in REQUIRED_COLUMNS if name not in (reader.fieldnames or [])
    ]
    if missing:
        raise ValueError(
            "Retraction Watch data has no column(s): %s" % ", ".join(missing)
        )

    rows = []
    seen = set()
    for record in reader:
        if None in record or None in record.values():
            raise ValueError(
                f"Malformed Retraction Watch row at line {reader.line_num}"
            )
        row = row_from_record(record)
        if row is None or row[1:] in seen:
            continue
        seen.add(row[1:])
        rows.append(row)
    return rows


def download_retractions(session):
    """Returns the text of the Retraction Watch CSV."""
    response = session.get(
        RETRACTION_WATCH_URL,
        headers={"User-Agent": user_agent},
        timeout=(10, 300),
    )
    response.raise_for_status()
    # "utf-8-sig" drops a byte order mark, which would otherwise end up in the
    # name of the first column.
    return response.content.decode("utf-8-sig")


def crossref_rows(session=None):
    """Yield a validated feed snapshot once the database holds the refresh lock."""
    if session is None:
        with requests.Session() as new_session:
            text = download_retractions(new_session)
    else:
        text = download_retractions(session)

    logger.info("Processing downloaded file")
    rows = parse_retractions(text)
    if len(rows) < MIN_RETRACTIONS:
        raise RuntimeError(
            "Only %d usable records in the Retraction Watch data (expected at "
            "least %d); keeping the existing table." % (len(rows), MIN_RETRACTIONS)
        )

    yield from rows


def get_crossref_retractions(database: Database, session=None):
    count = database.replace_retractions(crossref_rows(session))
    logger.info("Wrote %d records to database", count)
    return count


def main():
    try:
        bot_settings = load_bot_settings()
        database = Database(bot_settings["db"])
        get_crossref_retractions(database)
    except Exception:
        logger.exception("Could not update the retractions table")
        return 1
    return 0


if __name__ == "__main__":
    configure_logging("findretraction.log")
    sys.exit(main())
