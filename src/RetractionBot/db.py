import pymysql
import os
import datetime
import string

from .identifiers import is_doi, is_pmid

# DOIs match regardless of case, but only for A-Z (DOI Handbook 2025, section 4.3.4)
_FOLD = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)

# Longest value, in bytes, that each varbinary column of schema.sql can hold.
# Longer values are silently cut off (or rejected, depending on sql_mode).
COLUMN_LIMITS = {
    "origin": 20,
    "original_doi": 200,
    "retraction_doi": 200,
    "original_pmid": 200,
    "retraction_pmid": 200,
    "retraction_nature": 200,
    "url": 5000,
}

# Order of the values in the rows given to Database.replace_retractions().
RETRACTION_COLUMNS = ("timestamp",) + tuple(COLUMN_LIMITS) + ("notice_date",)

# The TIMESTAMP type only holds 1970-01-01 00:00:01 UTC to 2038-01-19. Dates
# outside 1971-2037 are stored as this value instead (a day in, so that no
# session time zone can push it out of range).
PLACEHOLDER_TIMESTAMP = datetime.datetime(1970, 1, 2)

INSERT_BATCH_SIZE = 1000


def fold_doi(doi):
    return doi.translate(_FOLD)


def clamp_timestamp(timestamp):
    """Returns ``timestamp``, or the placeholder if TIMESTAMP cannot hold it."""
    if 1971 <= timestamp.year <= 2037:
        return timestamp
    return PLACEHOLDER_TIMESTAMP


def check_lengths(rows):
    """
    Raises ValueError if a value does not fit in its column.

    Done up front so that a bad row stops a reload before anything has been
    changed, instead of being truncated in the table.
    """
    for number, row in enumerate(rows):
        if len(row) != len(RETRACTION_COLUMNS):
            raise ValueError(
                "row %d has %d values, expected %d"
                % (number, len(row), len(RETRACTION_COLUMNS))
            )
        for column, value in zip(COLUMN_LIMITS, row[1:-1]):
            size = len(str(value).encode("utf-8"))
            if size > COLUMN_LIMITS[column]:
                raise ValueError(
                    "row %d: %s is %d bytes, the column holds %d"
                    % (number, column, size, COLUMN_LIMITS[column])
                )


class Retraction:
    def __init__(
        self,
        origin,
        original_doi,
        retraction_doi,
        original_pmid,
        retraction_pmid,
        retraction_nature,
        url,
        notice_date=None,
    ):
        self.origin = origin.decode("utf-8")
        self.original_doi = original_doi.decode("utf-8")
        self.retraction_doi = retraction_doi.decode("utf-8")
        self.original_pubmed = original_pmid.decode("utf-8")
        self.retraction_pubmed = retraction_pmid.decode("utf-8")
        self.retraction_nature = retraction_nature.decode("utf-8")
        self.url = url.decode("utf-8")
        self.notice_date = notice_date


class Database:
    def __init__(self, db_settings: dict[str, str]):
        self._db = pymysql.connect(
            host=db_settings["host"],
            db=db_settings["name"],
            read_default_file=os.path.expanduser("~/replica.my.cnf"),
        )

    def replace_retractions(self, rows):
        """Build and publish a complete Aria snapshot while holding the refresh lock."""
        self._db.ping(reconnect=True)
        with self._db.cursor() as cur:
            lock_name = "CONCAT(DATABASE(), '.retractions.refresh')"
            cur.execute(f"SELECT GET_LOCK({lock_name}, 0)")
            if cur.fetchone()[0] != 1:
                raise RuntimeError("Could not acquire the retraction refresh lock")

            try:
                # The importer downloads and validates while holding the lock.
                # A competing run must not publish an older snapshot afterward.
                rows = list(rows)
                if not rows:
                    raise ValueError(
                        "Retraction Watch snapshot contained no usable rows"
                    )
                check_lengths(rows)

                cur.execute("DROP TABLE IF EXISTS retractions_new, retractions_old")
                cur.execute("CREATE TABLE retractions_new LIKE retractions")
                # STRICT_TRANS_TABLES can truncate later rows in an Aria batch.
                cur.execute(
                    "SET SESSION sql_mode = "
                    "CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), 'STRICT_ALL_TABLES')"
                )
                columns = ", ".join(RETRACTION_COLUMNS)
                values = ", ".join(["%s"] * len(RETRACTION_COLUMNS))
                query = f"INSERT INTO retractions_new ({columns}) VALUES ({values})"
                for start in range(0, len(rows), INSERT_BATCH_SIZE):
                    batch = rows[start : start + INSERT_BATCH_SIZE]
                    if cur.executemany(query, batch) != len(batch):
                        raise RuntimeError(
                            "Retraction Watch snapshot insert count did not match"
                        )
                self._db.commit()
                cur.execute(
                    "RENAME TABLE retractions TO retractions_old, "
                    "retractions_new TO retractions"
                )
                cur.execute("DROP TABLE retractions_old")
                return len(rows)
            finally:
                # A disconnected session already lost the lock. The next
                # refresh drops any leftover staging tables under the lock.
                if self._db.open:
                    cur.execute(f"DO RELEASE_LOCK({lock_name})")

    def get_latest_timestamp(self):
        """
        Get the latest timestamp from the database in the format YYYY-MM-DD
        """
        cur = self._db.cursor()
        query = """
            SELECT timestamp FROM retractions
            ORDER BY timestamp DESC
            LIMIT 1
        """
        self._db.ping(reconnect=True)
        cur.execute(query)
        fetch_one = cur.fetchone()
        if fetch_one:
            max_timestamp = fetch_one[0].strftime("%Y-%m-%d")
        else:
            # If no objects are in the database, pick an arbitrarily old date.
            max_timestamp = "1970-01-01"

        return max_timestamp

    def load_scan_rows(self):
        self._db.ping(reconnect=True)
        with self._db.cursor() as cur:
            cur.execute("""
                SELECT original_doi, original_pmid, retraction_doi, retraction_pmid,
                       retraction_nature, url, notice_date
                FROM retractions
            """)
            return cur.fetchall()

    def retrieve_retracted_identifier(self, identifier):
        identifier = identifier.strip()
        if is_doi(identifier):
            column = "original_doi"
            identifier = fold_doi(identifier)
        elif is_pmid(identifier):
            column = "original_pmid"
        else:
            return []

        self._db.ping(reconnect=True)
        with self._db.cursor() as cur:
            cur.execute(
                f"""
                SELECT origin, original_doi, retraction_doi, original_pmid,
                       retraction_pmid, retraction_nature, url, notice_date
                FROM retractions WHERE {column}=%s
                """,
                (identifier,),
            )
            return [Retraction(*row) for row in cur.fetchall()]

    def log_retraction_edit(
        self,
        timestamp,
        domain,
        page_title,
        original_doi,
        original_pmid,
        *,
        notice_doi="0",
        notice_pmid="0",
        action="unknown",
    ):
        """Records that ``page_title`` was flagged because of this paper."""
        cur = self._db.cursor()
        # Name the columns: edit_log lists retraction_doi between the DOI and
        # the PMID of the original paper, and a positional INSERT used to put
        # the PMID into retraction_doi.
        query = """
            INSERT INTO edit_log
                (timestamp, domain, page_title, original_doi, retraction_doi,
                 original_pmid, retraction_pmid, action)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        self._db.ping(reconnect=True)
        cur.execute(
            query,
            (
                timestamp,
                domain,
                page_title,
                original_doi,
                notice_doi,
                original_pmid,
                notice_pmid,
                action,
            ),
        )
        self._db.commit()

    def check_edits(self, page_title, id):
        cur = self._db.cursor()
        query = """
            SELECT * FROM edit_log WHERE page_title=%s AND (original_doi=%s OR original_pmid=%s)
        """
        self._db.ping(reconnect=True)
        cur.execute(query, (page_title, id, id))
        return list(cur.fetchall())
