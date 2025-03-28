import pymysql
import os
import datetime


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
    ):
        self.origin = origin.decode("utf-8")
        self.original_doi = original_doi.decode("utf-8")
        self.retraction_doi = retraction_doi.decode("utf-8")
        self.original_pubmed = original_pmid.decode("utf-8")
        self.retraction_pubmed = retraction_pmid.decode("utf-8")
        self.retraction_nature = retraction_nature.decode("utf-8")
        self.url = url.decode("utf-8")


class Database:
    def __init__(self, db_settings: dict[str, str]):
        self._db = pymysql.connect(
            host=db_settings["host"],
            db=db_settings["name"],
            read_default_file=os.path.expanduser("~/replica.my.cnf"),
        )

    def save_retraction_to_db(
        self,
        timestamp,
        origin,
        original_doi,
        retraction_doi,
        original_pmid,
        retraction_pmid,
        retraction_nature,
        url,
    ):
        """
        Given a certain type of identifier (e.g. doi, pmid), its origin
        (e.g. crossref, pubmed) and both the new (retraction) id and old
        (retracted) id, save this to the DB. type can be 'doi' or 'pmid'
        """
        self._db.ping(reconnect=True)
        cur = self._db.cursor()
        query = """
            INSERT INTO retractions
            VALUES ('{timestamp}', '{origin}', '{original_doi}', '{retraction_doi}', '{original_pmid}', '{retraction_pmid}', '{retraction_nature}', '{URLs}')"""

        if timestamp.year < 1971:
            cur.execute(
                query.format(
                    timestamp=datetime.datetime.fromtimestamp(60),
                    origin=origin,
                    original_doi=original_doi,
                    retraction_doi=retraction_doi,
                    original_pmid=original_pmid,
                    retraction_pmid=retraction_pmid,
                    retraction_nature=retraction_nature,
                    URLs=url,
                )
            )
        else:
            cur.execute(
                query.format(
                    timestamp=timestamp,
                    origin=origin,
                    original_doi=original_doi,
                    retraction_doi=retraction_doi,
                    original_pmid=original_pmid,
                    retraction_pmid=retraction_pmid,
                    retraction_nature=retraction_nature,
                    URLs=url,
                )
            )

    def truncate_db(self):
        self._db.ping(reconnect=True)
        cur = self._db.cursor()
        query = """TRUNCATE table retractions"""
        cur.execute(query.format())

    def retracted_id_exists(self, retraction_id):
        """
        Given a retraction ID string, checks if an entry already exists for it
        in the database. If so, return True.
        """
        cur = self._db.cursor()
        query = """
            SELECT COUNT(*) FROM retractions
            WHERE original_doi = "{retraction_id}" OR original_pmid = "{retraction_id}"
        """
        self._db.ping(reconnect=True)
        cur.execute(query.format(retraction_id=retraction_id))
        count_result = cur.fetchone()

        if count_result[0] != 0:
            return True
        else:
            return False

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

    def load_retracted_identifiers(self):
        cur = self._db.cursor()
        query = """
            SELECT original_doi,original_pmid FROM retractions ORDER BY RAND()
        """
        self._db.ping(reconnect=True)
        cur.execute(query)
        return list(cur.fetchall())

    def retrieve_retracted_identifier(self, id):
        cur = self._db.cursor()
        query = """
            SELECT * FROM retractions WHERE original_doi="{retraction_id}" OR original_pmid="{retraction_id}"
        """
        self._db.ping(reconnect=True)
        cur.execute(query.format(retraction_id=id))
        item = list(cur.fetchall())
        return [Retraction(x[1], x[2], x[3], x[4], x[5], x[6], x[7]) for x in item]

    def log_retraction_edit(self, timestamp, domain, page_title, orig_doi, orig_pmid):
        cur = self._db.cursor()
        query = """
            INSERT INTO edit_log
            VALUES ('{timestamp}', '{domain}', '{page_title}', '{orig_doi}', '{orig_pmid}', '{new_doi}', '{new_pmid}')
        """
        self._db.ping(reconnect=True)
        cur.execute(
            query.format(
                timestamp=timestamp,
                domain=domain,
                page_title=page_title,
                orig_doi=orig_doi,
                orig_pmid=orig_pmid,
                new_doi=0,
                new_pmid=0,
            )
        )

    def check_edits(self, page_title, id):
        cur = self._db.cursor()
        query = """
            SELECT * FROM edit_log WHERE page_title="{page_title}" AND (original_doi="{retraction_id}" OR original_pmid="{retraction_id}")
        """
        self._db.ping(reconnect=True)
        cur.execute(query.format(page_title=page_title, retraction_id=id))
        return list(cur.fetchall())
