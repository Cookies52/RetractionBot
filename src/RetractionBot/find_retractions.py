import lxml.etree
import lxml.html
import requests
import os
import csv
from .db import Database
import datetime
import logging
import lxml

from .retraction_bot import load_bot_settings

directory = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    filename=os.path.join(directory, "findretraction.log"),
    level=logging.INFO,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.WARNING)

user_agent = "RetractionBot (https://github.com/cookies52/RetractionBot; mailto:matthewdann52@gmail.com)"


def get_crossref_retractions(database: Database):
    # List of crossref retraction types based on, but stricter than,
    # https://github.com/fathomlabs/crossref-retractions/blob/master/index.js

    url = "https://gitlab.com/crossref/retraction-watch-data/-/raw/main/retraction_watch.csv?ref_type=heads"

    # for retraction_type in retraction_types:
    with requests.Session() as s:
        r = s.get(url)
        text = r.content.decode("utf-8", errors="replace")

        csv_reader = csv.DictReader(text.splitlines(), delimiter=",", quotechar='"')
        logger.info("Processing downloaded file")

        items_count = 0
        for item in csv_reader:
            try:

                items_count += 1

                timestamp = datetime.datetime.strptime(
                    item["OriginalPaperDate"], "%m/%d/%Y %H:%M"
                )
                try:

                    if (
                        item["OriginalPaperDOI"] != 0
                        and not database.retracted_id_exists(item["RetractionDOI"])
                    ) or (
                        item["OriginalPaperPubMedID"] != 0
                        and not database.retracted_id_exists(item["RetractionPubMedID"])
                    ):
                        logger.info("Saving retraction to db")
                        database.save_retraction_to_db(
                            timestamp=timestamp,
                            origin="Crossref",
                            original_doi=item["OriginalPaperDOI"],
                            retraction_doi=item["RetractionDOI"],
                            original_pmid=item["OriginalPaperPubMedID"],
                            retraction_pmid=item["RetractionPubMedID"],
                            retraction_nature=item["RetractionNature"],
                            url=item["URLS"],
                        )
                    else:
                        logger.info("%s already in db", item)
                except Exception as e:
                    logging.warning(
                        "Failed to write record %s to database : %s",
                        item["Record ID"],
                        repr(e),
                    )

            except Exception as e:
                logging.exception("Error passing Item %s", item, exc_info=e)
                continue
        logging.info("Wrote %d records to database", items_count)


def get_ncbi_retractions():
    with requests.Session() as s:

        for mode in ["correction"]:  # , "retraction", '"expression of concern"']
            r = s.get(
                f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&term=${mode}[filter]&usehistory=y&RetMax=100"
            )
            lxml.etree.parse(r.text)

            # Get details

            # https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?verb=GetRecord&identifier=oai:pubmedcentral.nih.gov:11924858&metadataPrefix=pmc_fm


if __name__ == "__main__":
    bot_settings = load_bot_settings()
    database = Database(bot_settings["db"])
    database.truncate_db()

    get_crossref_retractions(database)
