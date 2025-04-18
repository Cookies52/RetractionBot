import lxml.etree
import lxml.html
import requests
import csv
from .db import (
    save_retraction_to_db,
    retracted_id_exists,
    truncate_db,
)
import datetime
import logging
import lxml

logger = logging.getLogger(__name__)

user_agent = "RetractionBot (https://github.com/cookies52/RetractionBot; mailto:matthewdann52@gmail.com)"


def get_crossref_retractions():
    # List of crossref retraction types based on, but stricter than,
    # https://github.com/fathomlabs/crossref-retractions/blob/master/index.js

    url = "https://api.labs.crossref.org/data/retractionwatch"

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
                        and not retracted_id_exists(item["RetractionDOI"])
                    ) or (
                        item["OriginalPaperPubMedID"] != 0
                        and not retracted_id_exists(item["RetractionPubMedID"])
                    ):
                        save_retraction_to_db(
                            timestamp=timestamp,
                            origin="Crossref",
                            original_doi=item["OriginalPaperDOI"],
                            retraction_doi=item["RetractionDOI"],
                            original_pmid=item["OriginalPaperPubMedID"],
                            retraction_pmid=item["RetractionPubMedID"],
                            retraction_nature=item["RetractionNature"],
                            url=item["URLS"],
                        )
                except Exception as e:
                    logging.warning(
                        "Failed to write record %s to database : %s",
                        item["Record ID"],
                        repr(e),
                    )

            except Exception:
                logging.warning("Error passing Item %s", item)
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
    truncate_db()
    get_crossref_retractions()
