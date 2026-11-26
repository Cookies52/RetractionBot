import datetime
import logging
import time

import pywikibot
from pywikibot import pagegenerators

from .db import Database, fold_doi
from .identifiers import is_doi, is_pmid
from .settings import configure_logging, load_bot_settings
from .wiki_edit import edit_page, process_item

logger = logging.getLogger(__name__)


def check_bot_killswitches(site):
    """Read the per-wiki switch afresh and fail closed on read errors."""
    try:
        enabled = pywikibot.Page(site, "User:RetractionBot/run").get(force=True) == "yes"
    except Exception:
        logger.exception("Unable to read the bot killswitch")
        return False
    if not enabled:
        logger.warning("Bot killswitch is not set to yes on %s", site)
    return enabled


def search_query(identifier):
    doi, pmid = (part.decode("utf-8").strip() if isinstance(part, bytes)
                 else part.strip() for part in identifier)
    values = []
    if is_doi(doi):
        values.append(fold_doi(doi))
    if is_pmid(pmid):
        values.append(pmid)
    return " OR ".join('"' + value + '"' for value in values)


def search_pages(site, query):
    # Exceptions may occur while advancing a lazy page generator, not merely
    # when it is constructed.
    try:
        yield from pagegenerators.SearchPageGenerator(query, namespaces=[0], site=site)
    except Exception:
        logger.exception("Wikipedia search failed")
        time.sleep(60 * 5)


def run_bot():
    settings = load_bot_settings()
    database = Database(settings["db"])
    identifiers = database.load_retracted_identifiers()
    for language, template_map in settings["template_name_map"].items():
        field_map = settings["template_field_names"][language]
        listed = settings["citation_templates"].get(language, [])
        site = pywikibot.Site(language, "wikipedia")
        site.login()
        if not check_bot_killswitches(site):
            continue
        disabled = False
        for identifier in identifiers:
            query = search_query(identifier)
            if not query:
                continue
            time.sleep(5)
            for page in search_pages(site, query):
                try:
                    original = page.text
                except Exception:
                    logger.exception("Unable to read a search result")
                    continue
                updated, changes = edit_page(original, database, listed, template_map, field_map)
                if updated == original:
                    continue
                # Do not rely on a killswitch read made hours earlier.
                if not check_bot_killswitches(site):
                    disabled = True
                    break
                page.text = updated
                page.save(settings["summary_map"][language], minor=False)
                logger.info("Edited %s", page.title())
                logged = set()
                for record in changes:
                    key = (record.original_doi, record.original_pubmed,
                           record.retraction_doi, record.retraction_pubmed)
                    if key in logged:
                        continue
                    logged.add(key)
                    database.log_retraction_edit(
                        datetime.datetime.now(), language + ".wikipedia.org",
                        page.title(), key[0] or "0", key[1] or "0",
                        notice_doi=key[2] or "0", notice_pmid=key[3] or "0",
                    )
            if disabled:
                break


if __name__ == "__main__":
    configure_logging("retractionbot.log")
    logger.info("Starting bot run at %s", datetime.datetime.now())
    run_bot()
