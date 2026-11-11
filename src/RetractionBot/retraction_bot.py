import datetime
import logging
import os
import pywikibot
from pywikibot import pagegenerators
from pywikibot.exceptions import APIError, EditConflictError, PageRelatedError, ServerError
import pywikibot.login
from pymysql import MySQLError
import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader
import time

from .db import Database
from .identifiers import is_doi, is_pmid
from .page_edits import process_page

directory = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    filename=os.path.join(directory, "retractionbot.log"),
    level=logging.INFO,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class WikiSwitch:
    """Refresh a wiki's on-wiki switch periodically and before every write."""

    def __init__(self, site):
        self.site = site
        self.checked_at = float("-inf")
        self.allowed = False

    def enabled(self, *, fresh=False):
        now = time.monotonic()
        if not fresh and now - self.checked_at < 60:
            return self.allowed

        self.checked_at = now
        page = pywikibot.Page(self.site, "User:RetractionBot/run")
        try:
            self.allowed = page.get(force=True).strip() == "yes"
        except pywikibot.exceptions.Error as error:
            self.allowed = False
            logger.warning("Cannot read %s on %s: %s", page.title(), self.site, error)
        if not self.allowed:
            logger.warning("Stopping on %s because %s does not authorize edits", self.site, page.title())
        return self.allowed


def load_bot_settings():
    """Returns the contents of bot_settings.yaml"""
    with open("bot_settings.yml") as bot_settings_file:
        loaded_yaml = yaml.load(bot_settings_file, Loader=Loader)
    return loaded_yaml


def run_bot():
    bot_settings = load_bot_settings()
    bot_languages = bot_settings["template_name_map"]
    template_field_names = bot_settings["template_field_names"]
    citation_templates = bot_settings["citation_templates"]
    database = Database(bot_settings["db"])
    retracted_identifiers = database.load_retracted_identifiers()

    for language, template_map in bot_languages.items():

        field_map = template_field_names[language]

        site = pywikibot.Site(language, "wikipedia")
        switch = WikiSwitch(site)
        if not switch.enabled():
            continue
        site.login()

        for identifier in retracted_identifiers:
            if not switch.enabled():
                break
            search_string = ""
            original_id = identifier[0].decode("utf-8").strip()
            original_pmid = identifier[1].decode("utf-8").strip()

            logger.info("Starting processing %s - %s", original_id, original_pmid)
            doi_valid = is_doi(original_id)
            pmid_valid = is_pmid(original_pmid)

            if doi_valid and pmid_valid:
                search_string = '"' + original_id + '" OR "' + original_pmid + '"'
            elif doi_valid:
                search_string = '"' + original_id + '"'
            elif pmid_valid:
                search_string = '"' + original_pmid + '"'
            else:
                continue

            time.sleep(5)
            def transform(text):
                return process_page(
                    text, database.retrieve_retracted_identifier, template_map,
                    field_map, citation_templates.get(language, ()),
                )

            def record_changes(page, changes):
                log_page_changes(database, language, page, changes)

            scan_identifier(site, search_string, switch, bot_settings["summary_map"][language],
                            transform, record_changes)


def log_page_changes(database, language, page, changes):
    title = page.title()
    timestamp = datetime.datetime.now()
    for change in changes:
        record = change.record
        try:
            database.log_retraction_edit(
                timestamp, language + ".wikipedia.org", title,
                record.original_doi, record.original_pubmed,
                notice_doi=record.retraction_doi,
                notice_pmid=record.retraction_pubmed,
                action=change.action,
            )
        except MySQLError:
            logger.exception(
                "Wiki edit saved, but could not record %s for %s (%s / %s)",
                change.action, title, record.original_doi, record.original_pubmed,
            )


def scan_identifier(site, search, switch, summary, transform, record_changes):
    complete = True
    try:
        for page in pagegenerators.SearchPageGenerator(search, namespaces=[0], site=site):
            if not switch.enabled():
                return False
            if not edit_page(page, switch, summary, transform, record_changes):
                complete = False
    except (APIError, ServerError) as error:
        logger.warning("Search %r failed on %s: %s", search, site, error)
        time.sleep(60)
        return False
    return complete


def edit_page(page, switch, summary, transform, record_changes):
    for attempt in range(2):
        try:
            original = page.get(force=True) if attempt else page.text
            text, changes = transform(original)
            if text == original:
                return True
            if not switch.enabled(fresh=True):
                return False
            page.text = text
            page.save(summary=summary, minor=False, nocreate=True)
        except EditConflictError as error:
            if attempt:
                logger.warning("Edit conflict persisted on %s: %s", page.title(), error)
                return False
            continue
        except PageRelatedError as error:
            logger.info("Skipping %s: %s", page.title(), error)
            return True
        except (APIError, ServerError) as error:
            logger.warning("Could not read or save %s: %s", page.title(), error)
            return False

        logger.info("Successfully edited %s", page.title())
        record_changes(page, changes)
        return True



if __name__ == "__main__":
    logger.info("Starting bot run at {dt}".format(dt=datetime.datetime.now()))
    run_bot()
