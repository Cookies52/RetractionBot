import datetime
import logging
import time

import pywikibot
from pymysql import MySQLError
from pywikibot import pagegenerators
from pywikibot.exceptions import APIError, EditConflictError, PageRelatedError, ServerError

from .db import Database
from .page_edits import process_page
from .scan_queue import build_snapshot, open_scan_queue
from .settings import configure_logging, load_bot_settings


logger = logging.getLogger(__name__)


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


def run_bot():
    settings = load_bot_settings()
    scan = settings.get("scan", {})
    limit = scan.get("limit_per_wiki", 500)
    if type(limit) is not int or limit < 1:
        raise ValueError("scan.limit_per_wiki must be a positive integer")
    state_file = scan.get("state_file", "~/.local/state/retractionbot/scan.sqlite3")
    with open_scan_queue(state_file) as queue:
        if queue is None:
            logger.info("Another RetractionBot scan already owns %s", state_file)
            return
        run_scan(settings, queue, limit)


def run_scan(settings, queue, limit):
    database = Database(settings["db"])
    snapshot = None
    for language, template_map in settings["template_name_map"].items():
        site = pywikibot.Site(language, "wikipedia")
        switch = WikiSwitch(site)
        if not switch.enabled():
            continue
        site.login()
        if snapshot is None:
            snapshot = build_snapshot(database.load_scan_rows())

        field_map = settings["template_field_names"][language]
        citation_names = settings["citation_templates"].get(language, ())
        summary = settings["summary_map"][language]
        queue.sync(language, snapshot)

        def transform(text):
            return process_page(
                text, database.retrieve_retracted_identifier, template_map, field_map,
                citation_names,
            )

        def record_changes(page, changes):
            log_page_changes(database, language, page, changes)

        for doi, pmid in queue.work(language, limit):
            if not switch.enabled():
                break
            search = " OR ".join(f'"{identifier}"' for identifier in (doi, pmid) if identifier)
            if not search:
                continue
            queue.started(language, doi, pmid)
            time.sleep(5)
            if scan_identifier(site, search, switch, summary, transform, record_changes):
                queue.finished(language, doi, pmid)


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
    configure_logging("retractionbot.log")
    logger.info("Starting bot run at {dt}".format(dt=datetime.datetime.now()))
    run_bot()
