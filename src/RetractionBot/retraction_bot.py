import datetime
import logging
import os
import pywikibot
from pywikibot import pagegenerators
import pywikibot.login
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
            page_list = []
            try:
                page_list = pagegenerators.SearchPageGenerator(
                    search_string, namespaces=[0], site=site
                )
            except Exception as e:
                logger.error(e)
                time.sleep(60 * 5)
                continue

            for wp_page in page_list:
                page_text = ""
                try:
                    page_text = wp_page.text
                except Exception as e:
                    logger.error(e)
                    continue

                page_text, changes = process_page(
                    page_text, database.retrieve_retracted_identifier, template_map,
                    field_map, citation_templates.get(language, ()),
                )

                # Only bother trying to make an edit if we changed anything
                if page_text != wp_page.text:
                    if not switch.enabled(fresh=True):
                        break
                    wp_page.text = page_text
                    wp_page.save(bot_settings["summary_map"][language], minor=False)

                    logger.info(
                        "Successfully edited {page_name} with "
                        "retracted source(s).".format(page_name=wp_page.title())
                    )
                    for change in changes:
                        database.log_retraction_edit(
                            datetime.datetime.now(),
                            language + ".wikipedia.org",
                            wp_page,
                            change.record.original_doi,
                            0,
                        )



if __name__ == "__main__":
    logger.info("Starting bot run at {dt}".format(dt=datetime.datetime.now()))
    run_bot()
