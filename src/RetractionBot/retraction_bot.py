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


def check_bot_killswitches(site):
    """
    Verifies that bot killswitch hasn't been edited, for this site or Meta.

    Checks User:RetractionBot/run, an openly editable page, to see if an
    editor has disabled the bot. If the page contains anything other than
    "yes", don't run. Checks per-wiki.
    Returns True if bot can run, False otherwise.
    """
    run_page_name = "User:RetractionBot/run"

    for a_site in [site]:
        run_page = pywikibot.Page(a_site, run_page_name)

        if run_page.text != "yes":
            log_text = "{run_page_name} not set to 'yes' on {lang},"
            "not running.".format()
            logger.error(log_text)
            return False

    # If we haven't returned False then everything
    # seems to be fine, so return True.
    return True


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
        site.login()
        bot_can_run = check_bot_killswitches(site)

        for identifier in retracted_identifiers:
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
                if page_text != wp_page.text and bot_can_run:
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
