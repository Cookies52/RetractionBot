import datetime
import logging
import os
import pywikibot
from pywikibot import pagegenerators
import re
import pywikibot.login
import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader
import mwparserfromhell
import time

from .db import Database

directory = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    filename=os.path.join(directory, "retractionbot.log"),
    level=logging.INFO,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)
DOI_REGEX = r"\b(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?![\"&'<>])[a-zA-Z.\/0-9\-])+)\b"


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
    database = Database(bot_settings["db"])
    retracted_identifiers = database.load_retracted_identifiers()

    for language, template_map in bot_languages.items():

        field_map = template_field_names[language]

        site = pywikibot.Site(language, "wikipedia")
        site.login()
        bot_can_run = check_bot_killswitches(site)

        for identifier in retracted_identifiers:
            search_string = ""
            time.sleep(5)  # 10s sleep between API calls.
            original_id = identifier[0].decode("utf-8").strip()
            original_pmid = identifier[1].decode("utf-8").strip()

            logger.info("Starting processing %s - %s", original_id, original_pmid)
            doi_valid = bool(re.match(DOI_REGEX, original_id))
            pmid_valid = original_pmid != "0" and original_pmid != 0

            if doi_valid and pmid_valid:
                search_string = '"' + original_id + '" OR "' + original_pmid + '"'
            elif doi_valid:
                search_string = '"' + original_id + '"'
            elif pmid_valid:
                search_string = '"' + original_pmid + '"'
            else:
                continue

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

                changes = []

                # Returns list of Tag objects with each cite.
                wikitext = mwparserfromhell.parse(page_text)

                logger.debug("Processing %s", wp_page)
                page_cites = [
                    x
                    for x in wikitext.filter_tags()
                    if x.tag.lower() == "ref" and re.findall(DOI_REGEX, str(x))
                ]

                num_cites_found = len(page_cites)

                if num_cites_found == 0:
                    logger.info(
                        "Couldn't find any DOIs on page {page}.".format(page=wp_page)
                    )
                    continue
                else:
                    logger.info("Page %s has %d dois cited", wp_page, num_cites_found)

                raw_templates = wikitext.filter_templates()

                doi_field = field_map.get("doi", "doi")
                pmid_field = field_map.get("pmid", "pmid")

                for i, item in enumerate(raw_templates):
                    new_code = None
                    if i == len(raw_templates) - 1 or raw_templates[
                        i + 1
                    ].name.casefold().strip() not in (
                        x.casefold() for x in template_map.values()
                    ):
                        if "cochrane" in str(item).lower():
                            continue
                        # Process new retractions
                        if item.name.lower() in [doi_field.lower(), "doi-inline"]:
                            logger.debug("Processing doi templates")
                            if item.has("1", ignore_empty=True):
                                record = database.retrieve_retracted_identifier(
                                    item.get("1").value.strip()
                                )
                            # get list of retractions in record
                            xitems = [x.retraction_nature for x in record]
                            for r in record:
                                if (
                                    "retraction" in xitems
                                    and r.retraction_nature == "retraction"
                                ):
                                    new_code = process_item(r, template_map, field_map)
                                if (
                                    "retracted" in xitems
                                    and r.retraction_nature == "retraction"
                                ):
                                    new_code = process_item(r, template_map, field_map)
                                if (
                                    new_code is None
                                    and "expression of concern" in xitems
                                    and r.retraction_nature == "expression of concern"
                                ):
                                    new_code = process_item(r, template_map, field_map)
                                elif new_code is None:
                                    new_code = process_item(r, template_map, field_map)

                                if new_code is not None:
                                    changes.append(r.original_doi)
                                    if not wikitext.contains(str(item) + str(new_code)):
                                        wikitext.replace(
                                            str(item), str(item) + str(new_code)
                                        )

                        if item.name.lower() == pmid_field.lower():
                            logger.debug("Processing pmid templates")
                            if item.has("1", ignore_empty=True):
                                record = database.retrieve_retracted_identifier(
                                    item.get("1").value.strip()
                                )
                            for r in record:
                                new_code = process_item(r, template_map, field_map)
                                if new_code is not None:
                                    changes.append(r.original_pubmed)
                                    wikitext.replace(
                                        str(item), str(item) + str(new_code)
                                    )

                        if "cite" in item.name.lower():
                            logger.debug("Processing cite templates")
                            record = []
                            if item.has(doi_field, ignore_empty=True):
                                doi_value = item.get(doi_field).value.strip()
                                logger.debug("DOI %s found", doi_value)
                                record = database.retrieve_retracted_identifier(
                                    doi_value
                                )
                            elif item.has(pmid_field, ignore_empty=True):
                                pmid_value = item.get(pmid_field).value.strip()
                                logger.debug("PMID %s found", pmid_value)
                                record = database.retrieve_retracted_identifier(
                                    pmid_value
                                )

                            for r in record:
                                new_code = process_item(r, template_map, field_map)
                                if (
                                    new_code is not None
                                    and r.original_doi not in changes
                                ):
                                    changes.append(r.original_doi)
                                    wikitext.replace(
                                        str(item), str(item) + str(new_code)
                                    )
                    else:
                        # Check existing retraction
                        if item.has(doi_field, ignore_empty=True):
                            record = database.retrieve_retracted_identifier(
                                item.get(doi_field).value.strip()
                            )
                        elif item.has(pmid_field, ignore_empty=True):
                            record = database.retrieve_retracted_identifier(
                                item.get(pmid_field).value.strip()
                            )
                        else:
                            continue
                        in_use = None

                        logger.debug("Existing retracted item: %s", record)

                        for r in record:
                            if (
                                r.retraction_nature is None
                                or r.retraction_nature == "Reinstatement"
                            ):
                                wikitext.replace(str(raw_templates[i + 1]), "")
                                continue
                            if (
                                r.retraction_nature is None
                                or r.retraction_nature == "Retraction"
                                and (
                                    in_use is None
                                    or in_use.retraction_nature != "Retraction"
                                )
                            ):
                                in_use = r
                            elif (
                                r.retraction_nature is None
                                or r.retraction_nature == "Expression of concern"
                                and (
                                    in_use is None
                                    or in_use.retraction_nature != "Retraction"
                                )
                            ):
                                in_use = r
                            elif r.retraction_nature is None or in_use is None:
                                in_use = r

                        logger.debug("In Use item: %s", in_use)

                        if in_use is not None:
                            new_code = process_item(in_use, template_map, field_map)
                            intentional_field = field_map.get(
                                "intentional", "intentional"
                            )
                            if raw_templates[i + 1].has(
                                intentional_field, ignore_empty=True
                            ):
                                new_code.add(
                                    intentional_field,
                                    raw_templates[i + 1]
                                    .get(intentional_field)
                                    .value.strip(),
                                )
                            if raw_templates[i + 1].has(
                                "pmcid", ignore_empty=True
                            ):
                                new_code.add(
                                    "pmcid",
                                    raw_templates[i + 1]
                                    .get("pmcid")
                                    .value.strip(),
                                )
                            if raw_templates[i + 1].has("checked", ignore_empty=True):
                                new_code.add(
                                    "checked",
                                    raw_templates[i + 1].get("checked").value.strip(),
                                )
                            if raw_templates[i + 1].has(
                                "doi-access", ignore_empty=True
                            ):
                                new_code.add(
                                    "doi-access",
                                    raw_templates[i + 1]
                                    .get("doi-access")
                                    .value.strip(),
                                )
                            if (
                                new_code is not None
                                and raw_templates[i + 1] != new_code
                            ):
                                wikitext.replace(raw_templates[i + 1], str(new_code))

                page_text = str(wikitext)

                # Only bother trying to make an edit if we changed anything
                if page_text != wp_page.text and bot_can_run:
                    wp_page.text = page_text
                    wp_page.save(bot_settings["summary_map"][language], minor=False)

                    logger.info(
                        "Successfully edited {page_name} with "
                        "retracted source(s).".format(page_name=wp_page.title())
                    )
                    for x in changes:
                        database.log_retraction_edit(
                            datetime.datetime.now(),
                            language + ".wikipedia.org",
                            wp_page,
                            x,
                            0,
                        )


def process_item(record, template_map, field_map):
    new_code = ""
    if record.retraction_nature == "Retraction":
        logger.info(
            "Generating Retraction template needed for DOI %s", record.original_doi
        )
        new_code = mwparserfromhell.nodes.template.Template(
            name=template_map.get("retracted", "Retracted")
        )
    elif record.retraction_nature == "Expression of concern":
        logger.info("Generating EoC template needed for DOI %s", record.original_doi)
        new_code = mwparserfromhell.nodes.template.Template(
            name=template_map.get("expression of concern", "Expression of Concern")
        )
    elif record.retraction_nature == "Correction":
        logger.info(
            "Generating Erratum template needed for DOI %s", record.original_doi
        )
        new_code = mwparserfromhell.nodes.template.Template(
            name=template_map.get("erratum", "Erratum")
        )
    else:
        logger.debug("No change needed for doi %s", record.original_doi)
        return None

    if record.retraction_doi != "0" and record.retraction_doi != "unavaliable":
        new_code.add(field_map.get("doi", "doi"), record.retraction_doi)
    if record.retraction_pubmed != "0":
        new_code.add(field_map.get("pmid", "pmid"), record.retraction_pubmed)
    if record.url != "":
        for idx, x in enumerate(record.url.split(";")):
            if x != "":
                new_code.add(str(idx + 1), x + " ''Retraction Watch''")
    return new_code


if __name__ == "__main__":
    logger.info("Starting bot run at {dt}".format(dt=datetime.datetime.now()))
    run_bot()
