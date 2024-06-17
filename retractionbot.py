import datetime
import logging
import os
import pywikibot
from pywikibot import pagegenerators
import re
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader
import mwparserfromhell
import time

from db import load_retracted_identifiers, log_retraction_edit, retrieve_retracted_identifier,check_edits

directory = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    filename=os.path.join(directory, 'retractionbot.log'),
                    level=logging.INFO)

DOI_REGEX = r"\b(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?![\"&'<>])[a-zA-Z.\/0-9\-])+)\b"


def check_bot_killswitches(site):
    """
    Verifies that bot killswitch hasn't been edited, for this site or Meta.

    Checks User:RetractionBot/run, an openly editable page, to see if an
    editor has disabled the bot. If the page contains anything other than
    "yes", don't run. Checks per-wiki.
    Returns True if bot can run, False otherwise.
    """
    meta_site = pywikibot.Site('meta', 'meta')
    run_page_name = "User:RetractionBot/run"

    for a_site in [site, meta_site]:
        run_page = pywikibot.Page(a_site, run_page_name)

        if run_page.text != "yes":
            log_text = "{run_page_name} not set to 'yes' on {lang},"
            "not running.".format(
                run_page_name=run_page_name,
                lang=a_site.lang
                )
            logger.error(log_text)
            return False

    # If we haven't returned False then everything
    # seems to be fine, so return True.
    return True


def load_bot_settings():
    """Returns the contents of bot_settings.yaml"""
    with open('bot_settings.yml') as bot_settings_file:
        loaded_yaml = yaml.load(bot_settings_file, Loader=Loader)
    return loaded_yaml

def run_bot():
    bot_settings = load_bot_settings()
    bot_languages = bot_settings['retracted_template_names']
    template_field_names = bot_settings['template_field_names']
    retracted_identifiers = load_retracted_identifiers()

    for language, lang_items in bot_languages.items():

        site = pywikibot.Site(language, 'wikipedia')
        bot_can_run = check_bot_killswitches(site)

        for identifier in retracted_identifiers:
            time.sleep(10) # 10s sleep between API calls.
            original_id = identifier[0].decode("utf-8")

            logger.info("Starting processing %s", original_id)

            page_list = pagegenerators.SearchPageGenerator('"' + original_id + '"',
                                                           namespaces=[0], site=site)

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
                page_cites = [x for x in wikitext.filter_tags() if x.tag.lower() == "ref" and re.findall(DOI_REGEX, str(x))]

                num_cites_found = len(page_cites)

                if num_cites_found == 0:
                    logger.debug("Couldn't find any DOIs on page {page}.".format(
                                page=wp_page))
                    continue
                else:
                    logger.info("Page %s has %d dois cited", wp_page, num_cites_found)

                raw_templates = wikitext.filter_templates()

                for i, item in enumerate(raw_templates):
                    new_code = None
                    if i == len(raw_templates) - 1 or raw_templates[i+1].name.lower().strip() not in ["erratum", "expression of concern", "retracted"]:
                        if "cochrane" in str(item).lower():
                            continue
                        # Process new retractions
                        if item.name.lower() in ["doi", "doi-inline"]:
                            logger.debug("Processing doi templates")
                            if item.has("1", ignore_empty=True):
                                record = retrieve_retracted_identifier(item.get("1").value.strip())
                                
                            for r in record:
                                new_code = process_item(r)
                                if new_code is not None:
                                    changes.append(r.original_doi)
                                    wikitext.replace(str(item), str(item) + str(new_code))

                        if item.name.lower() == "pmid":
                            logger.debug("Processing pmid templates")
                            if item.has("1", ignore_empty=True):
                                record = retrieve_retracted_identifier(item.get("1").value.strip())
                            for r in record:
                                new_code = process_item(r)
                                if new_code is not None:
                                    changes.append(r.original_pubmed)
                                    wikitext.replace(str(item), str(item) + str(new_code))                    
                    
                        if "cite" in item.name.lower():
                            logger.debug("Processing cite templates")
                            record = []
                            if item.has("doi", ignore_empty=True):
                                record = retrieve_retracted_identifier(item.get("doi").value.strip())
                            elif item.has("pmid", ignore_empty=True):
                                record = retrieve_retracted_identifier(item.get("pmid").value.strip())
                            for r in record:
                                new_code = process_item(r)  
                                if new_code is not None and r.original_doi not in changes:
                                    changes.append(r.original_doi)
                                    wikitext.replace(str(item), str(item) + str(new_code))
                    else:
                        # Check existing retraction
                        if item.has("doi", ignore_empty=True):
                            record = retrieve_retracted_identifier(item.get("doi").value.strip())
                        elif item.has("pmid", ignore_empty=True):
                            record = retrieve_retracted_identifier(item.get("pmid").value.strip())
                        else:
                            continue
                        in_use = None

                        for r in record:
                            if r.retraction_nature is None or r.retraction_nature == "Reinstatement":
                                wikitext.replace(str(raw_templates[i+1]), "")
                                continue
                            if r.retraction_nature is None or r.retraction_nature == "Retraction" and (in_use is None or in_use.retraction_nature != "Retraction"):
                                in_use = r
                            elif r.retraction_nature is None or r.retraction_nature == "Expression of concern" and (in_use is None or in_use.retraction_nature != "Retraction"):
                                in_use = r
                            elif r.retraction_nature is None or in_use is None:
                                in_use = r
                        new_code = process_item(in_use)
                        if raw_templates[i+1].has("intentional", ignore_empty=True):
                            new_code.add("intentional", raw_templates[i+1].get("intentional").value.strip())
                        if new_code is not None:
                            wikitext.replace(str(raw_templates[i+1]), str(new_code))

                page_text = str(wikitext)

                # Only bother trying to make an edit if we changed anything
                if page_text != wp_page.text and bot_can_run:
                    wp_page.text = page_text
                    edit_summary = "Flagging sources with dois highlighted by RetractionWatch."
                    wp_page.save(edit_summary, minor=False)

                    logger.info("Successfully edited {page_name} with "
                                "retracted source(s).".format(
                                    page_name=wp_page.title()
                                ))
                    for x in changes:
                        log_retraction_edit(datetime.datetime.now(),
                                        language + ".wikipedia.org",
                                        wp_page,
                                        x,
                                        0)
                    time.sleep(60) # 60s cooldown following edit

def process_item(record):
    new_code = ""
    if record.retraction_nature == "Retraction":
        logger.info("New Retraction template needed for DOI %s", record.original_doi)
        new_code = mwparserfromhell.nodes.template.Template(name="Retracted")
    elif record.retraction_nature == "Expression of concern":
        logger.info("New EoC template needed for DOI %s", record.original_doi)
        new_code = mwparserfromhell.nodes.template.Template(name="Expression of Concern")
    elif record.retraction_nature == "Correction":
        logger.info("New Erratum template needed for DOI %s", record.original_doi)
        new_code = mwparserfromhell.nodes.template.Template(name="Erratum")
    else:
        logger.debug("No change needed for doi %s", record.original_doi)
        return None
    
    if record.retraction_doi != '0':
        new_code.add("doi", record.retraction_doi)
    if record.retraction_pubmed != '0':
        new_code.add("pmid", record.retraction_pubmed)
    if record.url != "":
        for idx, x in enumerate(record.url.split(";")):
            if x != "":
                new_code.add(str(idx+1), x + " ''Retraction Watch''")
    return new_code


if __name__ == '__main__':
    logger.info("Starting bot run at {dt}".format(
        dt=datetime.datetime.now()
    ))
    run_bot()
