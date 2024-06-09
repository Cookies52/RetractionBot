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
                                                           namespaces=[0],

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
                page_cites = [x for x in wikitext.filter_tags() if re.findall(DOI_REGEX, str(x))]

                num_cites_found = len(page_cites)

                if num_cites_found == 0:
                    logger.debug("Couldn't find any DOIs inside "
                             "<ref> tags on page {page}.".format(
                                page=wp_page))
                    continue
                    
                logger.info("Page %s has %d dois cited", wp_page, num_cites_found)
                for cite in page_cites:
                    multiple_DOI = False

                    doi = re.findall(pattern=DOI_REGEX, string=str(cite))

                    if len(doi) != 1:
                        # This normally indicates a malformed reference or something else gone wrong
                        multiple_DOI = True

                    retracted_data = retrieve_retracted_identifier(doi[0])
                    if retracted_data is None or len(retracted_data) == 0:
                        logger.debug("skipping %s, doi doesn't appear to be retracted", doi[0])
                        continue
                    
                    # if > 1, try and select which paper is retracted.
                    if len (retracted_data) > 1:
                        for x in retracted_data:
                            if x[6].decode("utf-8"):
                                record = x
                                continue
                    else:
                        record = retracted_data[0]

                                
                    # for identifier in retacted_identifiers:
                    original_doi = record[2].decode("utf-8")
                    retraction_doi = record[3].decode("utf-8")
                    original_pubmed = record[4].decode("utf-8")
                    retraction_pubmed = record[5].decode("utf-8")
                    entry_type = record[6].decode("utf-8")
                    url = record[7].decode("utf-8")

                    cite_str = cite.contents

                    # We want to ignore journal=Cochrane Database Syst Rev for now due to issues with processing
                    if "journal=cochrane database syst rev" in cite_str.lower():
                        continue

                    # If we have attempted to fix this DOI before then skip
                    if len(check_edits(wp_page, original_doi)) != 0:
                        logger.debug("Skipping already completed page %s for DOI %s", wp_page.title(), original_doi)
                        continue

                    templates = cite_str.filter_templates()

                    if len(templates) == 1 and multiple_DOI is False:
                        new_code = ""
                        if entry_type == "Retraction":
                            logger.info("New Retraction template needed for DOI %s", original_doi)
                            new_code = mwparserfromhell.nodes.template.Template(name="Retracted")
                        elif entry_type == "Expression of concern":
                            logger.info("New EoC template needed for DOI %s", original_doi)
                            new_code = mwparserfromhell.nodes.template.Template(name="Expression of Concern")
                        elif entry_type == "Correction":
                            logger.info("New Erratum template needed for DOI %s", original_doi)
                            new_code = mwparserfromhell.nodes.template.Template(name="Erratum")
                        else:
                            logger.debug("No change needed for doi %s", original_doi)
                            continue
                        
                        if retraction_doi != '0':
                            new_code.add("doi", retraction_doi)
                        if retraction_pubmed != '0':
                            new_code.add("pmid", retraction_pubmed)
                        if url != "":
                           for idx, x in enumerate(url.split(";")):
                              if x != "":
                                 new_code.add(str(idx+1), x + " ''Retraction Watch''")
                        
                        changes.append(original_doi)
                        cite_str.append(new_code)
                    
                    else:
                        # Check the template is the correct type
                        for t in templates:
                            if t.name == "Expression of Concern" and entry_type == "Retraction":
                                t.name = "Retraction"
                            if t.name == "Retraction" and entry_type == "Expression of Concern":
                                t.name = "Expression of Concern"

                page_text = str(wikitext)

                # Only bother trying to make an edit if we changed anything
                if page_text != wp_page.text and bot_can_run:
                    wp_page.text = page_text
                    edit_summary = "Flagging sources with dois marked as retracted by RetractionWatch."

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

if __name__ == '__main__':
    logger.info("Starting bot run at {dt}".format(
        dt=datetime.datetime.now()
    ))
    run_bot()
