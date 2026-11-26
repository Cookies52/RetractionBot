"""
Existing {{Retracted}}, {{Erratum}} and {{Expression of concern}} tags.

The bot should save a page only when the notice DOI, PMID or Retraction Watch
links really change, and leave every other field where it is. Each input is a
real citation and tag from English Wikipedia, cut down to the <ref>. The
fake database rows match Retraction Watch's data file unless a comment says
otherwise.

Runs the whole run_bot() offline, like tests/test_offline_page_edits.py in
PR #14, but stands alone: `python -m pytest` from the repo root.
"""

import os

# pywikibot looks for a user-config.py when imported. These tests have no wiki
# account and never create a Site, so tell it not to look.
os.environ.setdefault("PYWIKIBOT_NO_USER_CONFIG", "2")

import copy
import socket
from pathlib import Path
from types import SimpleNamespace

import mwparserfromhell
import pytest
import yaml

from src.RetractionBot import retraction_bot as bot
from src.RetractionBot.db import Retraction, fold_doi

with open(Path(__file__).resolve().parents[1] / "bot_settings.yml") as f:
    _settings = yaml.safe_load(f)

# The real English settings, so the bot knows all three tag names.
SETTINGS = {
    key: {"en": _settings[key]["en"]}
    for key in (
        "template_name_map",
        "template_field_names",
        "summary_map",
        "citation_templates",
    )
}
SETTINGS["db"] = {}

RW = " ''Retraction Watch''"


def record(nature, paper, notice, urls=()):
    """A database row. paper and notice are (doi, pmid) pairs."""
    return Retraction(
        b"Crossref",
        paper[0].encode(),
        notice[0].encode(),
        paper[1].encode(),
        notice[1].encode(),
        nature.encode(),
        ";".join(urls).encode(),
    )


class FakeDatabase:
    def __init__(self, records):
        self.records = records

    def load_retracted_identifiers(self):
        return [
            (r.original_doi.encode(), r.original_pubmed.encode()) for r in self.records
        ]

    def retrieve_retracted_identifier(self, identifier):
        # Same match as the SQL: DOIs fold A-Z, PMIDs compare exactly.
        return [
            r
            for r in self.records
            if fold_doi(r.original_doi) == fold_doi(identifier)
            or r.original_pubmed == identifier
        ]

    def log_retraction_edit(self, *args, **kwargs):
        pass


class Page:
    def __init__(self, text):
        self.text = text
        self.saved = []

    def save(self, *args, **kwargs):
        self.saved.append(self.text)

    def title(self):
        return "Offline fixture"


@pytest.fixture(autouse=True)
def prohibit_network(monkeypatch):
    def deny(*args, **kwargs):
        pytest.fail("The offline page-edit fixture attempted to use the network")

    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket, "getaddrinfo", deny)


@pytest.fixture
def run_bot(monkeypatch):
    wiki = SimpleNamespace(login=lambda: None)
    switch = SimpleNamespace(
        text="yes", get=lambda **kwargs: "yes", title=lambda: "run"
    )
    monkeypatch.setattr(bot, "load_bot_settings", lambda: copy.deepcopy(SETTINGS))
    monkeypatch.setattr(bot.pywikibot, "Site", lambda *args: wiki)
    monkeypatch.setattr(bot.pywikibot, "Page", lambda *args: switch)
    monkeypatch.setattr(bot.time, "sleep", lambda seconds: None)

    def run(page, records):
        monkeypatch.setattr(bot, "Database", lambda *args: FakeDatabase(records))
        monkeypatch.setattr(
            bot.pagegenerators,
            "SearchPageGenerator",
            lambda *args, **kwargs: iter([page]),
        )
        bot.run_bot()

    return run


# Case 1: reordering and spacing only.
# 2026 in paleomammalogy, Special:Diff/1383029930. The bot moved doi-access
# to the end. User talk:RetractionBot/Archive 3, "Needless parameter
# reordering".
PALEO_CITE = (
    "{{Cite journal|last1=Peredo |first1=C. M. |last2=Pyenson |first2=N. "
    "|last3=Uhen |first3=M. D. |year=2022 |title=Lateral palatal foramina do "
    "not indicate baleen in fossil whales |journal=Scientific Reports "
    "|volume=12 |issue=1 |at=11448 |doi=10.1038/s41598-022-15684-8 "
    "|pmid=35794235 |pmc=9259611 |bibcode=2022NatSR..1211448P "
    "| doi-access = free }}"
)
PALEO_TAG = (
    "{{Retracted|doi=10.1038/s41598-025-88714-w|doi-access=free"
    "|pmid=39934295|intentional=yes}}"
)
# The same tag with editor-style spacing. Not from the wiki.
PALEO_TAG_SPACED = (
    "{{Retracted | doi = 10.1038/s41598-025-88714-w | doi-access = free\n"
    " | pmid = 39934295 | intentional = yes }}"
)
PALEO_DATA = [
    record(
        "Retraction",
        ("10.1038/s41598-022-15684-8", "35794235"),
        ("10.1038/s41598-025-88714-w", "39934295"),
    )
]

# Case 2: the tag has https, the data has http.
# Scientific misconduct, the bot's edit oldid 1384078028. Archive 3,
# "https replaced with http - seems backwards".
KATO_URL = (
    "retractionwatch.com/2012/06/13/"
    "shikeagi-kato-who-resigned-post-in-march-retracts-nature-paper/"
)
KATO_CITE = (
    "{{Cite journal\n"
    "| last1 = Kim | first1 = M. S.\n"
    "| last2 = Kondo | first2 = T.\n"
    "| last3 = Takada | first3 = I.\n"
    "| last4 = Youn | first4 = M. Y.\n"
    "| last5 = Yamamoto | first5 = Y.\n"
    "| last6 = Takahashi | first6 = S.\n"
    "| last7 = Matsumoto | first7 = T.\n"
    "| last8 = Fujiyama | first8 = S.\n"
    "| last9 = Shirode | first9 = Y.\n"
    "| doi = 10.1038/nature08456\n"
    "| last10 = Yamaoka | first10 = I.\n"
    "| last11 = Kitagawa | first11 = H.\n"
    "| last12 = Takeyama | first12 = K. I.\n"
    "| last13 = Shibuya | first13 = H.\n"
    "| last14 = Ohtake | first14 = F.\n"
    "| last15 = Kato | first15 = S.\n"
    "| title = DNA demethylation in hormone-induced transcriptional "
    "derepression\n"
    "| journal = Nature\n"
    "| volume = 461\n"
    "| issue = 7266\n"
    "| pages = 1007–1012\n"
    "| year = 2009\n"
    "| pmid = 19829383\n"
    "| bibcode = 2009Natur.461.1007K| s2cid = 4426439\n"
    "}}"
)
KATO_TAG = (
    "{{Retracted|doi=10.1038/nature11164|pmid=22699624"
    f"|https://{KATO_URL}{RW}|intentional=yes}}}}"
)
KATO_DATA = [
    record(
        "Retraction",
        ("10.1038/nature08456", "19829383"),
        ("10.1038/nature11164", "22699624"),
        [f"http://{KATO_URL}"],
    )
]

# Stephen Jackson (biologist), Special:Diff/1291919289: three links on two
# tags went from https to http. Archive 2, "Bot changing https:// URLs to
# http://".
WANSINK_URL = (
    "retractionwatch.com/2018/09/29/weekend-reads-lessons-from-the-downfall-"
    "of-brian-wansink-scientific-terrorism-redux-why-cochrane-booted-a-member/"
)
SAME_DAY_URL = (
    "retractionwatch.com/2019/04/11/"
    "authors-have-papers-in-nature-and-science-retracted-on-the-same-day/"
)
JACKSON_REFS = (
    "<ref>{{Cite journal |last1=Kaidi |first1=Abderrahmane |last2=Weinert "
    "|first2=Brian T. |last3=Choudhary |first3=Chunaram |last4=Jackson "
    "|first4=Stephen P. |date=2010-09-10 |title=RETRACTED: Human SIRT6 "
    "promotes DNA end resection through CtIP deacetylation|journal=Science "
    "|volume=329 |issue=5997 |pages=1348–1353 |doi=10.1126/science.1192049 "
    "|issn=1095-9203 |pmc=3276839 |pmid=20829486"
    "|bibcode=2010Sci...329.1348K }}"
    "{{Retracted|doi=10.1126/science.aax4558|pmid=30975768"
    f"|https://{WANSINK_URL}{RW}|https://{SAME_DAY_URL}{RW}|intentional=yes}}}}"
    "</ref>"
    "<ref>{{Cite journal |last1=Kaidi |first1=Abderrahmane |last2=Jackson "
    "|first2=Stephen P. |date=2013-06-06 |title=KAT5 tyrosine phosphorylation "
    "couples chromatin sensing to ATM signalling |journal=Nature |volume=498 "
    "|issue=7452 |pages=70–74 |doi=10.1038/nature12201 |pmc=3859897 "
    "|pmid=23708966|bibcode=2013Natur.498...70K }}"
    "{{Retracted|doi=10.1038/s41586-019-1142-2|pmid=30976101"
    f"|https://{SAME_DAY_URL}{RW}|intentional=yes}}}}"
    "</ref>"
)
JACKSON_DATA = [
    record(
        "Retraction",
        ("10.1126/science.1192049", "20829486"),
        ("10.1126/science.aax4558", "30975768"),
        [f"http://{WANSINK_URL}", f"http://{SAME_DAY_URL}"],
    ),
    record(
        "Expression of concern",
        ("10.1126/science.1192049", "20829486"),
        ("10.1126/science.aav4528", "30262489"),
        [f"http://{WANSINK_URL}"],
    ),
    record(
        "Retraction",
        ("10.1038/nature12201", "23708966"),
        ("10.1038/s41586-019-1142-2", "30976101"),
        [f"http://{SAME_DAY_URL}"],
    ),
]

# Inositol, Special:Diff/1248015704: https to http, and "concern" to
# "Concern". Archive 2, "eh?".
INOSITOL_URL = (
    "retractionwatch.com/2018/07/23/"
    "journal-flags-fertility-paper-after-company-wins-court-order/"
)
INOSITOL_CITE = (
    "{{cite journal |last1=Isabella |first1=R. |last2=Raffone |first2=E. "
    "|title=Does ovary need D-''chiro''-inositol? |journal=Journal of Ovarian "
    "Research |volume=5 |issue=1 |date=2012 |page=14 |pmid=22587479 "
    "|doi=10.1186/1757-2215-5-14 |doi-access=free |pmc=3447676}}"
)
INOSITOL_TAG = (
    "{{Expression of concern|doi=10.1186/s13048-018-0431-y|pmid=29976256"
    f"|https://{INOSITOL_URL}{RW}}}}}"
)
# Retraction Watch lists a second notice first. The bot uses the last.
INOSITOL_DATA = [
    record(
        "Expression of concern",
        ("10.1186/1757-2215-5-14", "22587479"),
        ("10.1186/s13048-018-0470-4", "30470242"),
        [f"http://{INOSITOL_URL}"],
    ),
    record(
        "Expression of concern",
        ("10.1186/1757-2215-5-14", "22587479"),
        ("10.1186/s13048-018-0431-y", "29976256"),
        [f"http://{INOSITOL_URL}"],
    ),
]

# Case 3: the reverse. The tag has http, the data has https.
# Benjamin K. Sovacool, the article as it stands. Retraction Watch's row
# still has http today. This row is the same with https, as it already is for
# newer posts.
SOVACOOL_URL = (
    "retractionwatch.com/2016/11/28/"
    "authors-retract-paper-linking-nuclear-power-slow-action-climate-change/"
)
SOVACOOL_CITE = (
    "{{cite journal |last1=Lawrence |first1=Andrew |last2=Sovacool "
    "|first2=Benjamin |last3=Stirling |first3=Andrew |title=RETRACTED "
    "ARTICLE: Nuclear energy and path dependence in Europe's 'Energy union': "
    "coherence or continued divergence? |journal=Climate Policy "
    "|date=3 July 2016 |volume=16 |issue=5 |pages=622–641 "
    "|doi=10.1080/14693062.2016.1179616 |doi-access=free "
    "|bibcode=2016CliPo..16..622L }}"
)
SOVACOOL_TAG = (
    "{{Retracted|doi=10.1080/14693062.2016.1260249"
    f"|http://{SOVACOOL_URL}{RW}|intentional=yes}}}}"
)
SOVACOOL_DATA = [
    record(
        "Retraction",
        ("10.1080/14693062.2016.1179616", ""),
        ("10.1080/14693062.2016.1260249", ""),
        [f"https://{SOVACOOL_URL}"],
    )
]

# Case 4: fields editors add must stay.
# Frontiers in Psychology, before oldid 1314331608, which dropped
# doi-access=free. Archive 2, "Bot shouldn't remove doi-access=free".
FRONTIERS_URLS = [
    "retractionwatch.com/2014/03/21/controversial-paper-linking-conspiracy-"
    "ideation-to-climate-change-skepticism-formally-retracted/",
    "retractionwatch.com/2014/04/04/journal-that-retracted-conspiracy-"
    "ideation-climate-skepticism-paper-says-it-did-not-cave-into-threats/",
    "retractionwatch.com/2014/04/06/co-author-of-retracted-conspiracy-"
    "ideation-climate-skepticism-paper-addresses-apparent-contradictions/",
    "retractionwatch.com/2015/07/08/recursive-recursiveness-retracted-"
    "lewandowsky-et-al-conspiracy-ideation-study-republished/",
]
FRONTIERS_CITE = (
    "{{Cite journal |last1=Lewandowsky |first1=Stephan |last2=Cook "
    "|first2=John |last3=Oberauer |first3=Klaus |last4=Marriott "
    "|first4=Michael |date=2013 |title=Recursive fury: Conspiracist ideation "
    "in the blogosphere in response to research on conspiracist ideation "
    "|journal=Frontiers in Psychology |volume=4 |page=73 "
    "|doi=10.3389/fpsyg.2013.00073 |pmid=23508808 |pmc=3600613 "
    "|issn=1664-1078 |doi-access=free }}"
)
FRONTIERS_TAG = (
    "{{Retracted|doi=10.3389/fpsyg.2014.00293|doi-access=free|pmid=24683402"
    + "".join(f"|http://{url}{RW}" for url in FRONTIERS_URLS)
    + "|intentional=yes}}"
)
FRONTIERS_DATA = [
    record(
        "Retraction",
        ("10.3389/fpsyg.2013.00073", "23508808"),
        ("10.3389/fpsyg.2014.00293", "24683402"),
        [f"http://{url}" for url in FRONTIERS_URLS],
    )
]

# MDPI, before oldid 1232637577, which dropped checked=yes. Archive 2,
# "Removes existing parameter checked=yes".
MDPI_URL = (
    "retractionwatch.com/2018/06/13/"
    "journal-corrects-but-will-not-retract-controversial-paper-on-internet-porn/"
)
MDPI_CITE = (
    "{{cite journal|doi = 10.3390/bs6030017|pmc = 5039517|pmid = 27527226"
    "|title = Is Internet Pornography Causing Sexual Dysfunctions? A Review "
    "with Clinical Reports.|journal = [[Behavioral Sciences (journal)"
    "|Behavioral Sciences]]|volume = 6|issue = 3|pages = 17|year = 2016"
    "|last1 = Park|first1 = Brian Y.|last2 = Wilson|first2 = Gary"
    "|last3 = Berger|first3 = Jonathan|last4 = Christman|first4 = Matthew"
    "|last5 = Reina|first5 = Bryn|last6 = Bishop|first6 = Frank"
    "|last7 = Klam|first7 = Warren|last8 = Doan|first8 = Andrew P."
    "|doi-access = free}}"
)
MDPI_TAG = (
    "{{Erratum|doi=10.3390/bs8060055|pmid=29857562"
    f"|http://{MDPI_URL}{RW}|checked=yes}}}}"
)
MDPI_DATA = [
    record(
        "Correction",
        ("10.3390/bs6030017", "27527226"),
        ("10.3390/bs8060055", "29857562"),
        [f"http://{MDPI_URL}"],
    )
]

# CEBPB, before oldid 1314618422, which swapped pmc for doi=unavailable.
# Archive 2, "adds unsupported doi=unavailable, removes pmc". PubMed has no
# DOI for this notice either.
CEBPB_URL = (
    "retractionwatch.com/2011/06/21/the-way-science-should-work-a-swift-"
    "clearly-worded-retraction-in-gd-after-legitimate-questions-by-another-"
    "group/"
)
CEBPB_CITE = (
    "{{cite journal | vauthors = Rooney JW, Calame KL | title = TIF1beta "
    "functions as a coactivator for C/EBPbeta and is required for induced "
    "differentiation in the myelomonocytic cell line U937 | journal = Genes "
    "& Development | volume = 15 | issue = 22 | pages = 3023–3038 "
    "| date = November 2001 | pmid = 11711437 | pmc = 312827 "
    "| doi = 10.1101/gad.937201 }}"
)
CEBPB_TAG = (
    f"{{{{Retracted|pmc=186452|pmid=12269264|http://{CEBPB_URL}{RW}"
    "|intentional=yes}}"
)
CEBPB_DATA = [
    record(
        "Retraction",
        ("10.1101/gad.937201", "11711437"),
        ("unavailable", "12269264"),
        [f"http://{CEBPB_URL}"],
    )
]

# A link with "=" in it. Gideon Koren, before Special:Diff/1359061722. The
# wiki reads "|http://retractionwatch.com/?s=gideon+koren ..." as a field
# named "http://retractionwatch.com/?s", not as the second link. Retraction
# Watch's row today has the same two links, so nothing needs to change.
KOREN_URLS = [
    "http://retractionwatch.com/2019/02/19/controversial-pediatrics-"
    "researcher-has-20-year-old-paper-retracted-for-misconduct/",
    "http://retractionwatch.com/?s=gideon+koren",
]
KOREN_CITE = (
    "{{Cite journal|vauthors=Diav-Citrin O, Atanackovic G, Koren G "
    "|title=An investigation into variability in the therapeutic response to "
    "deferiprone in patients with thalassemia major |journal=Therapeutic Drug "
    "Monitoring |volume=21 |issue=1 |pages=74–81 |date=February 1999 "
    "|pmid=10051057 |doi=10.1097/00007691-199902000-00011}}"
)
KOREN_TAG = (
    "{{Retracted|doi=10.1097/FTD.0000000000000607|pmid=30768496"
    + "".join(f"|{url}{RW}" for url in KOREN_URLS)
    + "|intentional=yes}}"
)
KOREN_DATA = [
    record(
        "Retraction",
        ("10.1097/00007691-199902000-00011", "10051057"),
        ("10.1097/FTD.0000000000000607", "30768496"),
        KOREN_URLS,
    )
]

# The same with "=" in the first link. Francesca Gino, before
# Special:Diff/1358978642. There the citation is a list item, not a <ref>;
# it is wrapped in one here because the bot skips pages with no <ref> DOIs.
GINO_URLS = [
    "http://retractionwatch.com/?s=%22Dan+Ariely%22",
    "http://retractionwatch.com/2021/09/14/"
    "highly-criticized-paper-on-dishonesty-retracted/",
]
GINO_CITE = (
    "{{cite journal |last1=Shu |last2=Mazar |last3=Gino | last4=Ariely "
    "| last5=Bazerman |date=2012 |title=Signing at the beginning makes ethics "
    "salient… |journal=[[Proceedings of the National Academy of Sciences]]"
    "|volume=109 |issue=38 |pages=15197–15300 |doi=10.1073/pnas.1209746109 "
    "|doi-access=free |pmid=22927408 |pmc=3458378 |author4-link=Dan Ariely }}"
)
GINO_TAG = (
    "{{Retracted|doi=10.1073/pnas.2115397118|pmid=34518237"
    + "".join(f"|{url}{RW}" for url in GINO_URLS)
    + "|intentional=yes}}"
)
GINO_DATA = [
    record(
        "Retraction",
        ("10.1073/pnas.1209746109", "22927408"),
        ("10.1073/pnas.2115397118", "34518237"),
        GINO_URLS,
    )
]

# Real changes.
# Susan B. Anthony Pro-Life America, the bot's edit oldid 1380779004 added
# two Retraction Watch links.
SBA_URLS = [
    "https://retractionwatch.com/2024/02/06/"
    "papers-used-by-judge-to-justify-abortion-pill-suspension-retracted/",
    "https://retractionwatch.com/2024/10/04/authors-sue-sage-over-"
    "discriminatory-retractions-of-papers-cited-in-abortion-pill-case/",
    "https://retractionwatch.com/2026/09/15/lawsuit-over-retraction-of-"
    "abortion-pill-papers-heads-toward-arbitration-decision/",
]
SBA_CITE = (
    "{{Cite journal |last1=Studnicki |first1=James |last2=Harrison "
    "|first2=Donna J. |last3=Longbons |first3=Tessa |last4=Skop "
    "|first4=Ingrid |author-link4=Ingrid Skop |last5=Reardon "
    "|first5=David C. |last6=Fisher |first6=John W. |last7=Tsulukidze "
    "|first7=Maka |last8=Craver |first8=Christopher |date=2021-11-09 "
    "|title=A Longitudinal Cohort Study of Emergency Room Utilization "
    "Following Mifepristone Chemical and Surgical Abortions, 1999–2015 "
    "|journal=Health Services Research and Managerial Epidemiology "
    "|language=en |volume=8 |pages= |doi=10.1177/23333928211053965 "
    "|issn=2333-3928 |pmc=8581786 |pmid=34778493}}"
)
SBA_TAG = (
    "{{Retracted|doi=10.1177/23333928231216699|pmid=38328416"
    f"|{SBA_URLS[0]}{RW}|intentional=yes}}}}"
)
SBA_DATA = [
    record(
        "Expression of concern",
        ("10.1177/23333928211053965", "34778493"),
        ("10.1177/23333928231189400", "37529765"),
        SBA_URLS,
    ),
    record(
        "Retraction",
        ("10.1177/23333928211053965", "34778493"),
        ("10.1177/23333928231216699", "38328416"),
        SBA_URLS,
    ),
]

# Chernobyl disaster, the bot's edit oldid 1225795847 tagged the wrong notice
# DOI. Archive 2, "Mistake": the right one is 10.1038/36384, which PubMed
# lists for PMID 9363899. Retraction Watch still has the wrong one; the row
# here is the fixed one. The bot wrote pmed= then, a typo since fixed; this
# uses pmid=, as it writes today.
CHERNOBYL_CITE = (
    "{{cite journal |last1=Barker |first1=Robert J. |last2=Van Den Bussche "
    "|first2=Ronald A. |last3=Wright |first3=Amanda J. |last4=Wiggins "
    "|first4=Lara E. |last5=Hamilton |first5=Meredith J. |last6=Reat "
    "|first6=Erin P. |last7=Smith |first7=Micheal H. |last8=Lomakin "
    "|first8=Micheal D. |last9=Chesser |first9=Ronald K. |title=High levels "
    "of genetic change in rodents of Chernobyl |journal=Nature "
    "|date=April 1996 |volume=380 |issue=6576 |pages=707–708 "
    "|doi=10.1038/380707a0|pmid=8614463 |bibcode=1996Natur.380..707B "
    "|s2cid=4351740 }}"
)
CHERNOBYL_TAG = "{{Retracted|doi=10.1038/36382|pmid=9363899}}"
CHERNOBYL_DATA = [
    record(
        "Retraction",
        ("10.1038/380707a0", "8614463"),
        ("10.1038/36384", "9363899"),
    )
]

# Effects of climate change, the bot's edit oldid 1385925540 turned this
# {{Erratum}} into {{Retracted}}. The data has the correction row first.
KOTZ_URLS = [
    "https://retractionwatch.com/2025/12/03/"
    "authors-retract-nature-paper-projecting-high-costs-of-climate-change/",
    "https://retractionwatch.com/2026/09/02/oxford-researchers-retract-"
    "nature-paper-on-atmospheric-thirst-for-calculation-errors/",
]
KOTZ_CITE = (
    "{{Cite journal |last1=Kotz |first1=Mazimilian. |last2=Levermann "
    "|first2=Anders |last3=Wenz |first3=Leonie |date=2024-04-17 "
    "|title=The economic commitment of climate change |journal=Nature "
    "|language=en |volume=628 |issue=8008 |pages=551–557 "
    "|doi=10.1038/s41586-024-07219-0 |pmc=11023931 |pmid=38632481"
    "|bibcode=2024Natur.628..551K }}"
)
KOTZ_TAG = (
    "{{Erratum|doi=10.1038/s41586-024-07732-2|pmid=38914865"
    f"|{KOTZ_URLS[0]}{RW}|checked=yes}}}}"
)
KOTZ_DATA = [
    record(
        "Correction",
        ("10.1038/s41586-024-07219-0", "38632481"),
        ("10.1038/s41586-024-07732-2", "38914865"),
        KOTZ_URLS[:1],
    ),
    record(
        "Retraction",
        ("10.1038/s41586-024-07219-0", "38632481"),
        ("10.1038/s41586-025-09726-0", "41339560"),
        KOTZ_URLS,
    ),
]


def ref(cite, tag, name=""):
    return f"<ref{name}>{cite}{tag}</ref>"


FORMATTING_ONLY = [
    pytest.param(ref(PALEO_CITE, PALEO_TAG), PALEO_DATA, id="reordered"),
    pytest.param(ref(PALEO_CITE, PALEO_TAG_SPACED), PALEO_DATA, id="respaced"),
]
HTTPS_KEPT = [
    pytest.param(ref(KATO_CITE, KATO_TAG, ' name="Kato"'), KATO_DATA, id="misconduct"),
    pytest.param(JACKSON_REFS, JACKSON_DATA, id="jackson"),
    pytest.param(ref(INOSITOL_CITE, INOSITOL_TAG), INOSITOL_DATA, id="inositol"),
]
HTTP_KEPT = [
    pytest.param(
        ref(SOVACOOL_CITE, SOVACOOL_TAG, ' name="LawrenceSovacool2016"'),
        SOVACOOL_DATA,
        id="sovacool",
    ),
]
EDITOR_FIELDS_KEPT = [
    pytest.param(ref(FRONTIERS_CITE, FRONTIERS_TAG), FRONTIERS_DATA, id="doi-access"),
    pytest.param(
        ref(MDPI_CITE, MDPI_TAG, " name = OrigEDpaper"), MDPI_DATA, id="checked"
    ),
    pytest.param(
        ref(CEBPB_CITE, CEBPB_TAG, " name = pmid11711437"), CEBPB_DATA, id="pmc"
    ),
]
EQUALS_IN_LINK = [
    pytest.param(ref(KOREN_CITE, KOREN_TAG), KOREN_DATA, id="equals-in-link"),
    pytest.param(ref(GINO_CITE, GINO_TAG), GINO_DATA, id="equals-in-first-link"),
]
REAL_CHANGES = [
    pytest.param(ref(SBA_CITE, SBA_TAG), SBA_DATA, id="new-links"),
    pytest.param(ref(CHERNOBYL_CITE, CHERNOBYL_TAG), CHERNOBYL_DATA, id="new-doi"),
    pytest.param(ref(KOTZ_CITE, KOTZ_TAG, ' name="Kotz2024"'), KOTZ_DATA, id="erratum"),
]


@pytest.mark.parametrize("text, data", FORMATTING_ONLY)
def test_reordered_or_respaced_tag_is_not_saved(run_bot, text, data):
    page = Page(text)
    run_bot(page, data)
    assert page.saved == []


@pytest.mark.parametrize("text, data", HTTPS_KEPT)
def test_https_link_is_not_saved_as_http(run_bot, text, data):
    page = Page(text)
    run_bot(page, data)
    assert page.saved == []


@pytest.mark.parametrize("text, data", HTTP_KEPT)
def test_http_link_is_not_saved_as_https(run_bot, text, data):
    page = Page(text)
    run_bot(page, data)
    assert page.saved == []


@pytest.mark.parametrize("text, data", EDITOR_FIELDS_KEPT)
def test_editor_fields_alone_do_not_cause_a_save(run_bot, text, data):
    page = Page(text)
    run_bot(page, data)
    assert page.saved == []


@pytest.mark.parametrize("text, data", EQUALS_IN_LINK)
def test_link_with_equals_sign_is_not_added_again(run_bot, text, data):
    page = Page(text)
    run_bot(page, data)
    assert page.saved == []


def test_new_links_are_saved_and_intentional_stays(run_bot):
    page = Page(ref(SBA_CITE, SBA_TAG))
    run_bot(page, SBA_DATA)
    new_tag = (
        "{{Retracted|doi=10.1177/23333928231216699|pmid=38328416"
        f"|{SBA_URLS[0]}{RW}|intentional=yes|{SBA_URLS[1]}{RW}"
        f"|{SBA_URLS[2]}{RW}}}}}"
    )
    assert page.saved == [ref(SBA_CITE, new_tag)]


def test_new_notice_doi_is_saved_in_place(run_bot):
    page = Page(ref(CHERNOBYL_CITE, CHERNOBYL_TAG))
    run_bot(page, CHERNOBYL_DATA)
    new_tag = "{{Retracted|doi=10.1038/36384|pmid=9363899}}"
    assert page.saved == [ref(CHERNOBYL_CITE, new_tag)]


def test_erratum_becomes_retracted_and_checked_stays(run_bot):
    page = Page(ref(KOTZ_CITE, KOTZ_TAG, ' name="Kotz2024"'))
    run_bot(page, KOTZ_DATA)
    new_tag = (
        "{{Retracted|doi=10.1038/s41586-025-09726-0|pmid=41339560"
        f"|{KOTZ_URLS[0]}{RW}|checked=yes|{KOTZ_URLS[1]}{RW}}}}}"
    )
    assert page.saved == [ref(KOTZ_CITE, new_tag, ' name="Kotz2024"')]


@pytest.mark.parametrize(
    "text, data",
    FORMATTING_ONLY
    + HTTPS_KEPT
    + HTTP_KEPT
    + EDITOR_FIELDS_KEPT
    + EQUALS_IN_LINK
    + REAL_CHANGES,
)
def test_second_run_saves_nothing_more(run_bot, text, data):
    page = Page(text)
    run_bot(page, data)
    run_bot(page, data)
    assert len(page.saved) <= 1


def existing_tag(text):
    return mwparserfromhell.parse(text).filter_templates()[0]


def generated_tag(*urls):
    data = record(
        "Retraction",
        ("10.1038/nature08456", "19829383"),
        ("10.1038/nature11164", "22699624"),
        urls,
    )
    en = SETTINGS["template_name_map"]["en"]
    fields = SETTINGS["template_field_names"]["en"]
    return bot.process_item(data, en, fields)


def test_uppercase_DOI_gets_a_lowercase_doi():
    # The three tags read {{{doi|}}} only, so DOI= shows nothing.
    tag = existing_tag("{{Retracted|DOI=10.1038/nature11164|pmid=22699624}}")
    bot.update_template_in_place(tag, generated_tag(), "doi", "pmid")
    assert str(tag) == (
        "{{Retracted|DOI=10.1038/nature11164|pmid=22699624|doi=10.1038/nature11164}}"
    )


KATO_LINK = f"https://{KATO_URL}{RW}"
# The address the cite web in Scientific misconduct uses for the same post.
OLD_KATO_LINK = (
    "https://retractionwatch.wordpress.com/2012/06/13/"
    f"shikeagi-kato-who-resigned-post-in-march-retracts-nature-paper/{RW}"
)


@pytest.mark.parametrize(
    "links, expected",
    [
        # MediaWiki shows the last 1=, and it is right: no edit.
        pytest.param(
            f"|1={OLD_KATO_LINK}|1={KATO_LINK}",
            f"|1={OLD_KATO_LINK}|1={KATO_LINK}",
            id="last-right",
        ),
        # Only the first is right: one right 1= is left.
        pytest.param(
            f"|1={KATO_LINK}|1={OLD_KATO_LINK}",
            f"|1={KATO_LINK}",
            id="first-right",
        ),
    ],
)
def test_duplicate_1_fields(links, expected):
    head = "{{Retracted|doi=10.1038/nature11164|pmid=22699624"
    tag = existing_tag(head + links + "|intentional=yes}}")
    new = generated_tag(f"https://{KATO_URL}")
    bot.update_template_in_place(tag, new, "doi", "pmid")
    assert str(tag) == head + expected + "|intentional=yes}}"
