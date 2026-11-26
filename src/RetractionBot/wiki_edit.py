"""Conservative citation recognition and local, idempotent wikitext edits."""

import logging
import re

import mwparserfromhell
from mwparserfromhell.nodes import Comment, Template, Text

from .db import fold_doi
from .events import event_kind, select_record
from .identifiers import is_doi, is_pmid, normalize_notice_url

logger = logging.getLogger(__name__)
# Recognize the standard English family, configured local names, and explicit
# shorthand/module calls. Do not match arbitrary names containing 'cite'.
CITE_PREFIX = re.compile(r"^cite(?:\s+\S|$)")
CITE_ALIASES = {"citation", "citeweb", "citejournal", "citebook"}
# These are not bibliographic citation templates even though they begin 'cite'.
CITE_EXCLUSIONS = {"cite needed", "cite check", "cite bundle", "cite multiple"}
RW_SUFFIX = " ''Retraction Watch''"


def template_key(name):
    return " ".join(str(name).replace("_", " ").split()).casefold()


def is_citation_template(name, listed):
    name = template_key(name)
    return (name in listed or name in CITE_ALIASES or name == "#invoke:cite"
            or (name not in CITE_EXCLUSIONS and bool(CITE_PREFIX.match(name))))


def field_names(field_map, kind):
    """MediaWiki parameter names are case-sensitive; aliases must be explicit."""
    return (field_map.get(kind, kind), *field_map.get(kind + "_aliases", []))


def parameter_value(template, names):
    values = [str(p.value).strip() for p in template.params
              if str(p.name).strip() in names]
    # Conflicting duplicate identifiers are ambiguous. Never look one up.
    if len(set(values)) > 1:
        return None
    return values[-1] if values else ""


def citation_ids(item, listed, field_map):
    name = template_key(item.name)
    if name in {template_key(field_map.get("doi", "doi")), "doi-inline"}:
        doi, pmid = parameter_value(item, ("1",)), ""
    elif name == template_key(field_map.get("pmid", "pmid")):
        doi, pmid = "", parameter_value(item, ("1",))
    elif is_citation_template(name, listed):
        doi = parameter_value(item, field_names(field_map, "doi"))
        pmid = parameter_value(item, field_names(field_map, "pmid"))
    else:
        return "", ""
    if doi is None or pmid is None:
        return "", ""
    # Retain the bot's existing exclusion for Cochrane references.
    if "cochrane" in str(item).casefold():
        return "", ""
    return fold_doi(doi) if is_doi(doi) else "", pmid if is_pmid(pmid) else ""


def lookup_records(database, doi, pmid):
    doi_rows = database.retrieve_retracted_identifier(doi) if doi else []
    pmid_rows = database.retrieve_retracted_identifier(pmid) if pmid else []
    rows = [*doi_rows, *pmid_rows]
    # Both fields may be present, but refer to different papers. Do not merge
    # their event histories if the feed contradicts either supplied identifier.
    if any((doi and is_doi(r.original_doi) and fold_doi(r.original_doi) != doi)
           or (pmid and is_pmid(r.original_pubmed) and r.original_pubmed != pmid)
           for r in rows):
        return []
    if doi_rows and pmid_rows and not any(
        fold_doi(r.original_doi) == doi and r.original_pubmed == pmid for r in rows
    ):
        return []
    return rows


def status_names(template_map):
    return {
        "retraction": template_map.get("retraction", template_map.get("retracted", template_map.get("Retracted", "Retracted"))),
        "expression of concern": template_map.get("eoc", template_map.get("expression of concern", "Expression of Concern")),
        "correction": template_map.get("erratum", "Erratum"),
    }


def notice_links(record):
    urls = (normalize_notice_url(url) for url in record.url.split(";"))
    return list(dict.fromkeys(url + RW_SUFFIX for url in urls if url))


def safe_identifier(value):
    # Even syntactically valid DOI suffixes must not inject template syntax.
    return not any(c in value for c in "{}[]|")


def process_item(record, template_map, field_map):
    kind = event_kind(record)
    name = status_names(template_map).get(kind)
    if name is None:
        return None
    template = Template(name=name)
    doi = record.retraction_doi.strip()
    pmid = record.retraction_pubmed.strip()
    if is_doi(doi) and safe_identifier(doi):
        template.add(field_names(field_map, "doi")[0], doi)
    if is_pmid(pmid):
        template.add(field_names(field_map, "pmid")[0], pmid)
    for i, link in enumerate(notice_links(record), 1):
        template.add(str(i), link)
    return template


def numeric(name):
    return bool(re.fullmatch(r"[1-9][0-9]*", name))


def rw_link(param):
    """Only explicitly attributed Retraction Watch links are bot-owned."""
    name = str(param.name).strip()
    value = str(param.value).strip()
    if not numeric(name):
        return None
    match = re.fullmatch(r"(.*?)\s+''Retraction\s+Watch''", value)
    return match.group(1).strip() if match else None


def owned(param, field_map):
    name = str(param.name).strip()
    return (name in field_names(field_map, "doi") or name in field_names(field_map, "pmid")
            or rw_link(param) is not None)


def remove_param(template, param):
    # mwparserfromhell's removal API preserves implicit positional numbering.
    template.remove(param)


def ambiguous_numbering(template):
    groups = {}
    for param in template.params:
        name = str(param.name).strip()
        if numeric(name):
            groups.setdefault(name, []).append(param)
    return any(len(params) > 1 and any(rw_link(p) is None for p in params)
               for params in groups.values())


def update_template_in_place(existing, new, field_map):
    """Update only bot-owned fields; return False if unsafe to reconcile."""
    if ambiguous_numbering(existing):
        return False
    if template_key(existing.name) != template_key(new.name):
        # Avoid migrating editor-owned fields to a template of unknown schema.
        if any(not owned(p, field_map) for p in existing.params):
            return False
        old_name = str(existing.name)
        leading = old_name[:len(old_name) - len(old_name.lstrip())]
        trailing = old_name[len(old_name.rstrip()):]
        existing.name = leading + str(new.name).strip() + trailing

    for kind in ("doi", "pmid"):
        names = field_names(field_map, kind)
        old = [p for p in existing.params if str(p.name).strip() in names]
        desired = parameter_value(new, names)
        normalize = fold_doi if kind == "doi" else lambda v: v
        if len(old) == 1 and desired:
            if normalize(str(old[0].value).strip()) != normalize(desired):
                existing.add(str(old[0].name).strip(), desired)
        else:
            # Collapse duplicate managed aliases/parameters; remove stale or
            # empty managed fields, but never touch unsupported aliases.
            for param in old:
                remove_param(existing, param)
            if desired:
                existing.add(names[0], desired)

    desired_links = [str(p.value).strip() for p in new.params if rw_link(p) is not None]
    old_links = [p for p in existing.params if rw_link(p) is not None]
    old_values = [rw_link(p) + RW_SUFFIX for p in old_links]
    old_names = [str(p.name).strip() for p in old_links]
    # Ignore order and surrounding whitespace, but not an intentional HTTPS
    # upgrade. Duplicate keys/links are repaired, even if the last one agrees.
    if sorted(old_values) != sorted(desired_links) or len(set(old_names)) != len(old_names):
        for param in old_links:
            remove_param(existing, param)
        used = {str(p.name).strip() for p in existing.params}
        next_number = 1
        for value in desired_links:
            while str(next_number) in used:
                next_number += 1
            name = str(next_number)
            existing.add(name, value, showkey=True)
            used.add(name)
    return True


def adjacent_status(nodes, index, status_keys):
    """Find only status templates in the same sibling sequence."""
    found = []
    for j in range(index + 1, len(nodes)):
        node = nodes[j]
        if isinstance(node, Comment) or (isinstance(node, Text) and not str(node).strip()):
            continue
        if isinstance(node, Template) and template_key(node.name) in status_keys:
            found.append(j)
            continue
        break
    return found


def edit_container(code, database, listed, template_map, field_map, status_keys):
    changes = []
    # Reverse order keeps sibling indices stable while adding/removing tags.
    nodes = code.nodes
    for i in range(len(nodes) - 1, -1, -1):
        item = nodes[i]
        if not isinstance(item, Template):
            continue
        doi, pmid = citation_ids(item, listed, field_map)
        if not (doi or pmid):
            continue
        record = select_record(lookup_records(database, doi, pmid))
        if record is None:
            continue
        adjacent = adjacent_status(nodes, i, status_keys)
        if len(adjacent) > 1:
            logger.warning("Multiple adjacent status tags; manual review required")
            continue
        existing = nodes[adjacent[0]] if adjacent else None
        if event_kind(record) == "reinstatement":
            if existing is None:
                continue
            if any(not owned(p, field_map) for p in existing.params):
                logger.warning("Reinstated citation has editor-owned tag fields; manual review required")
                continue
            del nodes[adjacent[0]]
        else:
            new = process_item(record, template_map, field_map)
            if existing is None:
                nodes.insert(i + 1, new)
            else:
                before = str(existing)
                if not update_template_in_place(existing, new, field_map):
                    logger.warning("Ambiguous or editor-owned status tag; manual review required")
                    continue
                if str(existing) == before:
                    continue
        changes.append(record)
    return changes


def edit_page(text, database, citation_templates, template_map, field_map):
    """Edit direct page/ref siblings; never pair templates across containers."""
    code = mwparserfromhell.parse(text)
    listed = {template_key(name) for name in citation_templates}
    status_keys = {template_key(name) for name in status_names(template_map).values()}
    status_keys.update(template_key(name) for name in status_names({}).values())
    # A reference inside a reflist is still a reference, but arbitrary nested
    # citation-looking templates in unrelated parameters are not direct siblings.
    refs = [tag.contents for tag in code.filter_tags()
            if template_key(tag.tag) == "ref" and tag.contents is not None]
    changes = []
    for container in [code, *refs]:
        changes.extend(edit_container(container, database, listed, template_map, field_map, status_keys))
    return str(code), changes
