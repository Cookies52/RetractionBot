"""Find citations and change their adjacent notice tags without touching other nodes."""

from dataclasses import dataclass
import re

import mwparserfromhell
from mwparserfromhell.nodes import Comment, Template, Text

from .db import fold_doi
from .identifiers import is_doi, is_pmid


@dataclass
class PageChange:
    record: object
    action: str


def normalize_template_name(name):
    name = " ".join(str(name).replace("_", " ").split()).casefold()
    prefix, separator, rest = name.partition(":")
    if separator and prefix.strip() in {"template", "modèle"}:
        return rest.strip()
    return name


def field(template, name):
    return str(template.get(name).value).strip() if template.has(name) else ""


def citation_identifiers(template, citation_names, doi_field, pmid_field):
    name = normalize_template_name(template.name)
    if name in {normalize_template_name(doi_field), "doi-inline"}:
        doi, pmid = field(template, "1"), ""
    elif name == normalize_template_name(pmid_field):
        doi, pmid = "", field(template, "1")
    elif name in citation_names:
        doi, pmid = field(template, doi_field), field(template, pmid_field)
    else:
        return "", ""
    return fold_doi(doi) if is_doi(doi) else "", pmid if is_pmid(pmid) else ""


NOTICE_PRIORITY = {"Retraction": 3, "Expression of concern": 2, "Correction": 1}


def select_notice(records):
    # Until notice dates are available, leave mixed reinstatement histories alone.
    if any(record.retraction_nature == "Reinstatement" for record in records):
        return None
    return max(
        (record for record in records if record.retraction_nature in NOTICE_PRIORITY),
        key=lambda record: NOTICE_PRIORITY[record.retraction_nature],
        default=None,
    )


def process_item(record, template_map, field_map):
    keys = {
        "Retraction": ("retraction", "Retracted"),
        "Expression of concern": ("eoc", "Expression of Concern"),
        "Correction": ("erratum", "Erratum"),
    }
    if record.retraction_nature not in keys:
        return None
    key, default = keys[record.retraction_nature]
    legacy_keys = {"retraction": ("retracted", "Retracted"), "eoc": ("expression of concern",)}
    localized_name = template_map.get(key)
    if not localized_name:
        localized_name = next(
            (template_map[old] for old in legacy_keys.get(key, ()) if template_map.get(old)),
            default,
        )
    tag = Template(localized_name)
    doi = record.retraction_doi.strip()
    pmid = record.retraction_pubmed.strip()
    if is_doi(doi):
        tag.add(field_map.get("doi", "doi"), doi)
    if is_pmid(pmid):
        tag.add(field_map.get("pmid", "pmid"), pmid)
    for index, url in enumerate(filter(None, (url.strip() for url in record.url.split(";"))), 1):
        tag.add(str(index), url + " ''Retraction Watch''", showkey=True)
    return tag


def comparable_link(value):
    return re.sub(r"^https?://", "", " ".join(str(value).split()), flags=re.IGNORECASE)


def update_notice(existing, generated, doi_field, pmid_field):
    if normalize_template_name(existing.name) != normalize_template_name(generated.name):
        old_name = str(existing.name)
        leading = old_name[: len(old_name) - len(old_name.lstrip())]
        trailing = old_name[len(old_name.rstrip()) :]
        existing.name = leading + str(generated.name) + trailing

    for name in (doi_field, pmid_field):
        if not generated.has(name):
            continue
        old_value, new_value = field(existing, name), field(generated, name)
        if name == doi_field:
            old_value, new_value = fold_doi(old_value), fold_doi(new_value)
        if old_value != new_value:
            existing.add(name, field(generated, name))

    existing_links = {
        comparable_link(param.value)
        for param in existing.params
        if str(param.name).strip().isdigit()
    }
    index = 1
    for param in generated.params:
        if not str(param.name).isdigit() or comparable_link(param.value) in existing_links:
            continue
        while existing.has(str(index)):
            index += 1
        existing.add(str(index), str(param.value), showkey=True)


def process_page(text, lookup, template_map, field_map, citation_templates):
    wikicode = mwparserfromhell.parse(text)
    doi_field = field_map.get("doi", "doi")
    pmid_field = field_map.get("pmid", "pmid")
    citation_names = {normalize_template_name(name) for name in citation_templates}
    tag_names = {normalize_template_name(name) for name in template_map.values()}
    tag_names.update({"retracted", "expression of concern", "erratum"})
    containers = [wikicode]
    containers.extend(
        tag.contents for tag in wikicode.ifilter_tags() if tag.contents is not None
    )
    changes = []
    records_by_id = {}

    for container in containers:
        # Capture original indices. Inserting from right to left keeps earlier
        # indices valid and distinguishes identical templates in different refs.
        original_nodes = list(container.nodes)
        for index in range(len(original_nodes) - 1, -1, -1):
            citation = original_nodes[index]
            if not isinstance(citation, Template) or "cochrane" in str(citation).lower():
                continue
            doi, pmid = citation_identifiers(citation, citation_names, doi_field, pmid_field)
            identifier = doi or pmid
            if not identifier:
                continue
            if identifier not in records_by_id:
                records_by_id[identifier] = lookup(identifier)
            records = records_by_id[identifier]
            if doi and pmid and not records:
                if pmid not in records_by_id:
                    records_by_id[pmid] = lookup(pmid)
                # A valid, conflicting DOI is a different paper, even if its PMID
                # was copied into this citation.
                records = [
                    record for record in records_by_id[pmid]
                    if not is_doi(record.original_doi) or fold_doi(record.original_doi) == doi
                ]
            record = select_notice(records)
            if record is None:
                continue

            next_index = index + 1
            while next_index < len(original_nodes):
                node = original_nodes[next_index]
                if isinstance(node, Comment) or isinstance(node, Text) and not str(node).strip():
                    next_index += 1
                    continue
                break
            existing = original_nodes[next_index] if next_index < len(original_nodes) else None
            generated = process_item(record, template_map, field_map)
            if isinstance(existing, Template) and normalize_template_name(existing.name) in tag_names:
                before = str(existing)
                update_notice(existing, generated, doi_field, pmid_field)
                if before != str(existing):
                    changes.append(PageChange(record, "update"))
            else:
                container.insert(index + 1, generated)
                changes.append(PageChange(record, "add"))

    return str(wikicode), changes
