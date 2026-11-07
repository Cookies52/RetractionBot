"""Validation shared by the Retraction Watch import and citation matching."""

import re


def is_doi(value):
    return bool(re.fullmatch(r'10\.[0-9]{4,}(?:\.[0-9]+)*/[^\s"<>\\]+', value))


def is_pmid(value):
    return bool(re.fullmatch(r"[1-9][0-9]*", value))
