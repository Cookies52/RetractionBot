import pytest

from src.RetractionBot.identifiers import is_doi, is_pmid


@pytest.mark.parametrize("placeholder", ["", "0", "unavailable", "Unavailable", "１２３"])
def test_placeholders_are_neither_doi_nor_pmid(placeholder):
    assert not is_doi(placeholder)
    assert not is_pmid(placeholder)


def test_accepts_full_doi_and_positive_pmid():
    assert is_doi("10.1016/S0140-6736(20)31180-6")
    assert is_pmid("123")
    assert not is_doi('10.1234/a" OR "b')
