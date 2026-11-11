from datetime import date

from src.RetractionBot.scan_queue import build_snapshot, open_scan_queue


def row(doi=b"10.1234/a", pmid=b"0", nature=b"Retraction", day=1):
    return doi, pmid, b"10.1234/n", b"", nature, b"", date(2026, 1, day)


def test_snapshot_ignores_placeholders_and_is_order_independent():
    original = row()
    second_notice = row(nature=b"Correction", day=2)
    pmid_only = row(doi=b"unavailable", pmid=b"456")
    invalid = row(doi=b"unavailable")
    rows = [original, pmid_only, second_notice, invalid, second_notice]
    snapshot = build_snapshot(rows)
    assert {(r.doi, r.pmid) for r in snapshot} == {("10.1234/a", ""), ("", "456")}
    assert set(snapshot) == set(build_snapshot(reversed(rows)))


def test_progress_persists_per_wiki_and_new_notice_goes_first(tmp_path):
    path = tmp_path / "queue.db"
    first = row()
    second = row(doi=b"10.1234/b", day=2)
    with open_scan_queue(path) as queue:
        queue.sync("en", build_snapshot([first, second]))
        queue.sync("fr", build_snapshot([first, second]))
        assert queue.work("en", 1) == [("10.1234/b", "")]
        queue.finished("en", "10.1234/b", "")
    with open_scan_queue(path) as queue:
        assert queue.work("en", 1) == [("10.1234/a", "")]
        assert queue.work("fr", 1) == [("10.1234/b", "")]
        queue.sync("en", build_snapshot([first, second, row(doi=b"10.1234/b", nature=b"Reinstatement", day=3)]))
        assert queue.work("en", 1) == [("10.1234/b", "")]
        queue.sync("en", build_snapshot([first]))
        assert queue.work("en", 10) == [("10.1234/a", "")]


def test_overlap_is_rejected_and_a_failed_item_can_retry(tmp_path):
    path = tmp_path / "queue.db"
    with open_scan_queue(path) as queue:
        with open_scan_queue(path) as overlap:
            assert overlap is None
        queue.sync("en", build_snapshot([row(), row(doi=b"10.1234/b", day=2)]))
        queue.started("en", "10.1234/b", "")
        assert queue.work("en", 2) == [("10.1234/a", ""), ("10.1234/b", "")]
