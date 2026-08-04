import sys
from datetime import date

import pytest

import main as main_module


class FakeConnection:
    def __init__(self, events):
        self.events = events

    def commit(self):
        self.events.append("commit")

    def rollback(self):
        self.events.append("rollback")

    def close(self):
        self.events.append("close")


def test_main_runs_extract_transform_and_load_in_order(
    monkeypatch,
    transformed_item,
):
    events = []
    conn = FakeConnection(events)

    monkeypatch.setattr(
        main_module,
        "create_database_connection",
        lambda: conn,
    )

    def fake_extract(conn, base_date, num_of_rows):
        events.append(("extract", base_date, num_of_rows))
        return "run-1", date(2023, 6, 1)

    def fake_transform(conn, run_id, base_date):
        events.append(("transform", run_id, base_date))
        return [transformed_item]

    def fake_load(conn, items):
        events.append(("load", len(items)))
        return len(items)

    monkeypatch.setattr(main_module, "extract_stock_prices", fake_extract)
    monkeypatch.setattr(main_module, "transform_stock_prices", fake_transform)
    monkeypatch.setattr(main_module, "load_stock_prices", fake_load)
    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--base-date", "20230601", "--num-of-rows", "100"],
    )

    main_module.main()

    assert events == [
        ("extract", "20230601", 100),
        "commit",
        ("transform", "run-1", date(2023, 6, 1)),
        ("load", 1),
        "commit",
        "close",
    ]


def test_main_rolls_back_and_closes_when_extract_fails(monkeypatch):
    events = []
    conn = FakeConnection(events)
    monkeypatch.setattr(
        main_module,
        "create_database_connection",
        lambda: conn,
    )

    def failing_extract(conn, base_date, num_of_rows):
        events.append("extract")
        raise RuntimeError("extract failed")

    monkeypatch.setattr(main_module, "extract_stock_prices", failing_extract)
    monkeypatch.setattr(sys, "argv", ["main.py", "--base-date", "20230601"])

    with pytest.raises(RuntimeError, match="extract failed"):
        main_module.main()

    assert events == ["extract", "rollback", "close"]


def test_main_keeps_raw_commit_and_rolls_back_staging_on_transform_failure(
    monkeypatch,
):
    events = []
    conn = FakeConnection(events)
    monkeypatch.setattr(
        main_module,
        "create_database_connection",
        lambda: conn,
    )

    def fake_extract(conn, base_date, num_of_rows):
        events.append("extract")
        return "run-2", date(2023, 6, 1)

    def failing_transform(conn, run_id, base_date):
        events.append("transform")
        raise ValueError("transform failed")

    monkeypatch.setattr(main_module, "extract_stock_prices", fake_extract)
    monkeypatch.setattr(main_module, "transform_stock_prices", failing_transform)
    monkeypatch.setattr(sys, "argv", ["main.py", "--base-date", "20230601"])

    with pytest.raises(ValueError, match="transform failed"):
        main_module.main()

    assert events == [
        "extract",
        "commit",
        "transform",
        "rollback",
        "close",
    ]


def test_main_rejects_non_positive_num_of_rows(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--base-date", "20230601", "--num-of-rows", "0"],
    )

    with pytest.raises(SystemExit) as error:
        main_module.main()

    assert error.value.code == 2
