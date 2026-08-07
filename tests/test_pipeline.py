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

    def fake_fetch_raw(conn, run_id, base_date):
        events.append(("fetch_raw", run_id, base_date))
        return [{"response_total_count": 1}]

    def fake_validate_raw(raw_responses):
        events.append(("validate_raw", len(raw_responses)))

    def fake_validate_transformed(items, expected_base_date):
        events.append(
            ("validate_transformed", len(items), expected_base_date)
        )

    def fake_validate_staging(conn, items):
        events.append(("validate_staging", len(items)))

    def fake_delete_mart(conn, base_date):
        events.append(("delete_mart", base_date))

    def fake_delete_staging(conn, base_date):
        events.append(("delete_staging", base_date))

    def fake_build_mart(conn, base_date):
        events.append(("build_mart", base_date))
        return ["mart-row"]

    def fake_validate_mart(rows, items, expected_base_date):
        events.append(
            ("validate_mart", len(rows), len(items), expected_base_date)
        )

    def fake_validate_published(conn, base_date, expected_row_count):
        events.append(("verify_published", base_date, expected_row_count))
        return {
            "staging_row_count": 1,
            "mart_row_count": 1,
            "key_mismatch_count": 0,
        }

    monkeypatch.setattr(main_module, "extract_stock_prices", fake_extract)
    monkeypatch.setattr(main_module, "fetch_raw_responses", fake_fetch_raw)
    monkeypatch.setattr(main_module, "validate_raw_batch", fake_validate_raw)
    monkeypatch.setattr(main_module, "transform_stock_prices", fake_transform)
    monkeypatch.setattr(
        main_module,
        "validate_transformed_items",
        fake_validate_transformed,
    )
    monkeypatch.setattr(main_module, "load_stock_prices", fake_load)
    monkeypatch.setattr(
        main_module,
        "delete_daily_stock_rankings_for_date",
        fake_delete_mart,
    )
    monkeypatch.setattr(
        main_module,
        "delete_stock_prices_for_date",
        fake_delete_staging,
    )
    monkeypatch.setattr(
        main_module,
        "validate_staging_load",
        fake_validate_staging,
    )
    monkeypatch.setattr(
        main_module,
        "build_daily_stock_rankings",
        fake_build_mart,
    )
    monkeypatch.setattr(
        main_module,
        "validate_mart_rankings",
        fake_validate_mart,
    )
    monkeypatch.setattr(
        main_module,
        "validate_published_snapshot",
        fake_validate_published,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--base-date", "20230601", "--num-of-rows", "100"],
    )

    main_module.main()

    assert events == [
        ("extract", "20230601", 100),
        ("fetch_raw", "run-1", date(2023, 6, 1)),
        ("validate_raw", 1),
        "commit",
        ("transform", "run-1", date(2023, 6, 1)),
        ("validate_transformed", 1, date(2023, 6, 1)),
        ("delete_mart", date(2023, 6, 1)),
        ("delete_staging", date(2023, 6, 1)),
        ("load", 1),
        ("validate_staging", 1),
        ("build_mart", date(2023, 6, 1)),
        ("validate_mart", 1, 1, date(2023, 6, 1)),
        "commit",
        ("verify_published", date(2023, 6, 1), 1),
        "rollback",
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

    def fake_fetch_raw(conn, run_id, base_date):
        events.append("fetch_raw")
        return [{"response_total_count": 1}]

    def fake_validate_raw(raw_responses):
        events.append("validate_raw")

    def failing_transform(conn, run_id, base_date):
        events.append("transform")
        raise ValueError("transform failed")

    monkeypatch.setattr(main_module, "extract_stock_prices", fake_extract)
    monkeypatch.setattr(main_module, "fetch_raw_responses", fake_fetch_raw)
    monkeypatch.setattr(main_module, "validate_raw_batch", fake_validate_raw)
    monkeypatch.setattr(main_module, "transform_stock_prices", failing_transform)
    monkeypatch.setattr(sys, "argv", ["main.py", "--base-date", "20230601"])

    with pytest.raises(ValueError, match="transform failed"):
        main_module.main()

    assert events == [
        "extract",
        "fetch_raw",
        "validate_raw",
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


def test_main_runs_inclusive_date_range(monkeypatch):
    events = []
    conn = FakeConnection(events)
    monkeypatch.setattr(
        main_module,
        "create_database_connection",
        lambda: conn,
    )

    def fake_run_pipeline_for_date(conn, base_date, num_of_rows):
        events.append(("run_date", base_date, num_of_rows))

    monkeypatch.setattr(
        main_module,
        "run_pipeline_for_date",
        fake_run_pipeline_for_date,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "--base-date",
            "20230601",
            "--end-date",
            "20230603",
            "--num-of-rows",
            "50",
        ],
    )

    main_module.main()

    assert events == [
        ("run_date", date(2023, 6, 1), 50),
        ("run_date", date(2023, 6, 2), 50),
        ("run_date", date(2023, 6, 3), 50),
        "close",
    ]


def test_main_rejects_end_date_before_base_date(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "create_database_connection",
        lambda: pytest.fail("유효하지 않은 날짜 범위에서는 DB에 연결하면 안 됩니다."),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "--base-date",
            "20230603",
            "--end-date",
            "20230601",
        ],
    )

    with pytest.raises(SystemExit) as error:
        main_module.main()

    assert error.value.code == 2


def test_main_rejects_invalid_base_date_format(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "create_database_connection",
        lambda: pytest.fail("유효하지 않은 날짜는 DB에 연결하면 안 됩니다."),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--base-date", "2023-06-01"],
    )

    with pytest.raises(SystemExit) as error:
        main_module.main()

    assert error.value.code == 2


def test_extract_latest_available_raw_skips_empty_dates(monkeypatch):
    requested_dates = []

    def fake_extract(conn, base_date, num_of_rows):
        requested_dates.append(base_date)
        item_count = 10 if base_date == date(2023, 6, 2) else 0
        return {
            "run_id": f"run-{base_date}",
            "base_date": base_date.isoformat(),
            "raw_page_count": 1,
            "raw_item_count": item_count,
        }

    monkeypatch.setattr(main_module, "extract_raw_for_date", fake_extract)

    result = main_module.extract_latest_available_raw(
        conn=object(),
        reference_date=date(2023, 6, 5),
        num_of_rows=100,
    )

    assert requested_dates == [
        date(2023, 6, 4),
        date(2023, 6, 3),
        date(2023, 6, 2),
    ]
    assert result["base_date"] == "2023-06-02"
    assert result["raw_item_count"] == 10


def test_main_rolls_back_raw_when_raw_quality_validation_fails(monkeypatch):
    events = []
    conn = FakeConnection(events)
    monkeypatch.setattr(
        main_module,
        "create_database_connection",
        lambda: conn,
    )
    monkeypatch.setattr(
        main_module,
        "extract_stock_prices",
        lambda conn, base_date, num_of_rows: (
            "run-quality-fail",
            date(2023, 6, 1),
        ),
    )
    monkeypatch.setattr(
        main_module,
        "fetch_raw_responses",
        lambda conn, run_id, base_date: ["invalid-raw"],
    )

    def failing_raw_validation(raw_responses):
        events.append("validate_raw")
        raise ValueError("raw quality failed")

    monkeypatch.setattr(
        main_module,
        "validate_raw_batch",
        failing_raw_validation,
    )
    monkeypatch.setattr(sys, "argv", ["main.py", "--base-date", "20230601"])

    with pytest.raises(ValueError, match="raw quality failed"):
        main_module.main()

    assert events == ["validate_raw", "rollback", "close"]


def test_main_rolls_back_staging_when_load_quality_validation_fails(
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
    monkeypatch.setattr(
        main_module,
        "extract_stock_prices",
        lambda conn, base_date, num_of_rows: (
            "run-staging-quality-fail",
            date(2023, 6, 1),
        ),
    )
    monkeypatch.setattr(
        main_module,
        "fetch_raw_responses",
        lambda conn, run_id, base_date: [{"response_total_count": 1}],
    )
    monkeypatch.setattr(main_module, "validate_raw_batch", lambda rows: None)
    monkeypatch.setattr(
        main_module,
        "transform_stock_prices",
        lambda conn, run_id, base_date: [transformed_item],
    )
    monkeypatch.setattr(
        main_module,
        "validate_transformed_items",
        lambda items, expected_base_date: None,
    )
    monkeypatch.setattr(
        main_module,
        "load_stock_prices",
        lambda conn, items: len(items),
    )
    monkeypatch.setattr(
        main_module,
        "delete_daily_stock_rankings_for_date",
        lambda conn, base_date: events.append("delete_mart"),
    )
    monkeypatch.setattr(
        main_module,
        "delete_stock_prices_for_date",
        lambda conn, base_date: events.append("delete_staging"),
    )

    def failing_staging_validation(conn, items):
        events.append("validate_staging")
        raise ValueError("staging quality failed")

    monkeypatch.setattr(
        main_module,
        "validate_staging_load",
        failing_staging_validation,
    )
    monkeypatch.setattr(sys, "argv", ["main.py", "--base-date", "20230601"])

    with pytest.raises(ValueError, match="staging quality failed"):
        main_module.main()

    assert events == [
        "commit",
        "delete_mart",
        "delete_staging",
        "validate_staging",
        "rollback",
        "close",
    ]


def test_main_rolls_back_staging_and_mart_when_mart_validation_fails(
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
    monkeypatch.setattr(
        main_module,
        "extract_stock_prices",
        lambda conn, base_date, num_of_rows: (
            "run-mart-fail",
            date(2023, 6, 1),
        ),
    )
    monkeypatch.setattr(
        main_module,
        "fetch_raw_responses",
        lambda conn, run_id, base_date: [{"response_total_count": 1}],
    )
    monkeypatch.setattr(main_module, "validate_raw_batch", lambda rows: None)
    monkeypatch.setattr(
        main_module,
        "transform_stock_prices",
        lambda conn, run_id, base_date: [transformed_item],
    )
    monkeypatch.setattr(
        main_module,
        "validate_transformed_items",
        lambda items, expected_base_date: None,
    )
    monkeypatch.setattr(
        main_module,
        "load_stock_prices",
        lambda conn, items: len(items),
    )
    monkeypatch.setattr(
        main_module,
        "delete_daily_stock_rankings_for_date",
        lambda conn, base_date: events.append("delete_mart"),
    )
    monkeypatch.setattr(
        main_module,
        "delete_stock_prices_for_date",
        lambda conn, base_date: events.append("delete_staging"),
    )
    monkeypatch.setattr(
        main_module,
        "validate_staging_load",
        lambda conn, items: None,
    )
    monkeypatch.setattr(
        main_module,
        "build_daily_stock_rankings",
        lambda conn, base_date: ["mart-row"],
    )

    def failing_mart_validation(rows, items, expected_base_date):
        events.append("validate_mart")
        raise ValueError("mart quality failed")

    monkeypatch.setattr(
        main_module,
        "validate_mart_rankings",
        failing_mart_validation,
    )
    monkeypatch.setattr(sys, "argv", ["main.py", "--base-date", "20230601"])

    with pytest.raises(ValueError, match="mart quality failed"):
        main_module.main()

    assert events == [
        "commit",
        "delete_mart",
        "delete_staging",
        "validate_mart",
        "rollback",
        "close",
    ]


def test_main_preserves_existing_snapshot_when_api_returns_zero_items(monkeypatch):
    events = []
    conn = FakeConnection(events)
    monkeypatch.setattr(
        main_module,
        "create_database_connection",
        lambda: conn,
    )
    monkeypatch.setattr(
        main_module,
        "extract_stock_prices",
        lambda conn, base_date, num_of_rows: (
            "run-empty",
            date(2023, 6, 1),
        ),
    )
    monkeypatch.setattr(
        main_module,
        "fetch_raw_responses",
        lambda conn, run_id, base_date: [{"response_total_count": 0}],
    )
    monkeypatch.setattr(main_module, "validate_raw_batch", lambda rows: None)
    monkeypatch.setattr(
        main_module,
        "transform_stock_prices",
        lambda conn, run_id, base_date: [],
    )
    monkeypatch.setattr(
        main_module,
        "delete_daily_stock_rankings_for_date",
        lambda conn, base_date: events.append("delete_mart"),
    )
    monkeypatch.setattr(
        main_module,
        "delete_stock_prices_for_date",
        lambda conn, base_date: events.append("delete_staging"),
    )
    monkeypatch.setattr(sys, "argv", ["main.py", "--base-date", "20230601"])

    main_module.main()

    assert events == ["commit", "rollback", "close"]
