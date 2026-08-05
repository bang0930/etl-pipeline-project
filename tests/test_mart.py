import pytest

import mart.mart as mart_module


class FakeCursor:
    def __init__(self, rowcount=0, rows=None):
        self.rowcount = rowcount
        self.rows = rows or []
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, params):
        self.executed = (query, params)

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor

    def cursor(self, cursor_factory=None):
        return self.cursor_instance


def test_load_daily_stock_rankings_executes_ranked_upsert():
    cursor = FakeCursor(rowcount=2)
    conn = FakeConnection(cursor)

    loaded_count = mart_module.load_daily_stock_rankings(
        conn,
        "2023-06-01",
    )

    query, params = cursor.executed
    assert loaded_count == 2
    assert "DENSE_RANK()" in query
    assert "NULLS LAST" in query
    assert "ON CONFLICT (base_date, isin_code)" in query
    assert params == ("2023-06-01",)


def test_load_daily_stock_rankings_rejects_missing_staging_rows():
    conn = FakeConnection(FakeCursor(rowcount=0))

    with pytest.raises(ValueError, match="No Staging stock prices found"):
        mart_module.load_daily_stock_rankings(conn, "2023-06-01")


def test_build_daily_stock_rankings_rejects_count_mismatch(monkeypatch):
    monkeypatch.setattr(
        mart_module,
        "load_daily_stock_rankings",
        lambda conn, base_date: 2,
    )
    monkeypatch.setattr(
        mart_module,
        "fetch_daily_stock_rankings",
        lambda conn, base_date: [{"isin_code": "KR700088K015"}],
    )

    with pytest.raises(ValueError, match="Mart row count mismatch"):
        mart_module.build_daily_stock_rankings(object(), "2023-06-01")
