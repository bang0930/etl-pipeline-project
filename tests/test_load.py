from unittest.mock import MagicMock, Mock

import load.load as load_module


def test_load_stock_prices_returns_zero_for_empty_items():
    conn = Mock()

    result = load_module.load_stock_prices(conn, [])

    assert result == 0
    conn.cursor.assert_not_called()


def test_load_stock_prices_executes_batch_insert(
    monkeypatch,
    transformed_item,
):
    cursor = Mock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    conn = Mock()
    conn.cursor.return_value = cursor_context
    execute_values_mock = Mock()
    monkeypatch.setattr(
        load_module,
        "execute_values",
        execute_values_mock,
    )

    items = [transformed_item, {**transformed_item, "isin_code": "KR7005930003"}]
    result = load_module.load_stock_prices(conn, items)

    assert result == 2
    execute_values_mock.assert_called_once_with(
        cursor,
        load_module.INSERT_STOCK_PRICE_SQL,
        items,
        template=load_module.STOCK_PRICE_VALUES_TEMPLATE,
        page_size=1000,
    )


def test_load_sql_uses_plain_insert_after_snapshot_delete():
    normalized_sql = " ".join(load_module.INSERT_STOCK_PRICE_SQL.split())

    assert "INSERT INTO staging.stock_prices" in normalized_sql
    assert "ON CONFLICT" not in normalized_sql


def test_delete_stock_prices_for_date_executes_date_delete():
    cursor = Mock(rowcount=3)
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    conn = Mock()
    conn.cursor.return_value = cursor_context

    deleted_count = load_module.delete_stock_prices_for_date(
        conn,
        "2023-06-01",
    )

    assert deleted_count == 3
    cursor.execute.assert_called_once_with(
        load_module.DELETE_STOCK_PRICES_SQL,
        ("2023-06-01",),
    )
