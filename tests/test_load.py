from unittest.mock import MagicMock, Mock

import load.load as load_module


def test_load_stock_prices_returns_zero_for_empty_items():
    conn = Mock()

    result = load_module.load_stock_prices(conn, [])

    assert result == 0
    conn.cursor.assert_not_called()


def test_load_stock_prices_executes_batch_upsert(
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


def test_load_sql_uses_primary_key_upsert():
    normalized_sql = " ".join(load_module.INSERT_STOCK_PRICE_SQL.split())

    assert "ON CONFLICT (base_date, isin_code)" in normalized_sql
    assert "DO UPDATE SET" in normalized_sql
    assert "processed_at = NOW()" in normalized_sql
