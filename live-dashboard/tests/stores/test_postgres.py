from sales.stores import postgres


def test_recent_items_reads_the_schema_over_a_moving_window():
    """Verify that the query joins the project's tables and counts back from the time now."""
    sql = postgres.RECENT_ITEMS
    assert all(f"dashboard.{t}" in sql for t in ("order_items", "users", "products"))
    assert "clock_timestamp() - make_interval(mins => $1)" in sql


def test_only_rows_that_change_are_upserted():
    """Verify that orders and their items upsert on id, and the other tables insert only."""
    assert postgres.UPSERT_KEYS == {"orders": ["id"], "order_items": ["id"]}
    assert set(postgres.UPSERT_KEYS) < set(postgres.TABLES)
