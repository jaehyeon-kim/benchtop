from sales.dashboard.metrics import metric_cards, metrics, revenue_charts

RECORDS = [
    {
        "order_id": "o1",
        "item_id": "i1",
        "country": "France",
        "traffic_source": "Search",
        "sale_price": 10.04,
    },
    {
        "order_id": "o1",
        "item_id": "i2",
        "country": "Brasil",
        "traffic_source": "Email",
        "sale_price": 20.0,
    },
    {
        "order_id": "o2",
        "item_id": "i3",
        "country": "France",
        "traffic_source": "Search",
        "sale_price": 15.0,
    },
]


def test_metrics_count_orders_items_and_sales():
    """Verify the three metrics: distinct orders, distinct items and total sales."""
    assert metrics(RECORDS) == {
        "num_orders": 2,
        "num_order_items": 3,
        "total_sales": 45,
    }


def test_cards_show_the_change_since_the_last_update():
    """Verify that each card's delta is the difference from the previous metrics."""
    cards = metric_cards(
        {"num_orders": 5, "num_order_items": 8, "total_sales": 100},
        {"num_orders": 3, "num_order_items": 8, "total_sales": 120},
    )
    assert [c["delta"] for c in cards] == [2, 0, -20] and cards[2]["value"] == "$ 100"


def test_revenue_charts_put_the_largest_first():
    """Verify that revenue is grouped by country and by source, largest first."""
    country, source = revenue_charts(RECORDS)
    assert country["xAxis"]["data"] == ["France", "Brasil"]
    assert country["series"][0]["data"] == [25, 20]
    assert source["title"]["text"] == "Revenue by Traffic Source"
