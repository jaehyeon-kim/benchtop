"""Turns the WebSocket's records into the dashboard's metric cards and chart options.

The Next.js dashboard does the same in `nextjs/src/lib/processing.ts`.
"""

from collections import defaultdict

LABELS = {
    "num_orders": "Number of Orders",
    "num_order_items": "Number of Order Items",
    "total_sales": "Total Sales",
}
CHARTS = {
    "country": "Country",
    "traffic_source": "Traffic Source",
}  # revenue grouped by


def metrics(records: list[dict]) -> dict[str, int]:
    """
    Counts the orders and order items, and adds up the sales.

    Args:
        records (list[dict]): The order items, from the WebSocket.

    Returns:
        dict[str, int]: `num_orders`, `num_order_items` and `total_sales`, in dollars.
    """
    return {
        "num_orders": len({r["order_id"] for r in records}),
        "num_order_items": len({r["item_id"] for r in records}),
        "total_sales": round(sum(r["sale_price"] for r in records)),
    }


def metric_cards(current: dict[str, int], previous: dict[str, int]) -> list[dict]:
    """
    Builds the metric cards, each with its change since the last update.

    Args:
        current (dict[str, int]): The metrics now.
        previous (dict[str, int]): The metrics at the last update.

    Returns:
        list[dict]: Each card's `label`, `value` and `delta`.
    """
    return [
        {
            "label": label,
            "value": f"$ {current[k]}" if k == "total_sales" else current[k],
            "delta": current[k] - previous[k],
        }
        for k, label in LABELS.items()
    ]


def revenue_charts(records: list[dict]) -> list[dict]:
    """
    Builds the ECharts options of revenue by country and by traffic source.

    Args:
        records (list[dict]): The order items, from the WebSocket.

    Returns:
        list[dict]: One bar chart's options per grouping, largest revenue first.
    """
    charts = []
    for column, title in CHARTS.items():
        revenue: dict[str, float] = defaultdict(float)
        for r in records:
            revenue[r[column]] += r["sale_price"]
        bars = sorted(revenue.items(), key=lambda kv: kv[1], reverse=True)
        charts.append(
            {
                "title": {"text": f"Revenue by {title}"},
                "grid": {"containLabel": True},  # room for the rotated axis labels
                "xAxis": {
                    "type": "category",
                    "data": [k for k, _ in bars],
                    "axisLabel": {"rotate": 75},
                },
                "yAxis": {"type": "value"},
                "series": [
                    {
                        "type": "bar",
                        "colorBy": "data",
                        "data": [round(v) for _, v in bars],
                    }
                ],
                "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
            }
        )
    return charts
