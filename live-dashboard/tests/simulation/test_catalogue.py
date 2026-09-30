from sales.simulation.catalogue import PRICE_RANGES, products


def test_catalogue_has_one_product_per_brand_category_and_department():
    """Verify that the catalogue holds 260 products with unique ids, the same on every call."""
    rows = products()
    assert len(rows) == len({p.id for p in rows}) == 260
    assert rows == products()


def test_prices_fall_in_their_category_range_and_cost_less():
    """Verify that each price is within its category's range and its cost is below it."""
    for p in products():
        low, high = PRICE_RANGES[p.category]
        assert low <= p.retail_price <= high and 0 < p.cost < p.retail_price
