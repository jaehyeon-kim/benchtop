from collections import Counter

from ecommerce.simulation.catalogue import (
    BRANDS,
    CITIES,
    DIST_CENTERS,
    PRICE_RANGES,
    products,
)


def test_catalogue_has_one_product_per_brand_category_and_department():
    """Verify that there are 260 products: 5 brands, 26 categories and 2 departments."""
    rows = products()
    assert len(rows) == len(BRANDS) * len(PRICE_RANGES) * 2 == 260
    assert len({(p.brand, p.category, p.department) for p in rows}) == 260
    assert len({p.id for p in rows}) == len({p.sku for p in rows}) == 260


def test_prices_stay_in_their_category_range_and_cost_less():
    """Verify that each price is within its category's range and each cost is below it."""
    for p in products():
        low, high = PRICE_RANGES[p.category]
        assert low <= p.retail_price <= high
        assert 0 < p.cost < p.retail_price


def test_catalogue_is_the_same_on_every_call():
    """Verify that the catalogue is generated from a fixed seed."""
    assert products() == products()


def test_products_ship_from_known_centres():
    """Verify that every product's distribution centre exists."""
    ids = {c.id for c in DIST_CENTERS}
    assert len(ids) == 10
    assert {p.distribution_center_id for p in products()} <= ids


def test_cities_are_ten_in_each_of_six_countries():
    """Verify that there are 60 cities, 10 in each of 6 countries, all with a population."""
    assert len(CITIES) == 60
    assert set(Counter(c.country for c in CITIES).values()) == {10}
    assert all(c.population > 0 for c in CITIES)
