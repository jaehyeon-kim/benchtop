import csv
from collections import Counter
from pathlib import Path

FOLDER = Path(__file__).parents[2] / "ecommerce" / "simulation"


def _read(name: str) -> list[dict]:
    """Returns the rows of one of the blueprint's CSV files."""
    with open(FOLDER / name, newline="") as file:
        return list(csv.DictReader(file))


def test_catalogue_has_one_product_per_brand_category_and_department():
    """Verify that there are 260 products: 5 brands, 26 categories and 2 departments."""
    rows = _read("products.csv")
    brands = {p["brand"] for p in rows}
    categories = {p["category"] for p in rows}
    assert (len(brands), len(categories), len(rows)) == (5, 26, 260)
    assert len({(p["brand"], p["category"], p["department"]) for p in rows}) == 260
    assert len({p["id"] for p in rows}) == len({p["sku"] for p in rows}) == 260


def test_products_cost_less_than_their_price_and_ship_from_known_centres():
    """Verify that each cost is 39% to 59% of the price, and each product's distribution centre exists."""
    centres = {c["id"] for c in _read("dist_centers.csv")}
    assert len(centres) == 10
    for p in _read("products.csv"):
        price, cost = float(p["retail_price"]), float(p["cost"])
        assert 0.38 * price <= cost <= 0.6 * price
        assert p["distribution_center_id"] in centres


def test_cities_are_ten_in_each_of_six_countries():
    """Verify that there are 60 cities, 10 in each of 6 countries, all with a population."""
    cities = _read("cities.csv")
    assert len(cities) == 60
    assert set(Counter(c["country"] for c in cities).values()) == {10}
    assert all(int(c["population"]) > 0 for c in cities)
