import numpy as np

from sales.simulation.catalogue import COUNTRIES, products
from sales.simulation.shop import basket, new_user, with_status

NOW = "2026-09-30T00:00:00+00:00"


def test_a_new_user_lives_in_one_of_the_countries():
    """Verify that a new user's fields fall in their bounds and country list."""
    user = new_user(np.random.default_rng(1), NOW)
    assert user.country in COUNTRIES and 12 <= user.age <= 70


def test_a_basket_holds_different_products_of_one_order():
    """Verify that every item belongs to the order, the products differ, and the count matches."""
    rng = np.random.default_rng(2)
    for _ in range(50):
        order, items = basket(rng, new_user(rng, NOW), products(), NOW)
        assert len(items) == order.num_of_item == len({i.product_id for i in items})
        assert {i.order_id for i in items} == {order.id}


def test_a_status_change_keeps_the_rows_ids():
    """Verify that a new status copies the order and items with the same ids."""
    rng = np.random.default_rng(3)
    order, items = basket(rng, new_user(rng, NOW), products(), NOW)
    shipped = with_status([order, *items], "Shipped")
    assert [r.id for r in shipped] == [order.id, *(i.id for i in items)]
    assert {r.status for r in shipped} == {"Shipped"} and order.status == "Processing"
