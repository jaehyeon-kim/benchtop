import numpy as np
import pytest
from faker import Faker

from ecommerce.simulation.catalogue import products
from ecommerce.simulation.shop import advance, moved, new_order, new_user, pages

NOW, LATER = "2026-09-01T00:00:00+00:00", "2026-09-01T01:00:00+00:00"


@pytest.fixture
def order():
    rng, fake = np.random.default_rng(1), Faker()
    fake.seed_instance(1)
    return new_order(rng, products(), new_user(fake, rng, NOW), NOW)


def test_a_new_order_has_one_to_four_items_in_processing(order):
    """Verify that a new order has one to four items, all in Processing and linked to it."""
    head, items = order
    assert head.status == "Processing" and 1 <= head.num_of_items == len(items) <= 4
    assert {i.order_id for i in items} == {head.id}
    assert {i.status for i in items} == {"Processing"}


def test_an_order_moves_through_shipping_delivery_and_return(order):
    """Verify that each allowed move sets the status and its time column on the order and its items."""
    head, items = order
    for status in ("Shipped", "Delivered", "Returned"):
        head, items = advance(head, items, status, LATER)
        assert head.status == status and getattr(head, f"{status.lower()}_at") == LATER
        assert {i.status for i in items} == {status}


@pytest.mark.parametrize(
    "path",
    [["Delivered"], ["Returned"], ["Shipped", "Cancelled"], ["Cancelled", "Shipped"]],
)
def test_an_order_cannot_skip_or_reverse_a_status(order, path):
    """Verify that delivery needs shipping, a return needs delivery, and a shipped or cancelled order stays so."""
    head, items = order
    with pytest.raises(ValueError):
        for status in path:
            head, items = advance(head, items, status, LATER)


def test_a_move_changes_the_address_and_keeps_the_user():
    """Verify that a moved user keeps the id and gets a new street address and update time."""
    rng, fake = np.random.default_rng(2), Faker()
    fake.seed_instance(2)
    user = new_user(fake, rng, NOW)
    after = moved(user, fake, rng, LATER)
    assert after.id == user.id and after.updated_at == LATER
    assert after.street_address != user.street_address


def test_a_buyer_ends_the_visit_at_the_cart_and_purchase():
    """Verify that a visit starts at home and a buyer's visit ends with the cart and purchase pages."""
    rng = np.random.default_rng(3)
    visit = pages(rng, products(), buys=True)
    assert visit[0] == ("home", "/home") and [p for p, _ in visit[-2:]] == [
        "cart",
        "purchase",
    ]
    assert all(p != "purchase" for p, _ in pages(rng, products(), buys=False))
