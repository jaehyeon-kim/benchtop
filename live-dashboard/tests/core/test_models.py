import pytest
from pydantic import ValidationError

from sales.core.models import OrderItem, User

NOW = "2026-09-30T00:00:00+00:00"


def test_rows_out_of_bounds_are_refused():
    """Verify that an age outside 12 to 70 and a price of 0 are refused."""
    with pytest.raises(ValidationError):
        User(
            id="u",
            age=90,
            gender="M",
            country="France",
            traffic_source="Search",
            created_at=NOW,
        )
    with pytest.raises(ValidationError):
        OrderItem(
            id="i",
            order_id="o",
            user_id="u",
            product_id=1,
            status="Shipped",
            sale_price=0,
            created_at=NOW,
        )
