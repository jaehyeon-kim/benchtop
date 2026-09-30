import pytest
from pydantic import ValidationError

from ecommerce.core.models import Product
from ecommerce.simulation.catalogue import products


def test_a_product_must_cost_more_than_nothing():
    """Verify that the row models refuse values outside their bounds."""
    product = products()[0]
    with pytest.raises(ValidationError):
        Product(**{**product.model_dump(), "retail_price": 0})
