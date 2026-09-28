import pytest
from pydantic import ValidationError

from airq.models import Observation
from tests.conftest import ORIGIN


def test_rows_out_of_range_are_refused():
    """Verify that a PM2.5 value below 0 or above 500 is refused."""
    now = ORIGIN
    with pytest.raises(ValidationError):
        Observation(
            location_id="station-1", measured_at=now, pm2_5=-1.0, ingested_at=now
        )
    with pytest.raises(ValidationError):
        Observation(
            location_id="station-1", measured_at=now, pm2_5=501.0, ingested_at=now
        )
