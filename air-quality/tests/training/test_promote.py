from types import SimpleNamespace

import pytest
from mlflow.exceptions import MlflowException

from airq.training.promote import promote


class FakeClient:
    def __init__(self, aliases):
        self.aliases = aliases

    def get_model_version_by_alias(self, name, alias):
        if alias not in self.aliases:
            raise MlflowException("no such alias")
        return SimpleNamespace(version=self.aliases[alias])

    def set_registered_model_alias(self, name, alias, version):
        self.aliases[alias] = version


def test_promote_swaps_the_champion_and_the_challenger():
    """Verify that promotion makes the challenger the champion and the champion the challenger."""
    client = FakeClient({"champion": "1", "challenger": "3"})
    assert promote(client) == ("3", "1")
    assert client.aliases == {"champion": "3", "challenger": "1"}


def test_promote_without_a_challenger_stops():
    """Verify that promotion stops when there is no challenger."""
    with pytest.raises(SystemExit):
        promote(FakeClient({"champion": "1"}))
