from types import SimpleNamespace

import pytest
from mlflow.exceptions import MlflowException

from airq.stores import model_registry
from airq.stores.model_registry import promote, role


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


def _champion(monkeypatch, feature_set):
    class Client:
        def get_model_version_by_alias(self, name, alias):
            if feature_set is None:
                raise MlflowException("no champion")
            return SimpleNamespace(tags={"feature_set": feature_set})

    monkeypatch.setattr(model_registry.mlflow, "MlflowClient", Client)


def test_first_version_becomes_the_champion(monkeypatch):
    """Verify that the first version registered becomes the champion."""
    _champion(monkeypatch, None)
    assert role("v1") == "champion"


def test_other_feature_set_becomes_the_challenger(monkeypatch):
    """Verify that v2 becomes the challenger while v1 is the champion, and a new v1 stays champion."""
    _champion(monkeypatch, "v1")
    assert role("v2") == "challenger"
    assert role("v1") == "champion"


def test_after_promotion_v1_becomes_the_challenger(monkeypatch):
    """Verify that once v2 is the champion, a new v1 version becomes the challenger."""
    _champion(monkeypatch, "v2")
    assert role("v1") == "challenger"
