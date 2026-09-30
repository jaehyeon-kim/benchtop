import json

import numpy as np

from recommender.stores.valkey import read_models


class _Valkey:
    """Answers `mget` from a dict."""

    def __init__(self, values):
        self.values = values

    def mget(self, keys):
        return [self.values.get(k) for k in keys]


def test_models_are_read_with_a_cold_start_for_missing_ones():
    """Verify that a stored model is decoded and a missing one is identity and zeros."""
    client = _Valkey({"linucb:1": json.dumps({"A_inv": [[2, 0], [0, 2]], "b": [1, 1]})})
    models = read_models(client, ["1", "2"], 2)
    assert np.array_equal(models["1"][0], 2 * np.eye(2)) and np.array_equal(
        models["1"][1], [1, 1]
    )
    assert np.array_equal(models["2"][0], np.eye(2)) and np.array_equal(
        models["2"][1], np.zeros(2)
    )
