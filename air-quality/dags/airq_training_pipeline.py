"""Defines the training pipeline DAG.

- airq_training: started by hand. It trains the model version named in its `version`
  parameter, v1 or v2, and registers it. The first version becomes `champion`. After
  that, a new version of the champion's feature set becomes the new `champion`, and the
  other feature set becomes `challenger`. It then marks the models asset as updated,
  which starts airq_inference, so the new version has a forecast straight away.

The `airq` package is uploaded next to this file.
"""

from airflow.sdk import Asset, Param, dag, task

from airq.core.config import MODELS_ASSET

_MODELS = Asset(MODELS_ASSET)


@dag(
    schedule=None,
    params={
        "version": Param("v1", enum=["v1", "v2"], description="model version to train"),
    },
    tags=["airq"],
)  # fmt: skip
def airq_training():
    @task(outlets=[_MODELS])
    def train(params=None):
        from airq.training.train import train

        return train(params["version"])

    train()


airq_training()
