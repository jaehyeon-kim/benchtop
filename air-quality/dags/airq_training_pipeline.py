"""Training pipeline DAG.

- airq_training: triggered by hand, trains v1 and v2 and registers both. The new
  version of the champion's feature set becomes `champion`, the other `challenger`.
  It marks the models updated (an Airflow asset), which starts airq_inference, so
  the new versions have a forecast straight away.

The `airq` package is uploaded beside this file.
"""

from airflow.sdk import Asset, dag, task

from airq.config import MODELS_ASSET

_MODELS = Asset(MODELS_ASSET)


@dag(schedule=None, tags=["airq"])
def airq_training():
    @task(outlets=[_MODELS])
    def train():
        from airq.train import train

        return train()

    train()


airq_training()
