"""Training pipeline DAG.

- airq_training: triggered by hand, trains v1, registers it and moves the
  `champion` alias to the new version.

The `airq` package is uploaded beside this file.
"""

from airflow.sdk import dag, task


@dag(schedule=None, tags=["airq"])
def airq_training():
    @task
    def train():
        from airq.train import train

        return train()

    train()


airq_training()
