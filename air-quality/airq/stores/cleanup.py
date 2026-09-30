"""Removes everything this project has written, and keeps the services running.

It returns the stack to the state straight after `odctl up`, so the steps can be run
again from the start. It removes only this project's objects:

- Airflow: the DAG files in `s3://airflow/dags`, then the DAGs' records and run history.
- MLflow: the registered model, and the runs, logged models and artifact files of the
  experiment. The experiment itself stays, empty, because MLflow does not let a deleted
  experiment's name be used again.
- Feast: the project's entities and views.
- Iceberg: every table in the namespace, the namespace, and their files in SeaweedFS.

Airflow goes first, so no scheduled run writes to the tables while they are removed.
Each part is skipped when it is already gone, so running it twice is safe.

Run: python -m airq.stores.cleanup
"""

import logging

from airq.stores import airflow, feature_store, iceberg, model_registry, s3

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    client = s3.client()
    airflow.delete_dags(client)
    model_registry.delete_experiment(client)
    feature_store.delete_project()
    iceberg.drop_namespace(client)
