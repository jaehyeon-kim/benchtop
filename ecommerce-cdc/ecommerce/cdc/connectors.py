"""Deploys the Debezium source and S3 sink connectors from the JSON files beside this module.

Run: python -m ecommerce.cdc.connectors
"""

import json
from pathlib import Path

from ecommerce.core.config import CONNECTORS
from ecommerce.stores import connect

_FOLDER = Path(__file__).parent


def deploy() -> None:
    """Creates both connectors from their files, or updates them if they exist."""
    for name, file in CONNECTORS.items():
        connect.put(name, json.loads((_FOLDER / file).read_text()))


if __name__ == "__main__":
    deploy()
    print(f"Deployed {', '.join(CONNECTORS)}")
