"""The Kafka Connect store: this project's connectors, through Connect's REST API."""

import json
import time
import urllib.error
import urllib.request

from ecommerce.core.config import CONNECT_URL, CONNECTORS


def _request(method: str, path: str, body: dict | None = None) -> int:
    """Sends one request to the Connect REST API and returns the status code."""
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        f"{CONNECT_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request) as response:
            return response.status
    except urllib.error.HTTPError as e:
        return e.code


def put(name: str, config: dict) -> None:
    """
    Creates a connector, or updates it if it exists.

    Args:
        name (str): The connector's name.
        config (dict): Its settings.
    """
    _request("PUT", f"/connectors/{name}/config", config)


def delete() -> list[str]:
    """
    Deletes this project's connectors and their stored offsets, so a new source connector
    takes a fresh snapshot.

    Returns:
        list[str]: The connectors deleted.
    """
    deleted = []
    for name in CONNECTORS:
        if _request("PUT", f"/connectors/{name}/stop") == 404:
            continue
        time.sleep(3)  # offsets can be deleted only once the connector has stopped
        _request("DELETE", f"/connectors/{name}/offsets")
        _request("DELETE", f"/connectors/{name}")
        deleted.append(name)
    return deleted
