"""Sends the recent order items to the dashboards over a WebSocket.

Every `REFRESH_SECONDS`, `/ws` sends the order items of the last `LOOKBACK_MINUTES`,
with their users and products, as a JSON list of records.

Run: uvicorn sales.api.server:app --host 127.0.0.1 --port 8000
"""

import asyncio
import logging

import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from sales.core.config import DSN, LOOKBACK_MINUTES, REFRESH_SECONDS
from sales.stores import postgres

logger = logging.getLogger("uvicorn.error")
app = FastAPI()


@app.websocket("/ws")
async def stream(websocket: WebSocket) -> None:
    """
    Sends the recent order items every `REFRESH_SECONDS`, until the client leaves.

    Args:
        websocket (WebSocket): The client.
    """
    await websocket.accept()
    conn = await asyncpg.connect(DSN)
    try:
        while True:
            records = await postgres.recent_items(conn, LOOKBACK_MINUTES)
            logger.info("Sending %d records", len(records))
            await websocket.send_json(records)
            await asyncio.sleep(REFRESH_SECONDS)
    except WebSocketDisconnect:
        logger.info("Client disconnected")
    finally:
        await conn.close()
