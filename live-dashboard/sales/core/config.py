"""Settings shared by the control command, the WebSocket server, the dashboards and the tests.

The addresses are odctl's services as seen from the host. Every table lives in the
`dashboard` schema, so nothing else in the database is touched.
"""

DSN = "postgresql://user:password@127.0.0.1:5432/odctl"
SCHEMA = "dashboard"
PARAMS_TABLE = f"{SCHEMA}.params"  # desflow's ingress reads live changes from here

API_HOST, API_PORT = "127.0.0.1", 8000
WS_URL = f"ws://{API_HOST}:{API_PORT}/ws"
LOOKBACK_MINUTES = 5  # the server sends the order items of this window
REFRESH_SECONDS = 5  # and sends them again this often
