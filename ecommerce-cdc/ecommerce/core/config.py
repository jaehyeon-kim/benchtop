"""Settings: the odctl service addresses, this project's names, and the simulation's parameters.

The addresses are as seen from the host, on 127.0.0.1 rather than localhost, because with
IPv6 enabled in Docker some services reset connections to the IPv6 address of localhost.
"""

# odctl's PostgreSQL creates the `cdc` schema, puts it on the search path and publishes
# it as `cdc_pub`, which the Debezium connector reads.
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"
SCHEMA = "cdc"
REPLICATION_SLOT = "ecommerce_cdc"

KAFKA = "127.0.0.1:9092"
SCHEMA_REGISTRY = (
    "http://127.0.0.1:8081"  # Karapace, which Connect reaches as karapace:8081
)
TOPIC_PREFIX = "ecommerce"  # Debezium names each topic ecommerce.cdc.<table>
CONTROL_TOPIC = "ecommerce-control"  # parameter changes the running simulation reads

CONNECT_URL = "http://127.0.0.1:8083"
CONNECTORS = {"ecommerce-cdc-source": "source.json", "ecommerce-cdc-s3": "s3-sink.json"}

S3 = {
    "endpoint_url": "http://127.0.0.1:8333",
    "aws_access_key_id": "user",
    "aws_secret_access_key": "password",
    "region_name": "us-east-1",
}
BUCKET = "odctl-dev"
S3_PREFIX = "ecommerce-cdc/"  # the sink's files, one folder per topic

# The parameters that `ecommerce.simulation.control` can change while the simulation
# runs. Their values are in `ecommerce/simulation/shop.yaml`, and each is in desflow's
# registry under `ecommerce.`.
SIM_ID = "ecommerce"
ARRIVALS = ("visitor", "move")  # mean arrivals per second
SERVICES = ("page_view", "patience", "pick", "transit", "return_after")  # mean seconds
CHANCES = ("returning", "buy", "return")  # shares of visitors or orders
