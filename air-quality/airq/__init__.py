# Set the service address defaults before any module imports PyIceberg or MLflow,
# which read their environment variables when first imported or first used.
import airq.config  # noqa: F401
