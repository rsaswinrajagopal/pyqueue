import os

REDIS_HOST = os.environ.get("PYQUEUE_REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("PYQUEUE_REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("PYQUEUE_REDIS_DB", "0"))

DEFAULT_WORKER_COUNT = 1

PROCESSING_PREFIX = "queue:processing:"
PENDING_KEY = "queue:pending"
DELAYED_KEY = "queue:delayed"
DEAD_LETTER_KEY = "queue:dead_letter"


# HEARTBEAT_TTL >= 2 * HEARTBEAT_INTERVAL
HEARTBEAT_INTERVAL = 5.0
HEARTBEAT_TTL = 10
SCAVENGER_INTERVAL = 12.0
TICK_INTERVAL = 1.0
