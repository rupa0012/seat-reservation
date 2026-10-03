"""All settings come from environment variables so the same image runs locally and on the host."""
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://seats:seats@localhost:5432/seats")

# Static admin token for POST /shows. Override in production.
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-secret")

# Connection pool: the DB, not the web server, is the concurrency limit.
# Thousands of requests queue in the app; only POOL_MAX talk to Postgres at once.
POOL_MIN = int(os.getenv("POOL_MIN", "5"))
POOL_MAX = int(os.getenv("POOL_MAX", "20"))
POOL_TIMEOUT = float(os.getenv("POOL_TIMEOUT", "30"))        # seconds a request may wait for a connection
STATEMENT_TIMEOUT_MS = int(os.getenv("STATEMENT_TIMEOUT_MS", "15000"))

DEFAULT_PER_USER_LIMIT = 4
MAX_SEATS_PER_REQUEST = 10
MAX_SEATS_PER_SHOW = 50_000
MAX_SHOWS_IN_METRICS = 50

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
