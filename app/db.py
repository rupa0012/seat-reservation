"""Postgres connection pool + schema bootstrap."""
import logging
from pathlib import Path

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from . import config

log = logging.getLogger("app.db")

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

# Module-level pool; opened/closed by the FastAPI lifespan in main.py.
# autocommit=True: we open explicit `conn.transaction()` blocks, so BEGIN/COMMIT/ROLLBACK
# (and SAVEPOINT for nested blocks) are visible in the code instead of implicit.
pool = AsyncConnectionPool(
    conninfo=config.DATABASE_URL,
    min_size=config.POOL_MIN,
    max_size=config.POOL_MAX,
    timeout=config.POOL_TIMEOUT,
    open=False,
    kwargs={
        "autocommit": True,
        "row_factory": dict_row,
        "options": f"-c statement_timeout={config.STATEMENT_TIMEOUT_MS}",
    },
    check=AsyncConnectionPool.check_connection,
)


async def init_schema() -> None:
    """Create tables if missing. The advisory lock makes concurrent starts safe."""
    sql = SCHEMA_PATH.read_text()
    async with pool.connection(timeout=5) as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(727274)")
            await conn.execute(sql)
