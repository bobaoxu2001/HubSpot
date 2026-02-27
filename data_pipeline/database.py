"""
Database connection pool and helper utilities.

Uses psycopg2 with a connection-pool pattern suitable for both synchronous
pipeline execution and concurrent API workflows.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Sequence

import psycopg2
import psycopg2.extras
import psycopg2.pool

from config.settings import get_settings

logger = logging.getLogger(__name__)

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Lazily initialise a threaded connection pool."""
    global _pool
    if _pool is None or _pool.closed:
        cfg = get_settings().db
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            host=cfg.host,
            port=cfg.port,
            dbname=cfg.dbname,
            user=cfg.user,
            password=cfg.password,
        )
        logger.info("Database connection pool initialised (%s:%s/%s)", cfg.host, cfg.port, cfg.dbname)
    return _pool


@contextmanager
def get_connection():
    """Yield a connection from the pool; auto-return on exit."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


@contextmanager
def get_cursor(cursor_factory=None):
    """Yield a cursor (default: RealDictCursor) within a managed connection."""
    factory = cursor_factory or psycopg2.extras.RealDictCursor
    with get_connection() as conn:
        with conn.cursor(cursor_factory=factory) as cur:
            yield cur


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def execute(sql: str, params: Optional[Sequence] = None) -> None:
    """Execute a single statement (INSERT / UPDATE / DDL)."""
    with get_cursor() as cur:
        cur.execute(sql, params)


def execute_many(sql: str, params_seq: Sequence[Sequence]) -> None:
    """Execute a parameterised statement for many rows."""
    with get_cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, params_seq, page_size=100)


def fetch_all(sql: str, params: Optional[Sequence] = None) -> List[Dict[str, Any]]:
    """Run a SELECT and return all rows as dicts."""
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_one(sql: str, params: Optional[Sequence] = None) -> Optional[Dict[str, Any]]:
    """Run a SELECT and return the first row or None."""
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def init_schema(schema_path: str = "warehouse/schema.sql") -> None:
    """Apply the DDL schema file to the target database."""
    with open(schema_path) as fh:
        ddl = fh.read()
    with get_cursor() as cur:
        cur.execute(ddl)
    logger.info("Database schema applied from %s", schema_path)


def close_pool() -> None:
    """Shut down the connection pool."""
    global _pool
    if _pool and not _pool.closed:
        _pool.closeall()
        logger.info("Database connection pool closed")
        _pool = None
