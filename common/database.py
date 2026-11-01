from __future__ import annotations

import time
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from common.config import DATABASE_URL


@contextmanager
def connection():
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        yield conn


def wait_for_postgres(attempts: int = 30, delay: float = 2.0) -> None:
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            with connection() as conn:
                conn.execute("SELECT 1")
            return
        except Exception as exc:  # pragma: no cover - startup path
            last_error = exc
            time.sleep(delay)
    raise RuntimeError("PostgreSQL did not become ready") from last_error

