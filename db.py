"""
Подключение к базе данных Postgres.

Railway сам создаёт переменную окружения DATABASE_URL, когда вы
добавляете в проект плагин PostgreSQL ("New" -> "Database" -> "Add
PostgreSQL"). Никаких паролей вручную вводить не нужно — Railway всё
подставит сам.

Если DATABASE_URL не найдена (Postgres ещё не подключён) — is_configured()
вернёт False, и остальные модули (storage.py, confirmed.py, watchlist.py)
автоматически будут использовать локальные JSON-файлы вместо базы, чтобы
бот продолжал работать в любом случае.
"""

import os
import asyncio
import psycopg2
from psycopg2 import pool as pg_pool

DATABASE_URL = os.environ.get("DATABASE_URL", "")

_pool = None


def is_configured() -> bool:
    return bool(DATABASE_URL)


def _get_pool():
    global _pool
    if _pool is None and DATABASE_URL:
        _pool = pg_pool.SimpleConnectionPool(1, 5, DATABASE_URL)
    return _pool


def _run_sync(query, params=None, fetch=False, fetchone=False):
    pool = _get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            result = None
            if fetchone:
                result = cur.fetchone()
            elif fetch:
                result = cur.fetchall()
            conn.commit()
            return result
    except Exception as e:
        conn.rollback()
        print(f"[db] Ошибка запроса к базе: {e}")
        raise
    finally:
        pool.putconn(conn)


async def run(query, params=None, fetch=False, fetchone=False):
    """Асинхронная обёртка — выполняет запрос в отдельном потоке,
    чтобы не блокировать остальную работу бота."""
    return await asyncio.to_thread(_run_sync, query, params, fetch, fetchone)


async def init_schema():
    """Создаёт таблицы, если их ещё нет. Безопасно вызывать при каждом
    запуске (CREATE TABLE IF NOT EXISTS)."""
    if not is_configured():
        print("[db] DATABASE_URL не задана — работаю на локальных JSON-файлах.")
        return

    await run("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id SERIAL PRIMARY KEY,
            token_address TEXT NOT NULL,
            ts DOUBLE PRECISION NOT NULL,
            liquidity_usd DOUBLE PRECISION,
            volume_24h_usd DOUBLE PRECISION,
            price_usd DOUBLE PRECISION
        )
    """)
    await run("CREATE INDEX IF NOT EXISTS idx_snapshots_addr_ts ON snapshots(token_address, ts)")

    await run("""
        CREATE TABLE IF NOT EXISTS confirmed_tokens (
            address TEXT PRIMARY KEY,
            name TEXT,
            source TEXT,
            added_ts DOUBLE PRECISION,
            last_score INTEGER,
            last_verdict TEXT,
            next_check_ts DOUBLE PRECISION
        )
    """)

    await run("""
        CREATE TABLE IF NOT EXISTS watchlist (
            address TEXT PRIMARY KEY,
            name TEXT,
            manual_flags JSONB
        )
    """)

    print("[db] Postgres подключена, таблицы проверены/созданы.")
