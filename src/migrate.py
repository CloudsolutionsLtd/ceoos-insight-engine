"""
Apply the insight-engine schema, then exit:  python -m src.migrate

Run as a Kubernetes Job before rollout with RUN_MIGRATIONS=false on the
Deployment. Idempotent; an advisory lock serialises concurrent runs.
"""
import asyncio
import sys

import asyncpg

from src.config import settings


async def main() -> int:
    import src.main as app_main

    app_main.db_pool = await asyncpg.create_pool(
        host=settings.postgres_host, port=settings.postgres_port, database=settings.postgres_db,
        user=settings.postgres_user, password=settings.postgres_password,
        ssl="require" if settings.postgres_ssl else None, min_size=1, max_size=1,
    )
    try:
        await app_main.ensure_database_tables()
        print("insight-engine schema up to date")
        return 0
    finally:
        await app_main.db_pool.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
