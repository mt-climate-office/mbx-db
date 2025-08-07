from sqlalchemy import URL, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.asyncio import AsyncEngine


def make_connection_string(
    username: str,
    password: str,
    host: str,
    database: str,
    port: int=5432,
) -> URL:
    return URL.create(
        "postgresql+asyncpg",
        username=username,
        password=password,
        host=host,
        database=database,
        port=port
    )


async def create_data_schema(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS data"))
        await conn.commit()


async def create_network_schema(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS network"))
        await conn.commit()
