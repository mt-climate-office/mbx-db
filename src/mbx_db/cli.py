from mesonet_in_a_box.config import Config
from .models import Base
from .database import (
    make_connection_string,
    create_network_schema,
    create_data_schema,
)
import os
from dotenv import load_dotenv
import typer
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
import asyncio


def callback():
    "Functionality for initializing, migrating and managing the TimescaleDB."
    ...


app = typer.Typer(rich_markup_mode="rich", callback=callback)


async def async_init_db(conn):
    async_engine: AsyncEngine = create_async_engine(conn)
    await create_network_schema(async_engine)
    await create_data_schema(async_engine)
    async with async_engine.begin() as conn_:
        await conn_.run_sync(Base.metadata.drop_all)
        await conn_.run_sync(Base.metadata.create_all)


@app.command()
def init_db():
    """Create a configuration file that all other `mbx` commands will reference.
    Please run this command before running any other `mbx` commands.
    """
    CONFIG = Config.load(Config.file)

    if CONFIG.directory is None:
        raise ValueError(
            "CONFIG directory cannot be None. Please rerun `mesonet configure`."
        )

    if CONFIG.env_file:
        load_dotenv(CONFIG.env_file)

    pg_username = os.getenv("POSTGRES_USER")
    if pg_username is None:
        raise ValueError("POSTGRES_USER environment variable couldn't be found.")

    pg_hostname = os.getenv("POSTGRES_HOST")
    if pg_hostname is None:
        raise ValueError("POSTGRES_HOST environment variable couldn't be found.")

    pg_pw = os.getenv("POSTGRES_PASSWORD")
    if pg_pw is None:
        raise ValueError("POSTGRES_PASSWORD environment variable couldn't be found.")

    pg_db = os.getenv("POSTGRES_DB")
    if pg_db is None:
        raise ValueError("POSTGRES_DB environment variable couldn't be found.")

    conn = make_connection_string(
        pg_username,
        pg_pw,
        pg_hostname,
        pg_db,
    )

    asyncio.run(async_init_db(conn))
