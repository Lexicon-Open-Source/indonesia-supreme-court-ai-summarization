from sqlalchemy.ext.asyncio import create_async_engine

from nats_consumer import (
    generate_nats_stream_configs,
    initialize_jetstream_client,
    initialize_nats,
)
from settings import get_settings


class AppContexts:
    def __init__(self):
        self.nats_client = None
        self.jetstream_client = None
        self.crawler_db_engine = create_async_engine(
            f"postgresql+asyncpg://{get_settings().db_user}:{get_settings().db_pass}@{get_settings().db_addr}/lexicon_bo_crawler",
            future=True,
        )

        self.case_db_engine = create_async_engine(
            f"postgresql+asyncpg://{get_settings().db_user}:{get_settings().db_pass}@{get_settings().db_addr}/lexicon_bo",
            future=True,
        )

    async def get_app_contexts(self, init_nats: bool = True) -> "AppContexts":
        if init_nats and self.nats_client is None:
            self.nats_client = await initialize_nats()
            stream_configs = generate_nats_stream_configs()
            self.jetstream_client = await initialize_jetstream_client(
                nats_client=self.nats_client,
                stream_configs=stream_configs,
            )

        return self
