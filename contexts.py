from sqlalchemy.ext.asyncio import create_async_engine
import sys

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
            f"postgresql+asyncpg://{get_settings().db_user}:{get_settings().db_pass}@{get_settings().db_addr}/postgres",
            future=True,
            connect_args={"server_settings": {"search_path": "bo_crawler_v1"}},
        )

        self.case_db_engine = create_async_engine(
            f"postgresql+asyncpg://{get_settings().db_user}:{get_settings().db_pass}@{get_settings().db_addr}/postgres",
            future=True,
            connect_args={"server_settings": {"search_path": "bo_v1"}},
        )

    async def get_app_contexts(self, init_nats: bool = True) -> "AppContexts":
        print(f"DIRECT LOG: get_app_contexts called with init_nats={init_nats}", flush=True)
        if init_nats and self.nats_client is None:
            print(f"DIRECT LOG: Initializing NATS with URL: {get_settings().nats__url}", flush=True)
            try:
                self.nats_client = await initialize_nats()
                print("DIRECT LOG: NATS initialized successfully", flush=True)
                stream_configs = generate_nats_stream_configs()
                print(f"DIRECT LOG: Generated stream configs: {stream_configs}", flush=True)
                self.jetstream_client = await initialize_jetstream_client(
                    nats_client=self.nats_client,
                    stream_configs=stream_configs,
                )
                print("DIRECT LOG: JetStream client initialized successfully", flush=True)
            except Exception as e:
                print(f"DIRECT LOG: ERROR initializing NATS: {str(e)}", flush=True)
                # Re-raise the exception to be caught by the caller
                raise
        elif self.nats_client is not None:
            print(f"DIRECT LOG: Using existing NATS client, connected: {self.nats_client.is_connected}", flush=True)
        else:
            print("DIRECT LOG: Skipping NATS initialization as requested", flush=True)

        print("DIRECT LOG: Successfully prepared app contexts", flush=True)
        return self
