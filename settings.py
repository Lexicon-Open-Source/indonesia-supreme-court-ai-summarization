import logging
import urllib.parse
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(
    level=logging.WARNING,  # Set the logging level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Set the log message format
)


class Settings(BaseSettings):
    openai_api_key: str
    db_addr: str
    db_user: str
    db_pass: str
    nats__url: str
    nats__num_of_summarizer_consumer_instances: int = 3
    async_http_request_timeout: int = 300

    model_config = SettingsConfigDict(env_file=".env", extra="allow")


@lru_cache
def get_settings():
    settings = Settings()
    settings.db_user = urllib.parse.quote(settings.db_user)
    settings.db_pass = urllib.parse.quote(settings.db_pass)

    return settings
