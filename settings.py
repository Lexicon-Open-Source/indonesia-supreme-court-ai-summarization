import logging
import urllib.parse
import os
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level
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

    # Optional Google Cloud Storage settings
    gcp_project_id: str | None = None
    gcp_credentials_json: str | None = None  # Path to service account JSON file

    model_config = SettingsConfigDict(env_file=".env", extra="allow")


@lru_cache
def get_settings():
    settings = Settings()
    settings.db_user = urllib.parse.quote(settings.db_user)
    settings.db_pass = urllib.parse.quote(settings.db_pass)

    # Set GCP credentials environment variable if provided
    if settings.gcp_credentials_json and os.path.exists(settings.gcp_credentials_json):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.gcp_credentials_json
        logging.info(f"Set GOOGLE_APPLICATION_CREDENTIALS to {settings.gcp_credentials_json}")

    return settings
