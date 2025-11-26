import logging
import urllib.parse
import os
import base64
import json
import tempfile
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",  # Set the log message format
)

# Global variable to store the temporary credentials file path
_temp_credentials_file: str | None = None


class Settings(BaseSettings):
    gemini_api_key: str

    # Crawler database (bo_crawler_v1 schema)
    crawler_db_addr: str
    crawler_db_user: str
    crawler_db_pass: str
    crawler_db_schema: str = "bo_crawler_v1"

    # Case database (bo_v1 schema)
    case_db_addr: str
    case_db_user: str
    case_db_pass: str
    case_db_schema: str = "bo_v1"

    nats__url: str
    nats__num_of_summarizer_consumer_instances: int = 3
    async_http_request_timeout: int = 300

    # Optional: Google Cloud Storage settings
    gcp_project_id: str | None = None
    gcp_credentials_base64: str | None = None  # Base64-encoded service account JSON

    model_config = SettingsConfigDict(env_file=".env", extra="allow")


def _decode_and_save_credentials(base64_credentials: str) -> str:
    """
    Decode base64 credentials and save to a temporary file.
    Returns the path to the temporary file.
    """
    global _temp_credentials_file

    try:
        # Decode the base64 string
        credentials_json = base64.b64decode(base64_credentials).decode('utf-8')

        # Validate it's valid JSON
        json.loads(credentials_json)

        # Create a temporary file that persists
        temp_file = tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.json',
            delete=False,
            prefix='gcp_credentials_'
        )
        temp_file.write(credentials_json)
        temp_file.close()

        _temp_credentials_file = temp_file.name
        return temp_file.name
    except base64.binascii.Error as e:
        raise ValueError(f"Invalid base64 encoding for GCP credentials: {e}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in GCP credentials: {e}")


@lru_cache
def get_settings():
    settings = Settings()

    # URL encode credentials for both databases
    settings.crawler_db_user = urllib.parse.quote(settings.crawler_db_user)
    settings.crawler_db_pass = urllib.parse.quote(settings.crawler_db_pass)
    settings.case_db_user = urllib.parse.quote(settings.case_db_user)
    settings.case_db_pass = urllib.parse.quote(settings.case_db_pass)

    # Set Gemini API key for litellm
    if settings.gemini_api_key:
        os.environ["GEMINI_API_KEY"] = settings.gemini_api_key
        logging.info("Set GEMINI_API_KEY for litellm")

    # Set GCP credentials from base64-encoded string (for GCS access, optional)
    if settings.gcp_credentials_base64:
        credentials_path = _decode_and_save_credentials(settings.gcp_credentials_base64)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        logging.info("Set GOOGLE_APPLICATION_CREDENTIALS from base64-encoded credentials")

    return settings
