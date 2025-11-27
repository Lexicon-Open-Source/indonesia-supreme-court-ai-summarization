import base64
import json
import logging
import os
import tempfile
import urllib.parse
from enum import Enum
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",  # Set the log message format
)

# Global variable to store the temporary credentials file path
_temp_credentials_file: str | None = None


class QueueBackendType(str, Enum):
    """Supported queue backend types."""
    NATS = "nats"
    PUBSUB = "pubsub"


class ExtractionMode(str, Enum):
    """Supported extraction modes."""
    TEXT = "text"  # Convert PDF to text first, then send text to LLM
    PDF = "pdf"    # Send PDF directly to LLM (Gemini native PDF support)


class Settings(BaseSettings):
    gemini_api_key: str
    lexicon_api_key: str  # API key for X-LEXICON-API-KEY header auth (required)

    # Crawler database (bo_crawler_v1 schema)
    crawler_db_addr: str
    crawler_db_user: str
    crawler_db_pass: str
    crawler_db_schema: str = "bo_crawler_v1"

    # Queue backend selection: "nats" or "pubsub"
    queue_backend: QueueBackendType = QueueBackendType.NATS

    # Extraction mode: "text" (PDF to text) or "pdf" (direct PDF to LLM)
    extraction_mode: ExtractionMode = ExtractionMode.TEXT

    # NATS configuration (used when queue_backend="nats")
    nats__url: str | None = None
    nats__num_of_summarizer_consumer_instances: int = 3

    # Google Pub/Sub configuration (used when queue_backend="pubsub")
    pubsub__project_id: str | None = None
    pubsub__topic_name: str = "supreme-court-extraction"
    pubsub__subscription_name: str = "supreme-court-extraction-sub"
    pubsub__dlq_topic_name: str = "supreme-court-extraction-dlq"
    pubsub__dlq_subscription_name: str = "supreme-court-extraction-dlq-sub"
    pubsub__num_of_consumer_instances: int = 3

    async_http_request_timeout: int = 300

    # LLM extraction settings
    extraction_chunk_size: int = 50  # Number of pages per LLM chunk
    extraction_model: str = "gemini/gemini-2.5-flash-lite"  # Primary model (fast/cheap)
    extraction_fallback_model: str | None = "gemini/gemini-2.5-flash"  # First fallback
    extraction_fallback_model_2: str | None = "gemini/gemini-2.5-pro"  # Second fallback (most capable)

    # Optional: Google Cloud Storage settings
    gcp_project_id: str | None = None
    gcp_credentials_base64: str | None = None  # Base64-encoded service account JSON

    model_config = SettingsConfigDict(env_file=".env", extra="allow")

    def get_num_consumer_instances(self) -> int:
        """Get the number of consumer instances based on queue backend."""
        if self.queue_backend == QueueBackendType.PUBSUB:
            return self.pubsub__num_of_consumer_instances
        return self.nats__num_of_summarizer_consumer_instances

    def get_pubsub_project_id(self) -> str | None:
        """Get Pub/Sub project ID, falling back to GCP project ID."""
        return self.pubsub__project_id or self.gcp_project_id


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

    # URL encode database credentials
    settings.crawler_db_user = urllib.parse.quote(settings.crawler_db_user)
    settings.crawler_db_pass = urllib.parse.quote(settings.crawler_db_pass)

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
