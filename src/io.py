import asyncio
import tempfile
import logging
import re
import os
from urllib.parse import urlparse

import aiofiles
from httpx import AsyncClient
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Column, Field, SQLModel, String, select
from sqlmodel.ext.asyncio.session import AsyncSession
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from unstructured.documents.elements import Footer, Header
from unstructured.partition.pdf import partition_pdf

from settings import get_settings

# Add imports for Google Cloud Storage, if available
try:
    from google.cloud import storage
    from google.auth.exceptions import DefaultCredentialsError
    HAS_GCS = True
except ImportError:
    HAS_GCS = False


class Extraction(SQLModel, table=True):
    id: str = Field(primary_key=True)
    artifact_link: str
    raw_page_link: str
    metadata_: str | None = Field(
        sa_column=Column("metadata", String, default=None)
    )  # somehow this converted to dict already


class Cases(SQLModel, table=True):
    id: str = Field(primary_key=True)
    decision_number: str
    summary: str | None
    summary_en: str | None
    summary_formatted: str | None
    summary_formatted_en: str | None


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
    retry=retry_if_not_exception_type((ValueError, NotImplementedError)),
)
async def get_extraction_db_data_and_validate(
    extraction_id: str, crawler_db_engine: Engine, case_db_engine: Engine
) -> tuple[Extraction, Cases]:
    logging.info(f"Validating extraction data for ID: {extraction_id}")
    try:
        # Query crawler database
        logging.debug(f"Querying crawler database for extraction ID: {extraction_id}")
        async_crawler_db_session = sessionmaker(bind=crawler_db_engine, class_=AsyncSession)
        async with async_crawler_db_session() as session:
            result_iterator = await session.execute(
                select(Extraction).where(Extraction.id == extraction_id)
            )
        crawler_query_result = [result_ for result_ in result_iterator]
        if not crawler_query_result:
            logging.error(f"Extraction ID {extraction_id} not found in crawler database")
            raise ValueError(f"extraction id {extraction_id} not found")
        crawler_meta: Extraction = crawler_query_result[0][0]
        logging.debug(f"Found extraction data: {crawler_meta.id}")

        # Validate document source
        if not crawler_meta.raw_page_link.startswith(
            "https://putusan3.mahkamahagung.go.id"
        ):
            logging.error(f"Unsupported document source: {crawler_meta.raw_page_link}")
            raise NotImplementedError("only support supreme court document")

        # Validate case number
        decision_number = crawler_meta.metadata_.get("number", None)
        if decision_number is None:
            logging.error(f"Case number not found in metadata: {crawler_meta.metadata_}")
            raise ValueError(
                "case number identifier not found in `extraction` table : "
                f"{crawler_meta.metadata_}"
            )
        logging.debug(f"Found decision number: {decision_number}")

        # Query case database
        logging.debug(f"Querying case database for decision number: {decision_number}")
        async_case_db_session = sessionmaker(bind=case_db_engine, class_=AsyncSession)
        async with async_case_db_session() as session:
            result_iterator = await session.execute(
                select(Cases).where(Cases.decision_number == decision_number)
            )

        case_meta = [result for result in result_iterator]
        if not case_meta:
            logging.error(f"Decision number {decision_number} not found in cases database")
            raise ValueError(
                "case number identifier not found in `cases` table : "
                f"{crawler_meta.metadata_}"
            )
        logging.info(f"Successfully validated extraction data for {extraction_id} with decision number {decision_number}")
        return crawler_meta, case_meta[0][0]
    except Exception as e:
        if isinstance(e, (ValueError, NotImplementedError)):
            # Let these exceptions propagate as is since they're already handled
            raise
        logging.error(f"Error in get_extraction_db_data_and_validate for {extraction_id}: {str(e)}")
        raise


def is_gcs_url(url: str) -> bool:
    """
    Check if a URL is from Google Cloud Storage.

    Args:
        url: The URL to check

    Returns:
        bool: True if the URL is from Google Cloud Storage, False otherwise
    """
    try:
        parsed_url = urlparse(url)
        return parsed_url.netloc == "storage.googleapis.com" and parsed_url.scheme in ["http", "https"]
    except Exception as e:
        logging.error(f"Error parsing URL {url}: {str(e)}")
        return False


async def download_from_gcs(uri_path: str, local_path: str) -> None:
    """
    Download a file from Google Cloud Storage.

    Args:
        uri_path: The GCS URL
        local_path: Local path to save the file

    Raises:
        ValueError: If failed to download the file
    """
    logging.info(f"Downloading from GCS: {uri_path}")

    if not HAS_GCS:
        logging.error("Google Cloud Storage libraries are not installed")
        raise ValueError("Google Cloud Storage libraries are not installed. Please install 'google-cloud-storage'")

    try:
        # Extract bucket and blob path from the URL
        # Format: https://storage.googleapis.com/bucket-name/path/to/file
        parsed_url = urlparse(uri_path)
        path_parts = parsed_url.path.lstrip('/').split('/', 1)

        if len(path_parts) != 2:
            raise ValueError(f"Invalid GCS URL format: {uri_path}")

        bucket_name = path_parts[0]
        blob_name = path_parts[1]

        logging.debug(f"Extracted bucket: {bucket_name}, blob: {blob_name}")

        # Initialize storage client with retries
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                storage_client = storage.Client()
                break
            except DefaultCredentialsError:
                if attempt == max_retries - 1:
                    logging.warning("No GCP credentials found, trying anonymous access")
                    storage_client = storage.Client.create_anonymous_client()
                else:
                    logging.warning(f"Failed to initialize storage client, attempt {attempt + 1}/{max_retries}")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff

        # Get bucket and blob with timeout
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            # Check if blob exists
            if not blob.exists():
                raise ValueError(f"Blob {blob_name} does not exist in bucket {bucket_name}")

            # Download to file with timeout
            blob.download_to_filename(local_path, timeout=30)
            logging.info(f"Successfully downloaded from GCS to {local_path}")

        except Exception as e:
            logging.error(f"Error accessing GCS bucket/blob: {str(e)}")
            raise

    except Exception as e:
        logging.error(f"Error downloading from GCS: {str(e)}")
        # If GCS access failed, try via signed URL
        try:
            logging.info("Attempting to access via public URL")
            async with AsyncClient(timeout=get_settings().async_http_request_timeout) as client:
                # Try with a different user agent to avoid potential filtering
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Accept": "application/pdf,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Connection": "keep-alive"
                }
                response = await client.get(uri_path, headers=headers, follow_redirects=True)
                if response.status_code != 200:
                    raise ValueError(f"Failed to download from public URL: HTTP {response.status_code}, Response: {response.text}")

                # Verify content type
                content_type = response.headers.get("content-type", "")
                if not content_type.startswith("application/pdf"):
                    logging.warning(f"Unexpected content type: {content_type} for URL: {uri_path}")

                async with aiofiles.open(local_path, "wb") as afp:
                    await afp.write(response.content)
                    await afp.flush()

                logging.info(f"Successfully downloaded via public URL to {local_path}")
        except Exception as inner_e:
            logging.error(f"Error accessing via public URL: {str(inner_e)}")
            raise ValueError(f"Failed to download from GCS: {str(e)}, and public URL access failed: {str(inner_e)}")


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def read_pdf_from_uri(uri_path: str) -> tuple[dict[int, str], int]:
    logging.info(f"Reading PDF from URI: {uri_path}")
    try:
        logging.debug(f"Downloading file from {uri_path}")
        with tempfile.NamedTemporaryFile(delete=True, suffix=".pdf") as temp_file:
            try:
                # Check if this is a GCS URL
                if is_gcs_url(uri_path):
                    logging.info(f"Detected Google Cloud Storage URL: {uri_path}")
                    await download_from_gcs(uri_path, temp_file.name)
                else:
                    # Standard HTTP download
                    async with AsyncClient(
                        timeout=get_settings().async_http_request_timeout,
                        follow_redirects=True
                    ) as client:
                        logging.debug(f"Sending HTTP request to {uri_path}")
                        # Add user-agent to avoid potential filtering
                        headers = {
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                            "Accept": "application/pdf,*/*",
                            "Accept-Language": "en-US,en;q=0.9",
                            "Connection": "keep-alive"
                        }
                        response = await client.get(uri_path, headers=headers)
                        if response.status_code != 200:
                            logging.error(f"Failed to download PDF, status code: {response.status_code}, response: {response.text}")
                            raise ValueError(f"Failed to download PDF: HTTP {response.status_code}")

                        # Verify content type
                        content_type = response.headers.get("content-type", "")
                        if not content_type.startswith("application/pdf"):
                            logging.warning(f"Unexpected content type: {content_type} for URL: {uri_path}")

                        logging.debug(f"Writing response content to temporary file: {temp_file.name}")
                        async with aiofiles.open(temp_file.name, "wb") as afp:
                            await afp.write(response.content)
                            await afp.flush()

                # Verify file exists and has content
                if not os.path.exists(temp_file.name):
                    raise ValueError(f"Temporary file {temp_file.name} was not created")

                if os.path.getsize(temp_file.name) == 0:
                    raise ValueError(f"Temporary file {temp_file.name} is empty")

                logging.debug(f"Partitioning PDF file from {temp_file.name}")
                try:
                    elements = partition_pdf(temp_file.name)
                    if not elements:
                        raise ValueError("No elements found in PDF file")
                except Exception as e:
                    logging.error(f"Failed to partition PDF: {str(e)}")
                    raise ValueError(f"Failed to parse PDF file: {str(e)}")
            except Exception as e:
                logging.error(f"Error processing PDF file: {str(e)}")
                raise

        logging.debug("Extracting contents from PDF elements")
        contents = {}
        current_page = 0
        for el in elements:
            if type(el) in [Header, Footer]:
                continue

            current_page = el.metadata.page_number
            current_content = contents.get(current_page, "")
            current_content += "\n" + str(el)
            contents[current_page] = current_content

            await asyncio.sleep(0.01)

        if not contents:
            raise ValueError("No content extracted from PDF")

        max_page = current_page
        logging.info(f"Successfully extracted {len(contents)} pages from PDF, max page: {max_page}")
        return contents, max_page
    except Exception as e:
        logging.error(f"Error in read_pdf_from_uri for {uri_path}: {str(e)}")
        raise


async def write_summary_to_db(
    case_db_engine: Engine,
    decision_number: str,
    summary: str,
    summary_text: str,
    translated_summary: str,
    translated_summary_text: str,
):
    logging.info(f"Updating DB with summary for decision number: {decision_number}")
    try:
        async_case_db_session = sessionmaker(bind=case_db_engine, class_=AsyncSession)
        async with async_case_db_session() as session:
            try:
                logging.debug(f"Querying case with decision number: {decision_number}")
                result_iterator = await session.execute(
                    select(Cases).where(Cases.decision_number == decision_number)
                )
                case = result_iterator.one()[0]

                logging.debug(f"Updating case data for decision number: {decision_number}")
                case.summary = summary_text
                case.summary_en = translated_summary_text
                case.summary_formatted = summary
                case.summary_formatted_en = translated_summary
                session.add(case)

                logging.debug(f"Committing changes for decision number: {decision_number}")
                await session.commit()
                await session.refresh(case)

                logging.info(f"Successfully updated summary for decision number: {decision_number}")
            except Exception as e:
                logging.error(f"Error updating case in database for decision number {decision_number}: {str(e)}")
                await session.rollback()
                raise
    except Exception as e:
        logging.error(f"Error in write_summary_to_db for decision number {decision_number}: {str(e)}")
        raise
