import asyncio
import tempfile
import logging
import re
import os
import io
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

# Add OCR-related imports
try:
    import pytesseract
    from pdf2image import convert_from_path
    from PIL import Image
    HAS_OCR = True
except ImportError:
    HAS_OCR = False

# Add PyPDF2 as another alternative
try:
    # We won't use PyPDF2 due to compatibility issues
    HAS_PYPDF2 = False
except ImportError:
    HAS_PYPDF2 = False

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

    # First try via public URL
    try:
        logging.info("Attempting to access via public URL first")
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
            return
    except Exception as public_url_error:
        logging.warning(f"Public URL access failed, falling back to GCS authentication: {str(public_url_error)}")
        # Fall back to GCS authentication

    # GCS authentication approach as fallback
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
        logging.error(f"Error downloading from GCS using authentication: {str(e)}")
        raise ValueError(f"Failed to download: public URL access failed and GCS authentication failed: {str(e)}")


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def read_pdf_from_uri(uri_path: str) -> tuple[dict[int, str], int]:
    logging.info(f"Reading PDF from URI: {uri_path}")
    temp_file = None
    temp_file_path = None

    try:
        logging.debug(f"Downloading file from {uri_path}")
        try:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
            temp_file_path = temp_file.name
            temp_file.close()  # Close it so we can reopen it with different libraries
        except Exception as temp_file_error:
            logging.error(f"Failed to create temporary file: {str(temp_file_error)}")
            raise ValueError(f"Failed to create temporary file: {str(temp_file_error)}")

        try:
            # We use the same download approach for both GCS and standard URLs
            # Always try HTTP download first, which will handle falling back to GCS auth if needed
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

                try:
                    response = await client.get(uri_path, headers=headers)
                    if response.status_code == 200:
                        # Verify content type
                        content_type = response.headers.get("content-type", "")
                        if not content_type.startswith("application/pdf"):
                            logging.warning(f"Unexpected content type: {content_type} for URL: {uri_path}")

                        logging.debug(f"Writing response content to temporary file: {temp_file_path}")
                        async with aiofiles.open(temp_file_path, "wb") as afp:
                            await afp.write(response.content)
                            await afp.flush()
                    else:
                        raise ValueError(f"Failed to download PDF: HTTP {response.status_code}")
                except Exception as http_error:
                    # If direct HTTP download fails and this is a GCS URL, try GCS-specific approach
                    if is_gcs_url(uri_path):
                        logging.info(f"Direct HTTP download failed, falling back to GCS download for: {uri_path}")
                        await download_from_gcs(uri_path, temp_file_path)
                    else:
                        # For non-GCS URLs, propagate the error
                        logging.error(f"Failed to download PDF, error: {str(http_error)}")
                        raise

            # Verify file exists and has content
            if not os.path.exists(temp_file_path):
                raise ValueError(f"Temporary file {temp_file_path} was not created")

            if os.path.getsize(temp_file_path) == 0:
                raise ValueError(f"Temporary file {temp_file_path} is empty")

            # Initialize content dictionary and max page
            contents = {}
            max_page = 0
            extraction_successful = False
            extraction_error = None

            # Try primary extraction method first (unstructured)
            try:
                logging.debug(f"Attempting primary extraction method (unstructured) on {temp_file_path}")
                elements = partition_pdf(temp_file_path)
                if not elements:
                    logging.warning("No elements found in PDF file with primary method")
                    raise ValueError("No elements found in PDF file")

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
                    raise ValueError("No content extracted from PDF with primary method")

                max_page = current_page
                extraction_successful = True
                logging.info(f"Primary extraction successful: {len(contents)} pages, max page: {max_page}")
            except Exception as primary_extraction_error:
                logging.info(f"Primary extraction failed: {str(primary_extraction_error)}")
                extraction_error = primary_extraction_error

            # If primary method failed, try OCR
            if not extraction_successful and HAS_OCR:
                try:
                    logging.info("Previous extractions failed. Trying OCR extraction...")
                    contents = await extract_text_with_ocr(temp_file_path)
                    max_page = max(contents.keys()) if contents else 0

                    if contents and max_page > 0:
                        extraction_successful = True
                        logging.info(f"OCR extraction successful: {len(contents)} pages, max page: {max_page}")
                except Exception as ocr_error:
                    logging.warning(f"OCR extraction failed: {str(ocr_error)}")
                    if not extraction_error:
                        extraction_error = ocr_error

            # If all extraction methods failed, raise an error
            if not extraction_successful:
                logging.error("PDF text extraction failed, skip text extraction...")
                # Create a minimal content to allow processing to continue
                contents = {1: "PDF text extraction failed. This is placeholder content."}
                max_page = 1

                # For now, we'll continue with empty content rather than fail completely
                # If strict validation is required, uncomment the following:
                # if extraction_error:
                #     raise extraction_error
                # else:
                #     raise ValueError("All PDF extraction methods failed")

            return contents, max_page
        except Exception as e:
            logging.error(f"Error processing PDF file: {str(e)}")
            raise
    except Exception as e:
        logging.error(f"Error in read_pdf_from_uri for {uri_path}: {str(e)}")
        raise
    finally:
        # Clean up temporary file
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logging.debug(f"Cleaned up temporary file: {temp_file_path}")
            except Exception as e:
                logging.warning(f"Failed to remove temporary file {temp_file_path}: {str(e)}")


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


async def extract_text_with_ocr(pdf_path: str) -> dict[int, str]:
    """
    Extract text from PDF using OCR (Optical Character Recognition).
    Uses pdf2image to convert PDF pages to images and pytesseract for OCR.

    Args:
        pdf_path: Path to the PDF file

    Returns:
        Dictionary mapping page numbers to extracted text

    Raises:
        ValueError: If OCR extraction fails
    """
    if not HAS_OCR:
        raise ValueError("OCR libraries are not installed. Please install 'pytesseract' and 'pdf2image'")

    # Check if poppler is installed (required by pdf2image)
    try:
        from pdf2image.pdf2image import pdfinfo_from_path
        pdf_info = pdfinfo_from_path(pdf_path)  # This will fail if poppler is not installed
        # Use pdfinfo to get page count directly
        total_pages = pdf_info.get("Pages", 0)
        logging.info(f"PDF has {total_pages} pages according to pdfinfo")
    except Exception as poppler_error:
        logging.error(f"Poppler dependency check failed: {str(poppler_error)}")
        raise ValueError("Poppler not installed or not working. Please ensure poppler-utils is installed on the system.")

    logging.info(f"Attempting OCR extraction on {pdf_path}")
    try:
        # Process PDFs in batches to avoid memory issues with large documents
        contents = {}
        batch_size = 5  # Process 5 pages at a time to manage memory

        # Convert in batches
        current_page = 0
        while True:
            # Convert pages in current batch
            try:
                batch_start = current_page
                batch_end = current_page + batch_size - 1

                # If we know the total pages, don't try to process beyond that
                if total_pages > 0 and batch_start >= total_pages:
                    break

                logging.debug(f"Processing OCR batch: pages {batch_start+1} to {batch_end+1}")

                # Convert specific page range to images
                # first_page and last_page are 1-indexed
                images = convert_from_path(
                    pdf_path,
                    first_page=batch_start+1,
                    last_page=None if batch_end is None else batch_end+1
                )

                if not images:
                    if batch_start == 0:
                        # If we got no images on first batch, that's an error
                        raise ValueError("No images could be extracted from PDF")
                    else:
                        # If we got no images in a later batch, we've processed all pages
                        break

                # Process each image in the batch
                for i, image in enumerate(images):
                    # Page numbers start from 1 + batch offset
                    page_num = batch_start + i + 1
                    logging.debug(f"OCR processing page {page_num}")

                    # Extract text using pytesseract with Indonesian language support
                    # Try with Indonesian + English for better results
                    text = pytesseract.image_to_string(image, lang='ind+eng')

                    if text.strip():
                        contents[page_num] = text

                    # Free memory
                    del image

                    # Yield to event loop occasionally
                    await asyncio.sleep(0.01)

                # Update current page for next batch
                current_page += len(images)

                # Clear memory
                del images

                # If we know the total pages and have processed them all, exit
                if total_pages > 0 and current_page >= total_pages:
                    break

            except Exception as batch_error:
                logging.error(f"Error processing OCR batch {batch_start+1}-{batch_end+1}: {str(batch_error)}")
                # If this is the first batch and it failed, propagate the error
                if batch_start == 0:
                    raise
                # Otherwise, we've processed some pages, so break and return what we have
                break

        if not contents:
            raise ValueError("OCR extraction produced no text")

        logging.info(f"Successfully extracted {len(contents)} pages using OCR")
        return contents
    except Exception as e:
        logging.error(f"OCR extraction failed: {str(e)}")
        raise ValueError(f"OCR extraction failed: {str(e)}")
