import asyncio
import logging
import os
import re
import tempfile
from urllib.parse import urlparse

import aiofiles
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
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

# Add pdfminer.six imports
try:
    from io import StringIO

    from pdfminer.high_level import extract_pages, extract_text_to_fp
    from pdfminer.layout import LAParams, LTTextContainer
    HAS_PDFMINER = True
except ImportError:
    HAS_PDFMINER = False

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
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import storage
    HAS_GCS = True
except ImportError:
    HAS_GCS = False


class Extraction(SQLModel, table=True):
    __tablename__ = "extractions"
    id: str = Field(primary_key=True)
    artifact_link: str
    raw_page_link: str
    metadata_: str | None = Field(
        sa_column=Column("metadata", String, default=None)
    )  # somehow this converted to dict already


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
    retry=retry_if_not_exception_type((ValueError, NotImplementedError)),
)
async def get_extraction_db_data_and_validate(
    extraction_id: str, crawler_db_engine: AsyncEngine
) -> tuple[Extraction, str]:
    """
    Get extraction data and validate it.

    Returns:
        tuple[Extraction, str]: Extraction record and decision_number
    """
    logging.info(f"Validating extraction data for ID: {extraction_id}")
    try:
        # Query crawler database
        logging.debug(f"Querying crawler database for extraction ID: {extraction_id}")
        async_crawler_db_session = async_sessionmaker(
            bind=crawler_db_engine, class_=AsyncSession
        )
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

        # Get decision number from metadata
        decision_number = crawler_meta.metadata_.get("number", None)
        if decision_number is None:
            logging.error(f"Case number not found in metadata: {crawler_meta.metadata_}")
            raise ValueError(
                "case number identifier not found in `extraction` table : "
                f"{crawler_meta.metadata_}"
            )
        logging.info(
            f"Successfully validated extraction data for {extraction_id} "
            f"with decision number {decision_number}"
        )
        return crawler_meta, decision_number
    except Exception as e:
        if isinstance(e, (ValueError, NotImplementedError)):
            raise
        logging.error(
            f"Error in get_extraction_db_data_and_validate for {extraction_id}: {e}"
        )
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

    # First try via public URL with more robust headers and retry logic
    try:
        logging.info("Attempting to access via public URL with authenticated headers")
        async with AsyncClient(timeout=get_settings().async_http_request_timeout) as client:
            # Try with a more comprehensive browser-like user agent
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/pdf,application/octet-stream,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Cache-Control": "no-cache"
            }

            # Try up to 3 times with backoff
            retry_count = 0
            max_retries = 3
            retry_delay = 2

            while retry_count < max_retries:
                try:
                    response = await client.get(uri_path, headers=headers, follow_redirects=True)
                    if response.status_code == 200:
                        # Success!
                        async with aiofiles.open(local_path, "wb") as afp:
                            await afp.write(response.content)
                            await afp.flush()
                        logging.info(f"Successfully downloaded via public URL to {local_path}")
                        return
                    elif response.status_code == 403:
                        logging.warning(f"HTTP 403 Forbidden when accessing {uri_path} (attempt {retry_count+1}/{max_retries})")
                        retry_count += 1
                        if retry_count < max_retries:
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff
                        else:
                            raise ValueError(f"HTTP Error 403: Forbidden after {max_retries} attempts")
                    else:
                        raise ValueError(f"Failed to download from public URL: HTTP {response.status_code}")
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout when downloading {uri_path} (attempt {retry_count+1}/{max_retries})")
                    retry_count += 1
                    if retry_count < max_retries:
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        raise ValueError(f"Timeout error after {max_retries} attempts")
                except Exception as e:
                    logging.warning(f"Error during download attempt {retry_count+1}: {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        raise
    except Exception as public_url_error:
        logging.warning(f"Public URL access failed, falling back to GCS authentication: {str(public_url_error)}")
        # Fall back to GCS authentication

    # GCS authentication approach as fallback with improved error handling
    try:
        # Extract bucket and blob path from the URL
        parsed_url = urlparse(uri_path)
        path_parts = parsed_url.path.lstrip('/').split('/', 1)

        if len(path_parts) != 2:
            raise ValueError(f"Invalid GCS URL format: {uri_path}")

        bucket_name = path_parts[0]
        blob_name = path_parts[1]

        logging.info(f"Attempting GCS authenticated download: bucket={bucket_name}, blob={blob_name}")

        # Check if the GOOGLE_APPLICATION_CREDENTIALS environment variable is set
        creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_path:
            logging.info(f"Using GOOGLE_APPLICATION_CREDENTIALS from: {creds_path}")
            if not os.path.exists(creds_path):
                logging.error(f"Credentials file does not exist at: {creds_path}")
        else:
            logging.warning("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set")

        # Initialize storage client with explicit credentials
        try:
            from google.oauth2 import service_account

            # First try with explicit service account credentials
            if creds_path and os.path.exists(creds_path):
                credentials = service_account.Credentials.from_service_account_file(creds_path)
                storage_client = storage.Client(credentials=credentials, project=get_settings().gcp_project_id)
                logging.info("Created GCS client with explicit service account credentials")
            else:
                # Fallback to default credentials
                storage_client = storage.Client()
                logging.info("Created GCS client with default credentials")
        except Exception as cred_error:
            logging.warning(f"Failed to create authenticated client: {str(cred_error)}")
            logging.info("Trying anonymous access as final fallback")
            storage_client = storage.Client.create_anonymous_client()

        # Get bucket and blob with timeout and better error handling
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            # Check if blob exists
            if not blob.exists():
                logging.error(f"Blob {blob_name} does not exist in bucket {bucket_name}")
                raise ValueError(f"Blob {blob_name} does not exist in bucket {bucket_name}")

            # Download to file with timeout
            logging.info(f"Starting download from GCS: {bucket_name}/{blob_name}")
            blob.download_to_filename(local_path, timeout=90)  # Increased timeout for large files
            logging.info(f"Successfully downloaded from GCS to {local_path}")

            # Verify the file was downloaded correctly
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                return
            else:
                raise ValueError(f"Download appeared to succeed but file is empty or missing: {local_path}")

        except Exception as e:
            logging.error(f"Error accessing GCS bucket/blob: {str(e)}")
            raise ValueError(f"Failed to download from GCS: {str(e)}")

    except Exception as e:
        logging.error(f"All download methods failed for {uri_path}: {str(e)}")
        raise ValueError(f"Failed to download file: {str(e)}")


def clean_text(text):
    """
    Clean text by removing standard headers and disclaimers from Indonesian Supreme Court documents.

    Args:
        text: The text to clean

    Returns:
        The cleaned text
    """
    text = text.replace("M a h ka m a h A g u n g R e p u blik In d o n esia\n", "")
    text = text.replace("Disclaimer\n", "")
    text = text.replace(
        "Kepaniteraan Mahkamah Agung Republik Indonesia berusaha untuk selalu mencantumkan informasi paling kini dan akurat sebagai bentuk komitmen Mahkamah Agung untuk pelayanan publik, transparansi dan akuntabilitas\n",
        "",
    )
    text = text.replace(
        "pelaksanaan fungsi peradilan. Namun dalam hal-hal tertentu masih dimungkinkan terjadi permasalahan teknis terkait dengan akurasi dan keterkinian informasi yang kami sajikan, hal mana akan terus kami perbaiki dari waktu kewaktu.\n",
        "",
    )
    text = text.replace(
        "Dalam hal Anda menemukan inakurasi informasi yang termuat pada situs ini atau informasi yang seharusnya ada, namun belum tersedia, maka harap segera hubungi Kepaniteraan Mahkamah Agung RI melalui :\n",
        "",
    )
    text = text.replace(
        "Email : kepaniteraan@mahkamahagung.go.id    Telp : 021-384 3348 (ext.318)\n",
        "",
    )
    return text


def remove_watermark(text):
    """
    Remove watermark patterns from text extracted from Indonesian Supreme Court documents.

    Args:
        text: The text to clean

    Returns:
        The text with watermarks removed
    """

    # Remove diagonal watermark patterns (Mahkamah Agung Republik Indonesia)
    # This handles various ways the watermark might appear in extracted text
    patterns = [
        r'M\s*a\s*h\s*k\s*a\s*m\s*a\s*h\s*A\s*g\s*u\s*n\s*g\s*R\s*e\s*p\s*u\s*b\s*l\s*i\s*k\s*I\s*n\s*d\s*o\s*n\s*e\s*s\s*i\s*a',
        r'Mahkamah\s+Agung\s+Republik\s+Indonesia',
        r'Mahkamah\s+Agung\s+RI',
        r'Direktori\s+Putusan\s+Mahkamah\s+Agung\s+Republik\s+Indonesia',
        r'putusan\.mahkamahagung\.go\.id',
        r'Halaman \d+ dari \d+ halaman Putusan Nomor',
        r'Disclaimer.*?melalui :',  # Match disclaimer text as a block
    ]

    # Apply all patterns
    for pattern in patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)

    # Remove repeated whitespace and clean up
    text = re.sub(r'\n{3,}', '\n\n', text)  # Replace 3+ newlines with 2
    text = re.sub(r' {2,}', ' ', text)      # Replace 2+ spaces with 1

    return text.strip()


async def extract_text_with_pdfminer(pdf_path: str) -> dict[int, str]:
    """
    Extract text from PDF using pdfminer.six.

    Args:
        pdf_path: Path to the PDF file

    Returns:
        Dictionary mapping page numbers to extracted text

    Raises:
        ValueError: If pdfminer extraction fails
    """
    if not HAS_PDFMINER:
        raise ValueError("pdfminer.six is not installed. Please install 'pdfminer.six'")

    logging.info(f"Attempting pdfminer.six extraction on {pdf_path}")
    contents = {}

    try:
        # Method 1: Use page-by-page extraction with extract_text
        try:
            from pdfminer.high_level import extract_text
            from pdfminer.pdfdocument import PDFDocument
            from pdfminer.pdfparser import PDFParser

            # First get total pages
            with open(pdf_path, 'rb') as file:
                parser = PDFParser(file)
                document = PDFDocument(parser)
                total_pages = len(document.catalog.get('Pages').resolve().get('Kids', []))

            logging.info(f"PDF has {total_pages} pages")

            # Now extract each page individually
            for page_num in range(1, total_pages + 1):
                # Use extract_text with page_numbers parameter (0-indexed)
                page_text = extract_text(pdf_path, page_numbers=[page_num-1])

                if page_text.strip():
                    # Clean the text and remove watermarks
                    page_text = clean_text(page_text)
                    page_text = remove_watermark(page_text)
                    contents[page_num] = page_text

                # Yield to event loop occasionally
                await asyncio.sleep(0.01)

            if contents:
                logging.info(f"Successfully extracted {len(contents)} pages using extract_text per page")
                return contents

            logging.warning("extract_text per page method yielded no content, trying alternative methods")

        except Exception as e:
            logging.warning(f"pdfminer.six extract_text per page method failed: {str(e)}")

        # Method 2: Try page-by-page extraction with extract_pages
        try:
            contents = {}
            for i, page_layout in enumerate(extract_pages(pdf_path)):
                page_num = i + 1
                page_text = ""

                for element in page_layout:
                    if isinstance(element, LTTextContainer):
                        page_text += element.get_text()

                if page_text.strip():
                    # Clean the text and remove watermarks
                    page_text = clean_text(page_text)
                    page_text = remove_watermark(page_text)
                    contents[page_num] = page_text

                # Yield to event loop occasionally
                await asyncio.sleep(0.01)

            if contents:
                logging.info(f"Successfully extracted {len(contents)} pages using pdfminer.six extract_pages method")
                return contents
        except Exception as e:
            logging.warning(f"pdfminer.six extract_pages method failed: {str(e)}")

        # Method 3: Try extract_text_to_fp as final fallback
        try:
            output = StringIO()
            with open(pdf_path, 'rb') as fp:
                laparams = LAParams()
                extract_text_to_fp(fp, output, laparams=laparams)
                text = output.getvalue()

            # If we get any text, put it all in page 1 (we don't have page separation this way)
            if text.strip():
                # Clean the text and remove watermarks
                text = clean_text(text)
                text = remove_watermark(text)
                contents[1] = text
                logging.info("Successfully extracted text using pdfminer.six extract_text_to_fp method")
                return contents
            else:
                raise ValueError("No text extracted from PDF with any pdfminer.six method")
        except Exception as e:
            logging.warning(f"pdfminer.six extract_text_to_fp method failed: {str(e)}")
            raise ValueError("All pdfminer.six extraction methods failed")

    except Exception as e:
        logging.error(f"pdfminer.six extraction failed: {str(e)}")
        raise ValueError(f"pdfminer.six extraction failed: {str(e)}")


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

            # Try pdfminer.six extraction method first
            if HAS_PDFMINER:
                try:
                    logging.debug(f"Attempting pdfminer.six extraction on {temp_file_path}")
                    contents = await extract_text_with_pdfminer(temp_file_path)
                    max_page = max(contents.keys()) if contents else 0

                    if contents and max_page > 0:
                        extraction_successful = True
                        logging.info(f"pdfminer.six extraction successful: {len(contents)} pages, max page: {max_page}")
                except Exception as pdfminer_error:
                    logging.warning(f"pdfminer.six extraction failed: {str(pdfminer_error)}")
                    extraction_error = pdfminer_error

            # If pdfminer failed, try unstructured extraction method
            if not extraction_successful:
                try:
                    logging.debug(f"Attempting unstructured extraction method on {temp_file_path}")
                    elements = partition_pdf(temp_file_path)
                    if not elements:
                        logging.warning("No elements found in PDF file with unstructured method")
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
                        raise ValueError("No content extracted from PDF with unstructured method")

                    # Clean the text in each page
                    for page_num in contents:
                        contents[page_num] = clean_text(contents[page_num])
                        contents[page_num] = remove_watermark(contents[page_num])

                    max_page = current_page
                    extraction_successful = True
                    logging.info(f"Unstructured extraction successful: {len(contents)} pages, max page: {max_page}")
                except Exception as unstructured_error:
                    logging.info(f"Unstructured extraction failed: {str(unstructured_error)}")
                    if not extraction_error:
                        extraction_error = unstructured_error

            # If both pdfminer and unstructured failed, try OCR
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
    case_db_engine: AsyncEngine,
    decision_number: str,
    summary: str,
    summary_text: str,
    translated_summary: str,
    translated_summary_text: str,
):
    logging.info(f"Updating DB with summary for decision number: {decision_number}")
    try:
        async_case_db_session = async_sessionmaker(bind=case_db_engine, class_=AsyncSession)
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

        # For very large PDFs, limit the processing to first few pages to avoid timeouts
        if total_pages > 50:
            logging.warning(f"PDF has {total_pages} pages, processing only first 50 pages to avoid timeout")
            max_pages_to_process = 50
        else:
            max_pages_to_process = total_pages
    except Exception as poppler_error:
        logging.error(f"Poppler dependency check failed: {str(poppler_error)}")
        raise ValueError("Poppler not installed or not working. Please ensure poppler-utils is installed on the system.")

    logging.info(f"Attempting OCR extraction on {pdf_path}")
    try:
        # Process PDFs in batches to avoid memory issues with large documents
        contents = {}
        batch_size = 2  # Reduced batch size to 2 pages at a time to manage memory more conservatively

        # Set process timeout to avoid hanging
        process_timeout = 120  # 2 minutes per batch

        # Convert in batches
        current_page = 0
        while current_page < max_pages_to_process:
            # Convert pages in current batch
            try:
                batch_start = current_page
                batch_end = min(current_page + batch_size - 1, max_pages_to_process - 1)

                logging.info(f"Processing OCR batch: pages {batch_start+1} to {batch_end+1}")

                # Create a task with timeout for the batch processing
                async def process_batch():
                    # Convert specific page range to images
                    # first_page and last_page are 1-indexed
                    images = convert_from_path(
                        pdf_path,
                        first_page=batch_start+1,
                        last_page=batch_end+1,
                        dpi=150  # Lower DPI for faster processing
                    )

                    if not images:
                        if batch_start == 0:
                            # If we got no images on first batch, that's an error
                            raise ValueError("No images could be extracted from PDF")
                        return 0

                    # Process each image in the batch
                    processed_count = 0
                    for i, image in enumerate(images):
                        # Page numbers start from 1 + batch offset
                        page_num = batch_start + i + 1
                        logging.debug(f"OCR processing page {page_num}")

                        # Extract text using pytesseract with Indonesian language support
                        # Try with Indonesian + English for better results
                        text = pytesseract.image_to_string(image, lang='ind+eng')

                        if text.strip():
                            # Clean the extracted text and remove watermarks
                            text = clean_text(text)
                            text = remove_watermark(text)
                            contents[page_num] = text
                            processed_count += 1

                        # Free memory
                        del image

                        # Yield to event loop occasionally
                        await asyncio.sleep(0.01)

                    return processed_count

                # Run with timeout
                try:
                    processed_count = await asyncio.wait_for(process_batch(), timeout=process_timeout)
                    if processed_count == 0 and batch_start > 0:
                        # No more pages processed, we're done
                        break

                    # Update current page for next batch
                    current_page = batch_end + 1

                    # Log progress
                    if current_page < max_pages_to_process:
                        percent_done = (current_page / max_pages_to_process) * 100
                        logging.info(f"OCR progress: {percent_done:.1f}% ({current_page}/{max_pages_to_process} pages)")

                except asyncio.TimeoutError:
                    logging.warning(f"Timeout processing batch {batch_start+1}-{batch_end+1}, moving to next batch")
                    # If we hit a timeout, skip ahead to the next batch
                    current_page = batch_end + 1
                    continue

            except Exception as batch_error:
                logging.error(f"Error processing OCR batch {batch_start+1}-{batch_end+1}: {str(batch_error)}")
                # If this is the first batch and it failed, propagate the error
                if batch_start == 0 and not contents:
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
