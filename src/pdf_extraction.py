"""
PDF-based LLM Extraction module for processing Indonesian Supreme Court documents.

This module handles PDF documents by sending them directly to the LLM as PDF files,
bypassing text extraction. Gemini natively supports PDF input.

Processing flow:
1. Download PDF from URI
2. Split PDF into page chunks
3. Send PDF chunks directly to LLM for extraction
4. Database persistence to llm_extractions table
"""

import asyncio
import base64
import json
import logging
import os
import shutil
import tempfile
import traceback
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import aiofiles
from httpx import AsyncClient
from litellm import acompletion
from pypdf import PdfReader, PdfWriter
from pypdf.errors import PdfReadError
from tqdm import tqdm

from settings import get_settings
from src.extraction import (
    EXTRACTION_JSON_SCHEMA,
    ExtractionResult,
    TruncationError,
    _sanitize_json_control_chars,
    generate_summary_en,
    generate_summary_id,
)
from src.io import download_from_gcs, is_gcs_url

logger = logging.getLogger(__name__)


# =============================================================================
# Merge Utilities
# =============================================================================


def _merge_nested_value(curr_val: Any, new_val: Any) -> Any:
    """
    Merge a single value, handling nested dicts and lists.

    Args:
        curr_val: Current value
        new_val: New value from extraction

    Returns:
        Merged value
    """
    # If new value is a dict, recursively merge
    if isinstance(new_val, dict) and isinstance(curr_val, dict):
        result = curr_val.copy()
        for key, val in new_val.items():
            if key in curr_val:
                result[key] = _merge_nested_value(curr_val[key], val)
            elif val is not None:
                # New key, add it if value is not None
                result[key] = val
        return result

    # If new value is a list, use the new list if non-empty
    if isinstance(new_val, list):
        if new_val:
            return new_val
        # Empty list: keep current if it has data
        if isinstance(curr_val, list) and curr_val:
            return curr_val
        return new_val

    # For scalar values: use new value if not None
    if new_val is not None:
        return new_val

    return curr_val


def _deep_merge_extraction(
    current: dict[str, Any],
    new_result: "ExtractionResult",
) -> dict[str, Any]:
    """
    Deep merge a new ExtractionResult into the current extraction dict.

    This function properly handles the distinction between:
    - Fields not present in this chunk (should not overwrite existing values)
    - Fields explicitly set to None (should overwrite existing values)

    Uses model_fields_set to determine which fields were explicitly set by the LLM.

    Args:
        current: The current accumulated extraction dict
        new_result: The new ExtractionResult from the current chunk

    Returns:
        Updated extraction dict with properly merged values
    """
    # Get fields that were explicitly set in the new result
    fields_set = new_result.model_fields_set

    # Get all data including explicit Nones
    new_data = new_result.model_dump()

    # Start merging
    result = current.copy()

    for field_name in fields_set:
        new_val = new_data.get(field_name)
        curr_val = result.get(field_name)

        if curr_val is None:
            # No existing value, use new value if not None
            if new_val is not None:
                result[field_name] = new_val
        else:
            # Has existing value, merge appropriately
            result[field_name] = _merge_nested_value(curr_val, new_val)

    return result


# =============================================================================
# Constants and Prompts
# =============================================================================

PDF_EXTRACTION_SYSTEM_PROMPT = f"""You are a professional legal expert specialized in analyzing Indonesian Supreme Court (Mahkamah Agung) decision documents.

Your task is to extract structured information from court decision PDF documents. You will receive:
1. The current extraction result (may be empty or partially filled)
2. A PDF document containing pages from the court decision

You must:
1. Carefully read and understand all text in the PDF document
2. Extract any relevant information that matches the required fields
3. Update the extraction result with newly found information
4. Preserve existing information unless you find more accurate/complete data
5. Return the updated extraction result as valid JSON

Important guidelines:
- Only extract information that is explicitly stated in the document
- Use null for fields where information is not found
- Dates should be in YYYY-MM-DD format
- Monetary values should be numbers without currency symbols
- Prison sentences in months should be converted (e.g., "1 tahun 6 bulan" = 18)
- Be careful to distinguish between prosecution demands (tuntutan) and final verdict (putusan)
- ALWAYS provide an extraction_confidence score (0.0-1.0) indicating your overall confidence
- When encountering tables in the document, extract all data accurately
- Return ONLY valid JSON, no markdown code blocks or explanations

{EXTRACTION_JSON_SCHEMA}
"""

PDF_EXTRACTION_PROMPT = """# CURRENT EXTRACTION RESULT
This is the current state of extracted information (update with new findings):

{current_extraction}

# DOCUMENT PAGES
You are viewing pages {start_page} to {end_page} (chunk {chunk_number} of {total_chunks}) of the court decision document.
The attached PDF contains the actual document pages. Please extract all relevant information from these pages.

# INSTRUCTIONS
1. Carefully read all pages in the PDF
2. Extract any relevant information for the structured fields
3. Update the extraction result with new information found
4. Preserve existing data unless you find more accurate information
5. Provide an extraction_confidence score (0.0-1.0) based on document quality and extraction certainty
6. Return the complete updated extraction result as valid JSON
"""


# =============================================================================
# PDF Processing Functions
# =============================================================================


def get_pdf_page_count(pdf_path: str) -> int:
    """
    Get the total number of pages in a PDF file.

    Args:
        pdf_path: Path to PDF file

    Returns:
        Total number of pages

    Raises:
        ValueError: If the PDF file cannot be read or is invalid
    """
    try:
        reader = PdfReader(pdf_path)
        return len(reader.pages)
    except FileNotFoundError as e:
        logger.error(f"PDF file not found: {pdf_path}")
        raise ValueError(f"PDF file not found: {pdf_path}") from e
    except PdfReadError as e:
        logger.error(
            f"Failed to read PDF file (invalid or corrupted): {pdf_path} - {e}"
        )
        raise ValueError(
            f"Failed to read PDF file (invalid or corrupted): {pdf_path}"
        ) from e
    except Exception as e:
        logger.error(f"Unexpected error reading PDF file: {pdf_path} - {e}")
        raise ValueError(
            f"Unexpected error reading PDF file: {pdf_path} - {e}"
        ) from e


def split_pdf_to_chunks(
    pdf_path: str,
    chunk_size: int,
    output_dir: str | None = None,
) -> list[tuple[str, int, int]]:
    """
    Split PDF into smaller PDF files by page chunks.

    Args:
        pdf_path: Path to source PDF file
        chunk_size: Number of pages per chunk
        output_dir: Directory for output files (uses temp dir if None)

    Returns:
        List of tuples: (chunk_path, start_page, end_page)

    Raises:
        ValueError: If the PDF file cannot be read or is invalid
        IOError: If chunk files cannot be written
    """
    # Read source PDF with robust error handling
    try:
        reader = PdfReader(pdf_path)
        total_pages = len(reader.pages)
    except FileNotFoundError as e:
        logger.error(f"PDF file not found: {pdf_path}")
        raise ValueError(f"PDF file not found: {pdf_path}") from e
    except PermissionError as e:
        logger.error(f"Permission denied reading PDF file: {pdf_path} - {e}")
        raise ValueError(f"Permission denied reading PDF file: {pdf_path}") from e
    except PdfReadError as e:
        logger.error(
            f"Failed to read PDF file (invalid or corrupted): {pdf_path} - {e}"
        )
        raise ValueError(
            f"Failed to read PDF file (invalid or corrupted): {pdf_path}"
        ) from e
    except Exception as e:
        logger.error(f"Unexpected error reading PDF file: {pdf_path} - {e}")
        raise ValueError(f"Unexpected error reading PDF file: {pdf_path}") from e

    if total_pages == 0:
        raise ValueError(f"PDF has no pages: {pdf_path}")

    logger.info(f"Splitting PDF with {total_pages} pages into chunks of {chunk_size}")

    # Setup output directory
    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="pdf_chunks_")
    else:
        # Ensure output directory exists
        try:
            os.makedirs(output_dir, exist_ok=True)
        except PermissionError as e:
            logger.error(
                f"Permission denied creating output directory: {output_dir} - {e}"
            )
            raise IOError(
                f"Permission denied creating output directory: {output_dir}"
            ) from e
        except OSError as e:
            logger.error(f"Failed to create output directory: {output_dir} - {e}")
            raise IOError(f"Failed to create output directory: {output_dir}") from e

    chunks = []
    for start_idx in range(0, total_pages, chunk_size):
        end_idx = min(start_idx + chunk_size, total_pages)
        start_page = start_idx + 1  # 1-indexed
        end_page = end_idx  # 1-indexed

        chunk_filename = f"chunk_{start_page:04d}_{end_page:04d}.pdf"
        chunk_path = os.path.join(output_dir, chunk_filename)

        # Create chunk PDF with error handling
        try:
            writer = PdfWriter()
            for page_idx in range(start_idx, end_idx):
                writer.add_page(reader.pages[page_idx])

            with open(chunk_path, "wb") as f:
                writer.write(f)

        except PermissionError as e:
            logger.error(
                f"Permission denied writing chunk file: {chunk_path} - {e}"
            )
            _cleanup_partial_file(chunk_path)
            raise IOError(
                f"Permission denied writing chunk file: {chunk_path}"
            ) from e
        except (IOError, OSError) as e:
            logger.error(f"I/O error writing chunk file: {chunk_path} - {e}")
            _cleanup_partial_file(chunk_path)
            raise IOError(f"I/O error writing chunk file: {chunk_path}") from e
        except PdfReadError as e:
            # Can occur when adding pages from corrupted source
            logger.error(
                f"PDF error creating chunk (pages {start_page}-{end_page}): "
                f"{chunk_path} - {e}"
            )
            _cleanup_partial_file(chunk_path)
            raise ValueError(
                f"PDF error creating chunk (pages {start_page}-{end_page})"
            ) from e
        except Exception as e:
            logger.error(
                f"Unexpected error writing chunk file: {chunk_path} - {e}"
            )
            _cleanup_partial_file(chunk_path)
            raise IOError(
                f"Unexpected error writing chunk file: {chunk_path}"
            ) from e

        chunks.append((chunk_path, start_page, end_page))
        logger.debug(f"Created chunk: {chunk_filename} (pages {start_page}-{end_page})")

    logger.info(f"Split PDF into {len(chunks)} chunks")
    return chunks


def _cleanup_partial_file(file_path: str) -> None:
    """Remove a partially written file if it exists."""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"Cleaned up partial file: {file_path}")
    except OSError as e:
        logger.warning(f"Failed to clean up partial file: {file_path} - {e}")


def pdf_to_base64(pdf_path: str) -> str:
    """
    Convert PDF file to base64 string.

    Args:
        pdf_path: Path to PDF file

    Returns:
        Base64-encoded PDF content

    Raises:
        ValueError: If the PDF file cannot be read
    """
    try:
        with open(pdf_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except FileNotFoundError as e:
        logger.error(f"PDF file not found for base64 encoding: {pdf_path}")
        raise ValueError(f"PDF file not found: {pdf_path}") from e
    except OSError as e:
        logger.error(f"IO error reading PDF file for base64 encoding: {pdf_path} - {e}")
        raise ValueError(f"Failed to read PDF file: {pdf_path} - {e}") from e
    except Exception as e:
        logger.error(
            f"Unexpected error encoding PDF to base64: {pdf_path} - {e}"
        )
        raise ValueError(
            f"Unexpected error encoding PDF to base64: {pdf_path}"
        ) from e


# =============================================================================
# LLM Extraction Functions
# =============================================================================


async def _call_pdf_extraction_llm(
    messages: list[dict],
    model: str,
    chunk_number: int,
) -> ExtractionResult:
    """
    Call LLM with PDF input for extraction.

    Args:
        messages: Messages including PDF content
        model: Model identifier
        chunk_number: Current chunk number for logging

    Returns:
        ExtractionResult parsed from LLM response

    Raises:
        TruncationError: If response was truncated
        json.JSONDecodeError: If response is not valid JSON
    """
    logger.info(f"Chunk {chunk_number}: Calling PDF extraction model {model}")

    response = await acompletion(
        model=model,
        messages=messages,
        response_format={"type": "json_object"},
    )

    raw_content = response.choices[0].message.content
    finish_reason = response.choices[0].finish_reason

    # Check if response was truncated
    if finish_reason == "length":
        raise TruncationError(
            f"Chunk {chunk_number}: Response truncated due to max_tokens limit"
        )

    logger.debug(f"Raw LLM response for chunk {chunk_number}: {raw_content[:500]}...")

    # Clean up response
    cleaned_content = raw_content.strip()
    if cleaned_content.startswith("```json"):
        cleaned_content = cleaned_content[7:]
    elif cleaned_content.startswith("```"):
        cleaned_content = cleaned_content[3:]
    if cleaned_content.endswith("```"):
        cleaned_content = cleaned_content[:-3]
    cleaned_content = cleaned_content.strip()

    # Sanitize control characters
    cleaned_content = _sanitize_json_control_chars(cleaned_content)

    # Parse JSON
    parsed_json = json.loads(cleaned_content)

    # Validate and create result
    result = ExtractionResult(**parsed_json)

    # Check result quality
    non_null_fields = sum(1 for v in result.model_dump().values() if v is not None)
    logger.info(
        f"Chunk {chunk_number}: extracted {non_null_fields} non-null fields "
        f"using {model}"
    )

    if non_null_fields <= 1:
        logger.warning(
            f"Chunk {chunk_number} extraction mostly empty. "
            f"Raw response preview: {raw_content[:200]}"
        )

    return result


async def _try_pdf_model_with_retries(
    messages: list[dict],
    model: str,
    chunk_number: int,
    max_attempts: int = 3,
) -> ExtractionResult | None:
    """
    Try a PDF extraction model with retries. Returns None if all attempts fail.
    """
    for attempt in range(max_attempts):
        try:
            return await _call_pdf_extraction_llm(messages, model, chunk_number)
        except TruncationError:
            raise  # Don't retry truncation
        except json.JSONDecodeError as e:
            logger.warning(
                f"Chunk {chunk_number}: JSON parse error with {model} "
                f"(attempt {attempt + 1}/{max_attempts}): {e}"
            )
            if attempt < max_attempts - 1:
                await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.warning(
                f"Chunk {chunk_number}: Error with {model} "
                f"(attempt {attempt + 1}/{max_attempts}): {e}"
            )
            if attempt < max_attempts - 1:
                await asyncio.sleep(2 ** attempt)
    return None


async def extract_from_pdf_chunk(
    pdf_chunk_path: str,
    current_extraction: dict[str, Any],
    chunk_number: int,
    total_chunks: int,
    start_page: int,
    end_page: int,
) -> ExtractionResult:
    """
    Extract information from a PDF chunk using LLM.

    Args:
        pdf_chunk_path: Path to PDF chunk file
        current_extraction: Current extraction result to update
        chunk_number: Current chunk number (1-indexed)
        total_chunks: Total number of chunks
        start_page: First page number in chunk
        end_page: Last page number in chunk

    Returns:
        Updated ExtractionResult
    """
    logger.debug(
        f"Extracting from PDF chunk {chunk_number}/{total_chunks} "
        f"(pages {start_page}-{end_page})"
    )

    # Convert PDF to base64
    pdf_base64 = pdf_to_base64(pdf_chunk_path)

    # Build message content with text and PDF
    user_content = [
        {
            "type": "text",
            "text": PDF_EXTRACTION_PROMPT.format(
                current_extraction=json.dumps(
                    current_extraction, indent=2, ensure_ascii=False
                ),
                start_page=start_page,
                end_page=end_page,
                chunk_number=chunk_number,
                total_chunks=total_chunks,
            ),
        },
        {
            "type": "image_url",
            "image_url": {
                "url": f"data:application/pdf;base64,{pdf_base64}",
            },
        },
    ]

    messages = [
        {"role": "system", "content": PDF_EXTRACTION_SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]

    settings = get_settings()

    # Build model chain
    model_chain = []
    seen = set()
    for model in [
        settings.extraction_model,
        settings.extraction_fallback_model,
        settings.extraction_fallback_model_2,
    ]:
        if model and model not in seen:
            model_chain.append(model)
            seen.add(model)

    if not model_chain:
        raise ValueError("No extraction models configured")

    logger.debug(f"Chunk {chunk_number}: Model chain: {' -> '.join(model_chain)}")

    last_error = None
    for i, model in enumerate(model_chain):
        model_label = "Primary" if i == 0 else f"Fallback-{i}"
        try:
            result = await _try_pdf_model_with_retries(messages, model, chunk_number)
            if result is not None:
                if i > 0:
                    logger.info(
                        f"Chunk {chunk_number}: {model_label} model {model} succeeded"
                    )
                return result
            logger.warning(
                f"Chunk {chunk_number}: {model_label} model {model} "
                "failed after retries"
            )
        except TruncationError as e:
            logger.warning(
                f"Chunk {chunk_number}: {model_label} model {model} truncated, "
                f"trying next model..."
            )
            last_error = e
            continue

    # All models failed
    logger.error(
        f"Chunk {chunk_number}: All {len(model_chain)} models failed. "
        f"Last error: {last_error}"
    )
    raise last_error or ValueError(f"PDF extraction failed for chunk {chunk_number}")


# =============================================================================
# Main Processing Functions
# =============================================================================


async def process_document_pdf_extraction(
    decision_number: str,
    pdf_path: str,
) -> tuple[ExtractionResult, str, str]:
    """
    Process document through the PDF-based extraction pipeline.

    Args:
        decision_number: The court decision number
        pdf_path: Path to PDF file

    Returns:
        Tuple of (ExtractionResult, summary_id, summary_en)
    """
    logger.info(f"Starting PDF extraction pipeline for decision: {decision_number}")

    if not os.path.exists(pdf_path):
        raise ValueError(f"PDF file not found: {pdf_path}")

    settings = get_settings()
    chunk_size = settings.extraction_chunk_size

    # Create temp directory for chunks
    chunks_dir = tempfile.mkdtemp(prefix="pdf_extraction_")

    try:
        # Step 1: Split PDF into chunks
        chunks = split_pdf_to_chunks(
            pdf_path=pdf_path,
            chunk_size=chunk_size,
            output_dir=chunks_dir,
        )
        total_chunks = len(chunks)
        logger.info(
            f"Document split into {total_chunks} chunks (chunk_size={chunk_size} pages)"
        )

        if total_chunks == 0:
            raise ValueError(f"No chunks created for {decision_number}")

        # Step 2: Process each chunk iteratively
        current_extraction: dict[str, Any] = {}
        successful_chunks = 0

        for i, (chunk_path, start_page, end_page) in enumerate(
            tqdm(chunks, desc=f"Processing {decision_number}")
        ):
            chunk_number = i + 1

            logger.info(
                f"Processing chunk {chunk_number}/{total_chunks} for {decision_number} "
                f"(pages {start_page}-{end_page})"
            )

            try:
                result = await extract_from_pdf_chunk(
                    pdf_chunk_path=chunk_path,
                    current_extraction=current_extraction,
                    chunk_number=chunk_number,
                    total_chunks=total_chunks,
                    start_page=start_page,
                    end_page=end_page,
                )

                # Update current extraction using deep merge
                # This properly handles:
                # - Fields not in this chunk (preserved from previous)
                # - Fields explicitly set to None (can overwrite stale data)
                current_extraction = _deep_merge_extraction(
                    current_extraction, result
                )
                successful_chunks += 1
                logger.debug(
                    f"Chunk {chunk_number} processed, "
                    f"fields extracted: {len(current_extraction)}"
                )

            except Exception as e:
                logger.error(f"Error processing chunk {chunk_number}: {e}")
                logger.error(f"Full traceback:\n{traceback.format_exc()}")
                continue

        # Fail if no chunks were successfully processed
        if successful_chunks == 0:
            raise ValueError(
                f"All {total_chunks} chunks failed for {decision_number}"
            )

        # Step 3: Generate summaries
        logger.info(f"Generating summaries for {decision_number}")

        summary_id = await generate_summary_id(current_extraction)
        summary_en = await generate_summary_en(current_extraction)

        logger.info(f"PDF extraction pipeline completed for {decision_number}")

        return ExtractionResult(**current_extraction), summary_id, summary_en

    finally:
        # Cleanup temp chunks directory
        if os.path.exists(chunks_dir):
            shutil.rmtree(chunks_dir)
            logger.debug(f"Cleaned up chunks directory: {chunks_dir}")


def cleanup_temp_file(temp_path: str) -> None:
    """
    Safely clean up a temporary file.

    Args:
        temp_path: Path to the temporary file to delete
    """
    if temp_path and os.path.exists(temp_path):
        try:
            os.unlink(temp_path)
            logger.debug(f"Cleaned up temporary file: {temp_path}")
        except OSError as e:
            logger.warning(f"Failed to cleanup temp file {temp_path}: {e}")


async def _download_pdf_impl(uri_path: str, temp_path: str) -> None:
    """
    Internal implementation for downloading PDF to a given path.

    Args:
        uri_path: URL to download PDF from
        temp_path: Path to save the PDF to

    Raises:
        ValueError: If download fails
    """
    settings = get_settings()

    async with AsyncClient(
        timeout=settings.async_http_request_timeout, follow_redirects=True
    ) as client:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            ),
            "Accept": "application/pdf,*/*",
        }

        try:
            response = await client.get(uri_path, headers=headers)
            if response.status_code == 200:
                async with aiofiles.open(temp_path, "wb") as f:
                    await f.write(response.content)
                return
            else:
                raise ValueError(
                    f"Failed to download PDF: HTTP {response.status_code}"
                )
        except ValueError:
            # Re-raise ValueError (HTTP errors) without GCS fallback attempt
            raise
        except Exception:
            # Try GCS fallback if applicable
            if is_gcs_url(uri_path):
                logger.info(f"Direct download failed, trying GCS: {uri_path}")
                await download_from_gcs(uri_path, temp_path)
                return
            raise


@asynccontextmanager
async def download_pdf_temp_file(uri_path: str) -> AsyncIterator[str]:
    """
    Download PDF from URI to a temporary file with automatic cleanup.

    Use this as an async context manager for automatic cleanup:

        async with download_pdf_temp_file(uri) as pdf_path:
            # Use pdf_path
            ...
        # File is automatically deleted after the block

    Args:
        uri_path: URL to download PDF from

    Yields:
        Path to temporary PDF file

    Raises:
        ValueError: If download fails
    """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    temp_path = temp_file.name
    temp_file.close()

    try:
        await _download_pdf_impl(uri_path, temp_path)
        yield temp_path
    finally:
        cleanup_temp_file(temp_path)


async def download_pdf_to_temp_file(uri_path: str) -> str:
    """
    Download PDF from URI and save to temporary file.

    WARNING: Caller is responsible for cleanup! Use cleanup_temp_file() or
    os.unlink() when done. For automatic cleanup, use download_pdf_temp_file()
    context manager instead.

    Args:
        uri_path: URL to download PDF from

    Returns:
        Path to temporary PDF file

    Raises:
        ValueError: If download fails

    Example:
        temp_path = await download_pdf_to_temp_file(uri)
        try:
            # Use temp_path
            ...
        finally:
            cleanup_temp_file(temp_path)
    """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    temp_path = temp_file.name
    temp_file.close()

    try:
        await _download_pdf_impl(uri_path, temp_path)
        return temp_path
    except Exception:
        # Clean up on failure
        cleanup_temp_file(temp_path)
        raise
