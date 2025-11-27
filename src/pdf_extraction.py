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
import tempfile
import traceback
from typing import Any

import aiofiles
from httpx import AsyncClient
from litellm import acompletion
from pypdf import PdfReader, PdfWriter
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
    """
    reader = PdfReader(pdf_path)
    return len(reader.pages)


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
    """
    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)

    if total_pages == 0:
        raise ValueError("PDF has no pages")

    logger.info(f"Splitting PDF with {total_pages} pages into chunks of {chunk_size}")

    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="pdf_chunks_")

    chunks = []
    for start_idx in range(0, total_pages, chunk_size):
        end_idx = min(start_idx + chunk_size, total_pages)
        start_page = start_idx + 1  # 1-indexed
        end_page = end_idx  # 1-indexed

        # Create chunk PDF
        writer = PdfWriter()
        for page_idx in range(start_idx, end_idx):
            writer.add_page(reader.pages[page_idx])

        chunk_filename = f"chunk_{start_page:04d}_{end_page:04d}.pdf"
        chunk_path = os.path.join(output_dir, chunk_filename)

        with open(chunk_path, "wb") as f:
            writer.write(f)

        chunks.append((chunk_path, start_page, end_page))
        logger.debug(f"Created chunk: {chunk_filename} (pages {start_page}-{end_page})")

    logger.info(f"Split PDF into {len(chunks)} chunks")
    return chunks


def pdf_to_base64(pdf_path: str) -> str:
    """
    Convert PDF file to base64 string.

    Args:
        pdf_path: Path to PDF file

    Returns:
        Base64-encoded PDF content
    """
    with open(pdf_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


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
                f"Chunk {chunk_number}: {model_label} model {model} failed after retries"
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

                # Update current extraction
                current_extraction = result.model_dump(exclude_none=True)
                logger.debug(
                    f"Chunk {chunk_number} processed, "
                    f"fields extracted: {len(current_extraction)}"
                )

            except Exception as e:
                logger.error(f"Error processing chunk {chunk_number}: {e}")
                logger.error(f"Full traceback:\n{traceback.format_exc()}")
                continue

        # Step 3: Generate summaries
        logger.info(f"Generating summaries for {decision_number}")

        summary_id = await generate_summary_id(current_extraction)
        summary_en = await generate_summary_en(current_extraction)

        logger.info(f"PDF extraction pipeline completed for {decision_number}")

        return ExtractionResult(**current_extraction), summary_id, summary_en

    finally:
        # Cleanup temp chunks directory
        import shutil

        if os.path.exists(chunks_dir):
            shutil.rmtree(chunks_dir)
            logger.debug(f"Cleaned up chunks directory: {chunks_dir}")


async def download_pdf_to_temp_file(uri_path: str) -> str:
    """
    Download PDF from URI and save to temporary file.

    Args:
        uri_path: URL to download PDF from

    Returns:
        Path to temporary PDF file (caller responsible for cleanup)
    """
    settings = get_settings()

    # Create temp file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    temp_path = temp_file.name
    temp_file.close()

    async with AsyncClient(
        timeout=settings.async_http_request_timeout, follow_redirects=True
    ) as client:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/pdf,*/*",
        }

        try:
            response = await client.get(uri_path, headers=headers)
            if response.status_code == 200:
                async with aiofiles.open(temp_path, "wb") as f:
                    await f.write(response.content)
                return temp_path
            else:
                os.unlink(temp_path)
                raise ValueError(f"Failed to download PDF: HTTP {response.status_code}")
        except Exception as e:
            # Try GCS fallback if applicable
            if is_gcs_url(uri_path):
                logger.info(f"Direct download failed, trying GCS: {uri_path}")
                await download_from_gcs(uri_path, temp_path)
                return temp_path
            # Clean up on failure
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
