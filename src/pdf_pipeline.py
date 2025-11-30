"""
PDF-based extraction pipeline for processing Indonesian Supreme Court documents.

This module orchestrates the complete extraction workflow using direct PDF input:
1. Fetch extraction data from database
2. Download PDF file
3. Split PDF and send directly to LLM
4. Save results to llm_extractions table
"""

import logging
import os

from sqlalchemy.ext.asyncio import AsyncEngine

from src.extraction import (
    ExtractionResult,
    ExtractionStatus,
    save_llm_extraction,
    update_llm_extraction_status,
)
from src.io import get_extraction_db_data_and_validate
from src.pdf_extraction import (
    download_pdf_to_temp_file,
    process_document_pdf_extraction,
)

logger = logging.getLogger(__name__)


async def run_pdf_extraction_pipeline(
    extraction_id: str,
    crawler_db_engine: AsyncEngine,
) -> tuple[ExtractionResult, str, str, str]:
    """
    Run the complete PDF-based extraction pipeline for a court decision document.

    This function orchestrates:
    1. Validation of extraction data from database
    2. PDF download
    3. Chunked PDF extraction processing (sends PDF directly to LLM)
    4. Summary generation in ID and EN
    5. Persistence to llm_extractions table

    Args:
        extraction_id: The ID of the extraction record
        crawler_db_engine: Database engine for crawler schema

    Returns:
        Tuple of (ExtractionResult, summary_id, summary_en, decision_number)

    Raises:
        ValueError: If extraction data is invalid or not found
        Exception: If any step in the pipeline fails
    """
    logger.info(f"Starting PDF extraction pipeline for extraction_id: {extraction_id}")

    pdf_temp_path = None

    try:
        # Step 1: Validate and fetch extraction data
        logger.info(f"Step 1: Validating extraction data for {extraction_id}")
        crawler_meta, decision_number = await get_extraction_db_data_and_validate(
            extraction_id=extraction_id,
            crawler_db_engine=crawler_db_engine,
        )
        logger.info(f"Validated extraction for decision number: {decision_number}")

        # Step 2: Download PDF to temporary file
        logger.info(f"Step 2: Downloading PDF from {crawler_meta.artifact_link}")
        pdf_temp_path = await download_pdf_to_temp_file(crawler_meta.artifact_link)
        logger.info(f"Downloaded PDF to: {pdf_temp_path}")

        # Update status to processing
        try:
            await update_llm_extraction_status(
                db_engine=crawler_db_engine,
                extraction_id=extraction_id,
                status=ExtractionStatus.PROCESSING,
            )
        except Exception as status_error:
            logger.warning(f"Could not update status to processing: {status_error}")

        # Step 3: Process through PDF extraction pipeline
        logger.info("Step 3: Processing document through PDF extraction pipeline")
        extraction_result, summary_id, summary_en = await process_document_pdf_extraction(
            decision_number=decision_number,
            pdf_path=pdf_temp_path,
        )
        logger.info(f"PDF extraction completed for {decision_number}")

        # Step 4: Save to database
        logger.info("Step 4: Saving extraction result to database")
        await save_llm_extraction(
            db_engine=crawler_db_engine,
            extraction_id=extraction_id,
            extraction_result=extraction_result,
            summary_en=summary_en,
            summary_id=summary_id,
            status=ExtractionStatus.COMPLETED,
        )
        logger.info(f"Saved LLM extraction for {decision_number}")

        logger.info(f"PDF extraction pipeline completed successfully for {decision_number}")
        return extraction_result, summary_id, summary_en, decision_number

    except Exception as e:
        logger.exception(f"PDF extraction pipeline failed for {extraction_id}: {e}")
        # Try to update status to failed
        try:
            await update_llm_extraction_status(
                db_engine=crawler_db_engine,
                extraction_id=extraction_id,
                status=ExtractionStatus.FAILED,
            )
        except Exception as status_error:
            logger.warning(f"Could not update status to failed: {status_error}")
        raise

    finally:
        # Cleanup temporary PDF file
        if pdf_temp_path and os.path.exists(pdf_temp_path):
            try:
                os.unlink(pdf_temp_path)
                logger.debug(f"Cleaned up temporary PDF: {pdf_temp_path}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup temp PDF: {cleanup_error}")
