"""
Main extraction pipeline for processing Indonesian Supreme Court documents.

This module orchestrates the complete extraction workflow:
1. Fetch extraction data from database
2. Download and extract text from PDF
3. Process through LLM extraction pipeline
4. Save results to llm_extractions table
"""

import logging
from sqlalchemy.ext.asyncio import AsyncEngine

from src.io import get_extraction_db_data_and_validate, read_pdf_from_uri
from src.extraction import (
    ExtractionResult,
    ExtractionStatus,
    process_document_extraction,
    save_llm_extraction,
    update_llm_extraction_status,
)

logger = logging.getLogger(__name__)


async def run_extraction_pipeline(
    extraction_id: str,
    crawler_db_engine: AsyncEngine,
    case_db_engine: AsyncEngine,
) -> tuple[ExtractionResult, str, str, str]:
    """
    Run the complete extraction pipeline for a court decision document.

    This function orchestrates:
    1. Validation of extraction data from database
    2. PDF download and text extraction
    3. Chunked LLM extraction processing
    4. Summary generation in ID and EN
    5. Persistence to llm_extractions table

    Args:
        extraction_id: The ID of the extraction record
        crawler_db_engine: Database engine for crawler schema
        case_db_engine: Database engine for case schema

    Returns:
        Tuple of (ExtractionResult, summary_id, summary_en, decision_number)

    Raises:
        ValueError: If extraction data is invalid or not found
        Exception: If any step in the pipeline fails
    """
    logger.info(f"Starting extraction pipeline for extraction_id: {extraction_id}")

    try:
        # Step 1: Validate and fetch extraction data
        logger.info(f"Step 1: Validating extraction data for {extraction_id}")
        crawler_meta, case_meta = await get_extraction_db_data_and_validate(
            extraction_id=extraction_id,
            crawler_db_engine=crawler_db_engine,
            case_db_engine=case_db_engine,
        )
        decision_number = case_meta.decision_number
        logger.info(f"Validated extraction for decision number: {decision_number}")

        # Step 2: Download and extract text from PDF
        logger.info(f"Step 2: Reading PDF from {crawler_meta.artifact_link}")
        doc_content, max_page = await read_pdf_from_uri(crawler_meta.artifact_link)
        logger.info(f"Extracted {len(doc_content)} pages from PDF (max page: {max_page})")

        # Update status to processing
        try:
            await update_llm_extraction_status(
                db_engine=crawler_db_engine,
                extraction_id=extraction_id,
                status=ExtractionStatus.PROCESSING,
            )
        except Exception as status_error:
            # Don't fail if status update fails, just log it
            logger.warning(f"Could not update status to processing: {status_error}")

        # Step 3: Process through LLM extraction pipeline
        logger.info(f"Step 3: Processing document through LLM extraction pipeline")
        extraction_result, summary_id, summary_en = await process_document_extraction(
            decision_number=decision_number,
            doc_content=doc_content,
        )
        logger.info(f"LLM extraction completed for {decision_number}")

        # Step 4: Save to database
        logger.info(f"Step 4: Saving extraction result to database")
        await save_llm_extraction(
            db_engine=crawler_db_engine,
            extraction_id=extraction_id,
            extraction_result=extraction_result,
            summary_en=summary_en,
            summary_id=summary_id,
            status=ExtractionStatus.COMPLETED,
        )
        logger.info(f"Saved LLM extraction for {decision_number}")

        logger.info(f"Extraction pipeline completed successfully for {decision_number}")
        return extraction_result, summary_id, summary_en, decision_number

    except Exception as e:
        logger.exception(f"Extraction pipeline failed for {extraction_id}: {e}")
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
