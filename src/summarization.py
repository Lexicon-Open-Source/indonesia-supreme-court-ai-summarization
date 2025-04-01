import logging
import markdown
from bs4 import BeautifulSoup
from sqlalchemy.engine.base import Engine

from src.io import get_extraction_db_data_and_validate, read_pdf_from_uri
from src.module import generate_court_decision_summary_and_translation


async def extract_and_reformat_summary(
    extraction_id: str, crawler_db_engine: Engine, case_db_engine: Engine
) -> tuple[str, str, str]:
    logging.info(f"Starting extract_and_reformat_summary for extraction_id: {extraction_id}")
    try:
        logging.info(f"Validating extraction data for extraction_id: {extraction_id}")
        crawler_meta, case_meta = await get_extraction_db_data_and_validate(
            extraction_id=extraction_id,
            crawler_db_engine=crawler_db_engine,
            case_db_engine=case_db_engine,
        )

        logging.info(f"Reading PDF from URI for decision number: {case_meta.decision_number}")
        doc_content, max_page = await read_pdf_from_uri(crawler_meta.artifact_link)

        logging.info(f"Generating summary and translation for decision number: {case_meta.decision_number} with {max_page} pages")
        summary, translated_summary = await generate_court_decision_summary_and_translation(
            decision_number=case_meta.decision_number,
            doc_content=doc_content,
            max_page=max_page,
        )

        logging.info(f"Successfully extracted and reformatted summary for decision number: {case_meta.decision_number}")
        return summary, translated_summary, case_meta.decision_number
    except Exception as e:
        logging.error(f"Error in extract_and_reformat_summary for extraction_id {extraction_id}: {str(e)}")
        raise


def sanitize_markdown_symbol(content: str) -> str:
    logging.debug("Sanitizing markdown symbols")
    try:
        html = markdown.markdown(content)
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text()
    except Exception as e:
        logging.error(f"Error sanitizing markdown: {str(e)}")
        raise
