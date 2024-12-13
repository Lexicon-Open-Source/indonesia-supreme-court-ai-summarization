import markdown
from bs4 import BeautifulSoup
from sqlalchemy.engine.base import Engine

from src.io import get_extraction_db_data_and_validate, read_pdf_from_uri
from src.module import generate_court_decision_summary_and_translation


async def extract_and_reformat_summary(
    extraction_id: str, crawler_db_engine: Engine, case_db_engine: Engine
) -> tuple[str, str]:
    crawler_meta, case_meta = await get_extraction_db_data_and_validate(
        extraction_id=extraction_id,
        crawler_db_engine=crawler_db_engine,
        case_db_engine=case_db_engine,
    )

    doc_content, max_page = await read_pdf_from_uri(crawler_meta.artifact_link)
    summary, translated_summary = await generate_court_decision_summary_and_translation(
        decision_number=case_meta.decision_number,
        doc_content=doc_content,
        max_page=max_page,
    )
    return summary, translated_summary, case_meta.decision_number


def sanitize_markdown_symbol(content: str) -> str:
    html = markdown.markdown(content)
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()
