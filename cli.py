import asyncio
import logging
import sys
from functools import wraps

import typer

from contexts import AppContexts
from src.io import write_summary_to_db
from src.summarization import extract_and_reformat_summary, sanitize_markdown_symbol

# Configure logging for CLI
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("summarization-cli")

# Set sqlalchemy logging level to WARNING to reduce noise
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
# Quiet down httpx logs which can be verbose
logging.getLogger("httpx").setLevel(logging.WARNING)

app = typer.Typer()

CONTEXTS = AppContexts()


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@app.command()
@coro
async def summarization_cli(extraction_id: str):
    logger.info(f"Starting summarization CLI for extraction_id: {extraction_id}")
    try:
        logger.info("Initializing application contexts")
        contexts = await CONTEXTS.get_app_contexts(init_nats=False)

        logger.info(f"Extracting and reformatting summary for extraction_id: {extraction_id}")
        summary, translated_summary, decision_number = await extract_and_reformat_summary(
            extraction_id=extraction_id,
            crawler_db_engine=contexts.crawler_db_engine,
            case_db_engine=contexts.case_db_engine,
        )

        logger.info(f"Sanitizing summaries for decision number: {decision_number}")
        summary_text = sanitize_markdown_symbol(summary)
        translated_summary_text = sanitize_markdown_symbol(translated_summary)

        logger.info(f"Writing summaries to database for decision number: {decision_number}")
        await write_summary_to_db(
            case_db_engine=contexts.case_db_engine,
            decision_number=decision_number,
            summary=summary,
            summary_text=summary_text,
            translated_summary=translated_summary,
            translated_summary_text=translated_summary_text,
        )

        logger.info(f"Successfully processed summarization for decision number: {decision_number}")
    except Exception as e:
        logger.error(f"Error processing summarization for extraction_id {extraction_id}: {str(e)}")
        raise


if __name__ == "__main__":
    app()
