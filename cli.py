import asyncio
import logging
import sys
from functools import wraps

import typer

from contexts import AppContexts
from src.pipeline import run_extraction_pipeline

# Configure logging for CLI
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("extraction-cli")

# Set sqlalchemy logging level to WARNING to reduce noise
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
# Quiet down httpx logs which can be verbose
logging.getLogger("httpx").setLevel(logging.WARNING)
# Quiet down litellm info logs but keep warnings/errors
logging.getLogger("LiteLLM").setLevel(logging.WARNING)
logging.getLogger("litellm").setLevel(logging.WARNING)

app = typer.Typer()

CONTEXTS = AppContexts()


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@app.command()
@coro
async def extract(extraction_id: str):
    """
    Run LLM extraction pipeline for a court decision document.

    Args:
        extraction_id: The ID of the extraction record to process
    """
    logger.info(f"Starting extraction CLI for extraction_id: {extraction_id}")
    try:
        logger.info("Initializing application contexts")
        contexts = await CONTEXTS.get_app_contexts(init_nats=False)

        logger.info(f"Running extraction pipeline for extraction_id: {extraction_id}")
        extraction_result, summary_id, summary_en, decision_number = (
            await run_extraction_pipeline(
                extraction_id=extraction_id,
                crawler_db_engine=contexts.crawler_db_engine,
                case_db_engine=contexts.case_db_engine,
            )
        )

        # Log results
        fields_extracted = len(extraction_result.model_dump(exclude_none=True))
        logger.info(f"Successfully processed extraction for decision: {decision_number}")
        logger.info(f"Fields extracted: {fields_extracted}")
        logger.info(f"Summary ID length: {len(summary_id)} chars")
        logger.info(f"Summary EN length: {len(summary_en)} chars")

        # Print summary preview
        print("\n" + "=" * 60)
        print(f"Decision Number: {decision_number}")
        print(f"Fields Extracted: {fields_extracted}")
        print("=" * 60)
        print("\nIndonesian Summary Preview:")
        print(summary_id[:500] + "..." if len(summary_id) > 500 else summary_id)
        print("\n" + "=" * 60)

    except Exception as e:
        logger.exception(f"Error processing extraction for {extraction_id}: {e}")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    app()
