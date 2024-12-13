import asyncio
import logging
from functools import wraps

import typer

from contexts import AppContexts
from src.io import write_summary_to_db
from src.summarization import extract_and_reformat_summary, sanitize_markdown_symbol

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

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
    contexts = await CONTEXTS.get_app_contexts(init_nats=False)

    summary, translated_summary, decision_number = await extract_and_reformat_summary(
        extraction_id=extraction_id,
        crawler_db_engine=contexts.crawler_db_engine,
        case_db_engine=contexts.case_db_engine,
    )

    summary_text = sanitize_markdown_symbol(summary)
    translated_summary_text = sanitize_markdown_symbol(translated_summary)

    await write_summary_to_db(
        case_db_engine=contexts.case_db_engine,
        decision_number=decision_number,
        summary=summary,
        summary_text=summary_text,
        translated_summary=translated_summary,
        translated_summary_text=translated_summary_text,
    )


if __name__ == "__main__":
    app()
