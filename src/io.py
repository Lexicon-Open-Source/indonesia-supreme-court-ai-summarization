import asyncio
import tempfile

import aiofiles
from httpx import AsyncClient
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
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

from settings import get_settings


class Extraction(SQLModel, table=True):
    id: str = Field(primary_key=True)
    artifact_link: str
    raw_page_link: str
    metadata_: str | None = Field(
        sa_column=Column("metadata", String, default=None)
    )  # somehow this converted to dict already


class Cases(SQLModel, table=True):
    id: str = Field(primary_key=True)
    decision_number: str
    summary: str | None
    summary_en: str | None
    summary_formatted: str | None
    summary_formatted_en: str | None


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
    retry=retry_if_not_exception_type((ValueError, NotImplementedError)),
)
async def get_extraction_db_data_and_validate(
    extraction_id: str, crawler_db_engine: Engine, case_db_engine: Engine
) -> tuple[Extraction, Cases]:
    async_crawler_db_session = sessionmaker(bind=crawler_db_engine, class_=AsyncSession)
    async with async_crawler_db_session() as session:
        result_iterator = await session.execute(
            select(Extraction).where(Extraction.id == extraction_id)
        )
    crawler_query_result = [result_ for result_ in result_iterator]
    if not crawler_query_result:
        raise ValueError(f"extraction id {extraction_id} not found")
    crawler_meta: Extraction = crawler_query_result[0][0]

    if not crawler_meta.raw_page_link.startswith(
        "https://putusan3.mahkamahagung.go.id"
    ):
        raise NotImplementedError("only support supreme court document")

    decision_number = crawler_meta.metadata_.get("number", None)
    if decision_number is None:
        raise ValueError(
            "case number identifier not found in `extraction` table : "
            f"{crawler_meta.metadata_}"
        )

    async_case_db_session = sessionmaker(bind=case_db_engine, class_=AsyncSession)
    async with async_case_db_session() as session:
        result_iterator = await session.execute(
            select(Cases).where(Cases.decision_number == decision_number)
        )

    case_meta = [result for result in result_iterator]
    if not case_meta:
        raise ValueError(
            "case number identifier not found in `cases` table : "
            f"{crawler_meta.metadata_}"
        )

    return crawler_meta, case_meta[0][0]


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def read_pdf_from_uri(uri_path: str) -> tuple[dict[int, str], int]:
    print(f"downloading file from {uri_path}")
    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        async with AsyncClient(
            timeout=get_settings().async_http_request_timeout
        ) as client:
            response = await client.get(uri_path)

        async with aiofiles.open(temp_file.name, "wb") as afp:
            await afp.write(response.content)
            await afp.flush()

        elements = partition_pdf(temp_file.name)

    contents = {}
    for el in elements:
        if type(el) in [Header, Footer]:
            continue

        current_page = el.metadata.page_number
        current_content = contents.get(current_page, "")
        current_content += "\n" + str(el)
        contents[current_page] = current_content

        await asyncio.sleep(0.01)

    max_page = current_page

    return contents, max_page


async def write_summary_to_db(
    case_db_engine: Engine,
    decision_number: str,
    summary: str,
    summary_text: str,
    translated_summary: str,
    translated_summary_text: str,
):
    async_case_db_session = sessionmaker(bind=case_db_engine, class_=AsyncSession)
    async with async_case_db_session() as session:
        result_iterator = await session.execute(
            select(Cases).where(Cases.decision_number == decision_number)
        )
        case = result_iterator.one()[0]

        case.summary = summary_text
        case.summary_en = translated_summary_text
        case.summary_formatted = summary
        case.summary_formatted_en = translated_summary
        session.add(case)
        await session.commit()
        await session.refresh(case)

    print(f"updated summary decision number {decision_number}")
