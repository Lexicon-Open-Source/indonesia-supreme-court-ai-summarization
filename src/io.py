import asyncio
import tempfile
import logging

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
    logging.info(f"Validating extraction data for ID: {extraction_id}")
    try:
        # Query crawler database
        logging.debug(f"Querying crawler database for extraction ID: {extraction_id}")
        async_crawler_db_session = sessionmaker(bind=crawler_db_engine, class_=AsyncSession)
        async with async_crawler_db_session() as session:
            result_iterator = await session.execute(
                select(Extraction).where(Extraction.id == extraction_id)
            )
        crawler_query_result = [result_ for result_ in result_iterator]
        if not crawler_query_result:
            logging.error(f"Extraction ID {extraction_id} not found in crawler database")
            raise ValueError(f"extraction id {extraction_id} not found")
        crawler_meta: Extraction = crawler_query_result[0][0]
        logging.debug(f"Found extraction data: {crawler_meta.id}")

        # Validate document source
        if not crawler_meta.raw_page_link.startswith(
            "https://putusan3.mahkamahagung.go.id"
        ):
            logging.error(f"Unsupported document source: {crawler_meta.raw_page_link}")
            raise NotImplementedError("only support supreme court document")

        # Validate case number
        decision_number = crawler_meta.metadata_.get("number", None)
        if decision_number is None:
            logging.error(f"Case number not found in metadata: {crawler_meta.metadata_}")
            raise ValueError(
                "case number identifier not found in `extraction` table : "
                f"{crawler_meta.metadata_}"
            )
        logging.debug(f"Found decision number: {decision_number}")

        # Query case database
        logging.debug(f"Querying case database for decision number: {decision_number}")
        async_case_db_session = sessionmaker(bind=case_db_engine, class_=AsyncSession)
        async with async_case_db_session() as session:
            result_iterator = await session.execute(
                select(Cases).where(Cases.decision_number == decision_number)
            )

        case_meta = [result for result in result_iterator]
        if not case_meta:
            logging.error(f"Decision number {decision_number} not found in cases database")
            raise ValueError(
                "case number identifier not found in `cases` table : "
                f"{crawler_meta.metadata_}"
            )
        logging.info(f"Successfully validated extraction data for {extraction_id} with decision number {decision_number}")
        return crawler_meta, case_meta[0][0]
    except Exception as e:
        if isinstance(e, (ValueError, NotImplementedError)):
            # Let these exceptions propagate as is since they're already handled
            raise
        logging.error(f"Error in get_extraction_db_data_and_validate for {extraction_id}: {str(e)}")
        raise


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def read_pdf_from_uri(uri_path: str) -> tuple[dict[int, str], int]:
    logging.info(f"Reading PDF from URI: {uri_path}")
    try:
        logging.debug(f"Downloading file from {uri_path}")
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            try:
                async with AsyncClient(
                    timeout=get_settings().async_http_request_timeout
                ) as client:
                    logging.debug(f"Sending HTTP request to {uri_path}")
                    response = await client.get(uri_path)
                    if response.status_code != 200:
                        logging.error(f"Failed to download PDF, status code: {response.status_code}")
                        raise ValueError(f"Failed to download PDF: HTTP {response.status_code}")

                logging.debug(f"Writing response content to temporary file: {temp_file.name}")
                async with aiofiles.open(temp_file.name, "wb") as afp:
                    await afp.write(response.content)
                    await afp.flush()

                logging.debug("Partitioning PDF file")
                elements = partition_pdf(temp_file.name)
            except Exception as e:
                logging.error(f"Error processing PDF file: {str(e)}")
                raise

        logging.debug("Extracting contents from PDF elements")
        contents = {}
        current_page = 0
        for el in elements:
            if type(el) in [Header, Footer]:
                continue

            current_page = el.metadata.page_number
            current_content = contents.get(current_page, "")
            current_content += "\n" + str(el)
            contents[current_page] = current_content

            await asyncio.sleep(0.01)

        max_page = current_page
        logging.info(f"Successfully extracted {len(contents)} pages from PDF, max page: {max_page}")
        return contents, max_page
    except Exception as e:
        logging.error(f"Error in read_pdf_from_uri for {uri_path}: {str(e)}")
        raise


async def write_summary_to_db(
    case_db_engine: Engine,
    decision_number: str,
    summary: str,
    summary_text: str,
    translated_summary: str,
    translated_summary_text: str,
):
    logging.info(f"Updating DB with summary for decision number: {decision_number}")
    try:
        async_case_db_session = sessionmaker(bind=case_db_engine, class_=AsyncSession)
        async with async_case_db_session() as session:
            try:
                logging.debug(f"Querying case with decision number: {decision_number}")
                result_iterator = await session.execute(
                    select(Cases).where(Cases.decision_number == decision_number)
                )
                case = result_iterator.one()[0]

                logging.debug(f"Updating case data for decision number: {decision_number}")
                case.summary = summary_text
                case.summary_en = translated_summary_text
                case.summary_formatted = summary
                case.summary_formatted_en = translated_summary
                session.add(case)

                logging.debug(f"Committing changes for decision number: {decision_number}")
                await session.commit()
                await session.refresh(case)

                logging.info(f"Successfully updated summary for decision number: {decision_number}")
            except Exception as e:
                logging.error(f"Error updating case in database for decision number {decision_number}: {str(e)}")
                await session.rollback()
                raise
    except Exception as e:
        logging.error(f"Error in write_summary_to_db for decision number {decision_number}: {str(e)}")
        raise
