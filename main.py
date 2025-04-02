import asyncio
import json
import logging
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.exceptions import HTTPException
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from pydantic import BaseModel
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
)

from contexts import AppContexts
from nats_consumer import (
    CONSUMER_CONFIG,
    STREAM_NAME,
    STREAM_SUBJECTS,
    SUBJECT,
    close_nats_connection,
    create_job_consumer_async_task,
)
from settings import get_settings
from src.io import write_summary_to_db
from src.summarization import extract_and_reformat_summary, sanitize_markdown_symbol

# Configure logging - CHANGED TO DEBUG
logging.basicConfig(
    level=logging.DEBUG,  # Changed from INFO to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("summarization-api")

# Set SQLAlchemy logging level to WARNING to avoid verbose SQL logs
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
# Quiet down httpx logs which can be verbose
logging.getLogger("httpx").setLevel(logging.WARNING)


class SummarizationRequest(BaseModel):
    extraction_id: str


CONTEXTS = AppContexts()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    global CONTEXTS
    # startup event
    logger.info("Starting up summarization API")
    nats_consumer_job_connection = []
    try:
        logger.debug("About to get app contexts...")
        contexts = await CONTEXTS.get_app_contexts()
        logger.debug(f"NATS URL from settings: {get_settings().nats__url}")

        # Ensure stream exists before creating consumers
        try:
            logger.info(f"Ensuring stream {STREAM_NAME} exists")
            js = contexts.nats_client.jetstream()
            await js.add_stream(name=STREAM_NAME, subjects=[STREAM_SUBJECTS])
            logger.info(f"Stream {STREAM_NAME} confirmed")
        except Exception as e:
            if "already exists" not in str(e):
                logger.warning(f"Stream creation warning: {e}")
            else:
                logger.info(f"Stream {STREAM_NAME} already exists")

        # Add explicit check for NATS connection
        if not contexts.nats_client.is_connected:
            logger.error("NATS client is not connected!")
        else:
            logger.debug("NATS client is connected")

        num_of_summarizer_consumer_instances = (
            get_settings().nats__num_of_summarizer_consumer_instances
        )
        logger.info(f"Creating {num_of_summarizer_consumer_instances} NATS consumer instances (pull-based)")

        # More detailed logging for task creation
        try:
            consumer_tasks = create_job_consumer_async_task(
                nats_client=contexts.nats_client,
                jetstream_client=contexts.jetstream_client,
                consumer_config=CONSUMER_CONFIG,
                processing_func=generate_summary,
                num_of_consumer_instances=num_of_summarizer_consumer_instances,
            )
            logger.debug(f"Created {len(consumer_tasks)} consumer tasks")
            nats_consumer_job_connection.extend(consumer_tasks)

            # Add task status check
            for i, task in enumerate(consumer_tasks):
                logger.debug(f"Consumer task {i+1} status: done={task.done()}, cancelled={task.cancelled()}")

        except Exception as consumer_ex:
            logger.error(f"Error creating consumer tasks: {str(consumer_ex)}")

        logger.info("Startup completed successfully - consumers are ready for immediate message processing")
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")
        raise
    yield

    # shutdown event
    logger.info("Shutting down summarization API")
    for task in nats_consumer_job_connection:
        try:
            logger.debug(f"Closing NATS connection for task: {task}")
            close_task = asyncio.create_task(close_nats_connection(task))
            await close_task
        except Exception as e:
            logger.error(f"Error closing NATS connection: {str(e)}")
    logger.info("Shutdown completed")


app = FastAPI(lifespan=lifespan)


async def generate_summary(msg: Msg) -> None:
    global CONTEXTS
    logger.info("Starting generate_summary for NATS message")
    try:
        contexts = await CONTEXTS.get_app_contexts(init_nats=True)
        data = json.loads(msg.data.decode())
        logger.info(f"Processing summarization request: {data}")

        extraction_id = data.get("extraction_id")
        if not extraction_id:
            logger.error(f"Missing extraction_id in request data: {data}")
            # Acknowledge invalid messages to prevent reprocessing
            await msg.ack()
            return

        try:
            logger.info(f"Extracting and reformatting summary for extraction_id: {extraction_id}")
            (
                summary,
                translated_summary,
                decision_number,
            ) = await extract_and_reformat_summary(
                extraction_id=extraction_id,
                crawler_db_engine=contexts.crawler_db_engine,
                case_db_engine=contexts.case_db_engine,
            )

            logger.info(f"Sanitizing summary data for decision number: {decision_number}")
            summary_text = sanitize_markdown_symbol(summary)
            translated_summary_text = sanitize_markdown_symbol(translated_summary)

            logger.info(f"Updating database with summary for decision number: {decision_number}")
            await write_summary_to_db(
                case_db_engine=contexts.case_db_engine,
                decision_number=decision_number,
                summary=summary,
                summary_text=summary_text,
                translated_summary=translated_summary,
                translated_summary_text=translated_summary_text,
            )

            logger.info(f"Successfully processed summarization for decision number: {decision_number}")
            # Acknowledge successful processing
            await msg.ack()
        except Exception as e:
            logger.error(f"Failed to process summarization for {extraction_id}: {str(e)}")
            # For processing errors, we should not ack (let NATS retry delivery)
            # But if we're using max_deliver=2 in config, we should ack to prevent infinite retries
            await msg.ack()
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in message: {e}")
        # Acknowledge invalid messages to prevent reprocessing
        await msg.ack()
    except Exception as e:
        logger.error(f"Critical error in generate_summary: {str(e)}")
        # Log the message content in case of critical failures
        try:
            logger.error(f"Message content: {msg.data.decode()}")
        except:
            logger.error("Could not decode message data")
        # For critical errors, we should not ack (let NATS retry delivery)
        await msg.ack()  # Still ack to avoid stuck messages
    finally:
        sys.stdout.flush()


@app.post(
    "/court-decision/summarize",
    summary="Route for submitting summarization job",
)
@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def submit_summarization_job(
    payload: SummarizationRequest,
    app_contexts: Annotated[AppContexts, Depends(CONTEXTS.get_app_contexts)],
) -> dict:
    logger.info(f"Received summarization job request: {payload}")
    try:
        extraction_id = payload.extraction_id
        if not extraction_id:
            logger.error("Missing extraction_id in request payload")
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail="extraction_id is required"
            )

        nats_client: NATS = app_contexts.nats_client
        if not nats_client.is_connected:
            logger.warning("NATS client is not connected, reconnecting...")
            # Likely a reconnection will happen in get_app_contexts with init_nats=True
            app_contexts = await CONTEXTS.get_app_contexts(init_nats=True)
            nats_client = app_contexts.nats_client

        js = nats_client.jetstream()

        logger.debug(f"Adding stream {STREAM_NAME} with subjects {STREAM_SUBJECTS}")
        try:
            await js.add_stream(
                name=STREAM_NAME,
                subjects=[STREAM_SUBJECTS],
            )
        except Exception as e:
            if "already exists" not in str(e):
                logger.warning(f"Stream creation warning: {e}")
            else:
                logger.debug(f"Stream {STREAM_NAME} already exists")

        # Convert to JSON and check the message before publishing
        json_payload = payload.model_dump_json()
        logger.debug(f"Message to publish: {json_payload}")

        logger.info(f"Publishing summarization job for extraction_id: {extraction_id}")
        ack = await js.publish(SUBJECT, json_payload.encode())
        logger.info(f"Successfully submitted summarization job: {payload}, ack: {ack}")

        # Verify the stream stats after publishing
        try:
            stream_info = await js.stream_info(STREAM_NAME)
            logger.debug(f"Stream stats after publish: messages={stream_info.state.messages}, consumers={len(stream_info.state.consumer_count)}")
        except Exception as e:
            logger.warning(f"Could not get stream stats: {e}")

    except Exception as e:
        err_msg = f"Error processing summarization: {str(e)}; RECEIVED DATA: {payload}"
        logger.error(err_msg)
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Failed to process summarization request: {str(e)}"
        )

    return {"data": "success"}
