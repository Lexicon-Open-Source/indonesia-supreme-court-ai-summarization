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

# Add direct print statements for Docker logs - these will be visible regardless of logger configuration
print("DIRECT LOG: Starting application initialization", flush=True)
print(f"DIRECT LOG: Python version: {sys.version}", flush=True)

# Configure logging - CHANGED TO DEBUG
logging.basicConfig(
    level=logging.DEBUG,  # Changed from INFO to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
# Force stream handler to flush after each write
for handler in logging.root.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.flush = sys.stdout.flush

logger = logging.getLogger("summarization-api")
print(f"DIRECT LOG: Created logger: summarization-api at level {logging.getLevelName(logger.level)}", flush=True)

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
    print("DIRECT LOG: Starting up summarization API", flush=True)
    logger.info("Starting up summarization API")
    nats_consumer_job_connection = []
    try:
        print(f"DIRECT LOG: About to get app contexts with NATS URL: {get_settings().nats__url}", flush=True)
        logger.debug("About to get app contexts...")
        contexts = await CONTEXTS.get_app_contexts()
        print("DIRECT LOG: Got app contexts", flush=True)
        logger.debug(f"NATS URL from settings: {get_settings().nats__url}")

        # Ensure stream exists before creating consumers
        try:
            print(f"DIRECT LOG: Ensuring stream {STREAM_NAME} exists", flush=True)
            logger.info(f"Ensuring stream {STREAM_NAME} exists")
            js = contexts.nats_client.jetstream()
            await js.add_stream(name=STREAM_NAME, subjects=[STREAM_SUBJECTS])
            print(f"DIRECT LOG: Stream {STREAM_NAME} confirmed", flush=True)
            logger.info(f"Stream {STREAM_NAME} confirmed")
        except Exception as e:
            if "already exists" not in str(e):
                print(f"DIRECT LOG: Stream creation warning: {e}", flush=True)
                logger.warning(f"Stream creation warning: {e}")
            else:
                print(f"DIRECT LOG: Stream {STREAM_NAME} already exists", flush=True)
                logger.info(f"Stream {STREAM_NAME} already exists")

        # Add explicit check for NATS connection
        if not contexts.nats_client.is_connected:
            print("DIRECT LOG: NATS client is not connected!", flush=True)
            logger.error("NATS client is not connected!")
        else:
            print("DIRECT LOG: NATS client is connected", flush=True)
            logger.debug("NATS client is connected")

        num_of_summarizer_consumer_instances = (
            get_settings().nats__num_of_summarizer_consumer_instances
        )
        print(f"DIRECT LOG: Creating {num_of_summarizer_consumer_instances} NATS consumer instances", flush=True)
        logger.info(f"Creating {num_of_summarizer_consumer_instances} NATS consumer instances (pull-based)")

        # More detailed logging for task creation
        try:
            print("DIRECT LOG: Creating consumer tasks", flush=True)
            consumer_tasks = create_job_consumer_async_task(
                nats_client=contexts.nats_client,
                jetstream_client=contexts.jetstream_client,
                consumer_config=CONSUMER_CONFIG,
                processing_func=generate_summary,
                num_of_consumer_instances=num_of_summarizer_consumer_instances,
            )
            print(f"DIRECT LOG: Created {len(consumer_tasks)} consumer tasks", flush=True)
            logger.debug(f"Created {len(consumer_tasks)} consumer tasks")
            nats_consumer_job_connection.extend(consumer_tasks)

            # Add task status check
            for i, task in enumerate(consumer_tasks):
                print(f"DIRECT LOG: Consumer task {i+1} status: done={task.done()}, cancelled={task.cancelled()}", flush=True)
                logger.debug(f"Consumer task {i+1} status: done={task.done()}, cancelled={task.cancelled()}")

        except Exception as consumer_ex:
            print(f"DIRECT LOG: Error creating consumer tasks: {str(consumer_ex)}", flush=True)
            logger.error(f"Error creating consumer tasks: {str(consumer_ex)}")

        print("DIRECT LOG: Startup completed successfully", flush=True)
        logger.info("Startup completed successfully - consumers are ready for immediate message processing")
    except Exception as e:
        print(f"DIRECT LOG: Error during startup: {str(e)}", flush=True)
        logger.error(f"Error during startup: {str(e)}")
        raise
    yield

    # shutdown event
    print("DIRECT LOG: Shutting down summarization API", flush=True)
    logger.info("Shutting down summarization API")
    for task in nats_consumer_job_connection:
        try:
            print(f"DIRECT LOG: Closing NATS connection for task: {task}", flush=True)
            logger.debug(f"Closing NATS connection for task: {task}")
            close_task = asyncio.create_task(close_nats_connection(task))
            await close_task
        except Exception as e:
            print(f"DIRECT LOG: Error closing NATS connection: {str(e)}", flush=True)
            logger.error(f"Error closing NATS connection: {str(e)}")
    print("DIRECT LOG: Shutdown completed", flush=True)
    logger.info("Shutdown completed")


app = FastAPI(lifespan=lifespan)


async def generate_summary(msg: Msg) -> None:
    global CONTEXTS
    print("DIRECT LOG: Starting generate_summary for NATS message", flush=True)
    logger.info("Starting generate_summary for NATS message")
    try:
        contexts = await CONTEXTS.get_app_contexts(init_nats=True)
        data = json.loads(msg.data.decode())
        print(f"DIRECT LOG: Processing summarization request: {data}", flush=True)
        logger.info(f"Processing summarization request: {data}")

        extraction_id = data.get("extraction_id")
        if not extraction_id:
            print(f"DIRECT LOG: Missing extraction_id in request data: {data}", flush=True)
            logger.error(f"Missing extraction_id in request data: {data}")
            # Acknowledge invalid messages to prevent reprocessing
            await msg.ack()
            return

        try:
            print(f"DIRECT LOG: Extracting and reformatting summary for extraction_id: {extraction_id}", flush=True)
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

            print(f"DIRECT LOG: Sanitizing summary data for decision number: {decision_number}", flush=True)
            logger.info(f"Sanitizing summary data for decision number: {decision_number}")
            summary_text = sanitize_markdown_symbol(summary)
            translated_summary_text = sanitize_markdown_symbol(translated_summary)

            print(f"DIRECT LOG: Updating database with summary for decision number: {decision_number}", flush=True)
            logger.info(f"Updating database with summary for decision number: {decision_number}")
            await write_summary_to_db(
                case_db_engine=contexts.case_db_engine,
                decision_number=decision_number,
                summary=summary,
                summary_text=summary_text,
                translated_summary=translated_summary,
                translated_summary_text=translated_summary_text,
            )

            print(f"DIRECT LOG: Successfully processed summarization for decision number: {decision_number}", flush=True)
            logger.info(f"Successfully processed summarization for decision number: {decision_number}")
            # Acknowledge successful processing
            await msg.ack()
        except Exception as e:
            print(f"DIRECT LOG: Failed to process summarization for {extraction_id}: {str(e)}", flush=True)
            logger.error(f"Failed to process summarization for {extraction_id}: {str(e)}")
            # For processing errors, we should not ack (let NATS retry delivery)
            # But if we're using max_deliver=2 in config, we should ack to prevent infinite retries
            await msg.ack()
    except json.JSONDecodeError as e:
        print(f"DIRECT LOG: Invalid JSON in message: {e}", flush=True)
        logger.error(f"Invalid JSON in message: {e}")
        # Acknowledge invalid messages to prevent reprocessing
        await msg.ack()
    except Exception as e:
        print(f"DIRECT LOG: Critical error in generate_summary: {str(e)}", flush=True)
        logger.error(f"Critical error in generate_summary: {str(e)}")
        # Log the message content in case of critical failures
        try:
            print(f"DIRECT LOG: Message content: {msg.data.decode()}", flush=True)
            logger.error(f"Message content: {msg.data.decode()}")
        except:
            print("DIRECT LOG: Could not decode message data", flush=True)
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
