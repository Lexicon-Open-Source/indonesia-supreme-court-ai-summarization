import asyncio
import logging
import sys
from collections.abc import Callable

import nats
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, StreamConfig
from nats.js.errors import NotFoundError

from settings import get_settings

STREAM_NAME = "SUPREME_COURT_SUMMARIZATION_EVENT"
STREAM_SUBJECTS = f"{STREAM_NAME}.>"
SUBJECT = f"{STREAM_NAME}.summarize"
DURABLE_NAME = "SUPREME_COURT_SUMMARIZATION"
DEFAULT_WAIT_TIME_PER_PROCESS = 3600
DEFAULT_TIMEOUT_INTERVAL = 30
DEFAULT_WAIT_TIME_FOR_NEXT_FETCH = 1
PENDING_MSG_LIMIT = 1

CONSUMER_CONFIG = ConsumerConfig(
    filter_subject=STREAM_SUBJECTS,
    durable_name=DURABLE_NAME,
    ack_wait=DEFAULT_WAIT_TIME_PER_PROCESS,
    max_deliver=2,
    max_ack_pending=3,
)
STREAM_CONFIG = StreamConfig(name=STREAM_NAME, subjects=[STREAM_SUBJECTS])


async def initialize_nats() -> NATS:
    """
    Initialize the NATS client and connect to the NATS server.

    Returns:
        NATS: The initialized and connected NATS client.
    """
    nats_client = NATS()
    await nats_client.connect(
        get_settings().nats__url,
        error_cb=error_callback,
    )

    return nats_client


def generate_nats_stream_configs() -> list[StreamConfig]:
    """
    Generate the NATS stream configurations.

    Returns:
        list[StreamConfig]:
            A list of StreamConfig objects
                representing the NATS stream configurations.
    """
    return [
        STREAM_CONFIG,
    ]


async def initialize_jetstream_client(
    nats_client: NATS, stream_configs: list[StreamConfig]
) -> JetStreamContext:
    """
    Asynchronously initializes a Jetstream client.
    Args:
        nats_client (NATS):
            The initialized and connected NATS client.
        stream_configs (list[StreamConfig]):
            The list of configuration for the JetStream stream

    Returns:
        JetStreamContext:
            The upserted JetStream client.
    """
    jetstream_client = nats_client.jetstream()
    for stream_config in stream_configs:
        await upsert_jetstream_client(
            jetstream_client=jetstream_client,
            stream_config=stream_config,
        )

    return jetstream_client


async def upsert_jetstream_client(
    jetstream_client: JetStreamContext, stream_config: StreamConfig
) -> JetStreamContext:
    """
    Upsert a JetStream client by updating the stream configuration
        or adding a new stream if it does not exist.

    Args:
        jetstream_client (JetStreamContext):
            The JetStream client to upsert.
        stream_config (StreamConfig):
            The stream configuration to update or add.

    Returns:
        JetStreamContext:
            The updated or newly created JetStream client.
    """
    try:
        await jetstream_client.update_stream(config=stream_config)
    except NotFoundError:
        await jetstream_client.add_stream(config=stream_config)
    except Exception as err:
        logging.error(f"error when upserting jetstream client: {err}")

    return jetstream_client


async def error_callback(error: Exception) -> None:
    """
    An asynchronous callback function that handles errors.

    Args:
        error: The error that occurred.
    """
    if not isinstance(error, nats.errors.SlowConsumerError):
        error_msg = f"NATS client got error: {error}"
        logging.warning(error_msg)

        await asyncio.sleep(DEFAULT_TIMEOUT_INTERVAL)


def create_job_consumer_async_task(
    nats_client: NATS,
    jetstream_client: JetStreamContext,
    consumer_config: ConsumerConfig,
    processing_func: Callable,
    num_of_consumer_instances: int = 1,
) -> list[asyncio.Task]:
    """
    Asynchronously creates multiple job consumer tasks.

    Args:
        nats_client (NATS): NATS client.
        jetstream_client (JetStreamContext): JetStream context
        consumer_config (ConsumerConfig):
            The configuration for the consumer.
        processing_func (callable):
            The function to be executed for each job.
        num_of_consumer_instances (int):
            The number of consumer instances to create.

    Returns:
        List[asyncio.Task]:
            A list of asyncio tasks representing the consumer job connections.
    """
    nats_consumer_job_connection = []
    for _ in range(num_of_consumer_instances):
        nats_consumer_job_connection.append(
            asyncio.create_task(
                run_job_consumer(
                    nats_client=nats_client,
                    jetstream_client=jetstream_client,
                    consumer_config=consumer_config,
                    processing_func=processing_func,
                )
            ),
        )

    return nats_consumer_job_connection


async def run_job_consumer(
    nats_client: NATS,
    jetstream_client: JetStreamContext,
    consumer_config: ConsumerConfig,
    processing_func: Callable,
    fetch_job_batch_size: int = 1,
    wait_time_for_next_fetch: float = DEFAULT_WAIT_TIME_FOR_NEXT_FETCH,
) -> None:
    """
    Run the index pinvalue comparables job subscriber.

    Args:
        context (AppContexts):
            The context containing necessary information
                for the job execution.

    Returns:
        None
    """
    job_consumer = await create_pull_job_consumer(jetstream_client, consumer_config)

    while True:
        try:
            if not nats_client.is_connected:
                nats_client = await initialize_nats()

                stream_configs = generate_nats_stream_configs()
                jetstream_client = await initialize_jetstream_client(
                    nats_client=nats_client,
                    stream_configs=stream_configs,
                )

                job_consumer = await create_pull_job_consumer(
                    jetstream_client, consumer_config
                )

            msgs = await job_consumer.fetch(fetch_job_batch_size)
            for msg in msgs:
                await processing_func(msg)

        except asyncio.TimeoutError:
            await asyncio.sleep(wait_time_for_next_fetch)

        except Exception as e:
            logging.warning(f"Unknown err: {e}")
            await asyncio.sleep(wait_time_for_next_fetch)


async def create_pull_job_consumer(
    jetstream_client: JetStreamContext,
    consumer_config: ConsumerConfig,
) -> JetStreamContext.PullSubscription:
    """
    Initialize the job stream client and consumer.

    Args:
        jetstream_client (JetStreamContext):
            The JetStream client.
        consumer_config (ConsumerConfig):
            The consumer configuration.

    Returns:
        JetStreamContext.PullSubscription:
            A JobConsumer representing the pull subscription.
    """
    job_consumer = await jetstream_client.pull_subscribe(
        subject=consumer_config.filter_subject,
        durable=consumer_config.durable_name,
        config=consumer_config,
        pending_msgs_limit=PENDING_MSG_LIMIT,
    )
    print(f"Running {consumer_config.filter_subject} job subscriber..")
    sys.stdout.flush()

    return job_consumer


async def close_nats_connection(connection_task: asyncio.Task) -> None:
    """
    Closes a NATS connection.

    Args:
        connection_task (asyncio.Task): The task representing the connection.

    Returns:
        None.
    """
    # Perform connection cleanup here
    await asyncio.sleep(2)  # Simulating connection cleanup time
    connection_task.cancel()
