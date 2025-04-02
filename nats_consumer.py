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
DEFAULT_TIMEOUT_INTERVAL = 5
DEFAULT_WAIT_TIME_FOR_NEXT_FETCH = 0
PENDING_MSG_LIMIT = 5

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
    use_push_subscription: bool = True,  # Default is now ignored, we always use pull
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
        use_push_subscription (bool):
            Legacy parameter, now ignored as we always use pull-based subscription.

    Returns:
        List[asyncio.Task]:
            A list of asyncio tasks representing the consumer job connections.
    """
    nats_consumer_job_connection = []
    for _ in range(num_of_consumer_instances):
        nats_consumer_job_connection.append(
            asyncio.create_task(
                run_pull_job_consumer_improved(
                    nats_client=nats_client,
                    jetstream_client=jetstream_client,
                    consumer_config=consumer_config,
                    processing_func=processing_func,
                )
            ),
        )

    return nats_consumer_job_connection


async def run_pull_job_consumer_improved(
    nats_client: NATS,
    jetstream_client: JetStreamContext,
    consumer_config: ConsumerConfig,
    processing_func: Callable,
) -> None:
    """
    Run a pull-based job consumer for reliable message processing.

    Args:
        nats_client (NATS): NATS client.
        jetstream_client (JetStreamContext): JetStream context
        consumer_config (ConsumerConfig): The configuration for the consumer.
        processing_func (callable): The function to be executed for each job.

    Returns:
        None
    """
    # Handle message callback
    async def message_handler(msg):
        try:
            await processing_func(msg)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Only negative acknowledge on processing errors
            try:
                await msg.nak()
            except:
                logging.error("Failed to NAK message")

    # Setup subscription with proper error handling
    retry_count = 0
    max_retries = 10
    base_retry_delay = 1  # Start with 1 second

    while True:
        try:
            if not nats_client.is_connected:
                nats_client = await initialize_nats()
                jetstream_client = nats_client.jetstream()

                # Ensure stream exists
                stream_configs = generate_nats_stream_configs()
                await initialize_jetstream_client(
                    nats_client=nats_client,
                    stream_configs=stream_configs,
                )

            # Instead of messing with consumers, use a simple pull subscription
            logging.info(f"Creating pull subscriber for {consumer_config.filter_subject}")
            subscription = await jetstream_client.pull_subscribe(
                subject=consumer_config.filter_subject,
                durable=consumer_config.durable_name,
                config=consumer_config,
            )

            # Reset retry count on success
            retry_count = 0

            logging.info(f"Running pull-based subscription for {consumer_config.filter_subject}")

            # Process messages in a loop
            while nats_client.is_connected:
                try:
                    # Fetch messages in batches
                    msgs = await subscription.fetch(batch=10, timeout=1)
                    if msgs:
                        await asyncio.gather(*[processing_func(msg) for msg in msgs])
                except asyncio.TimeoutError:
                    # No messages, continue polling
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logging.error(f"Error processing message batch: {e}")
                    await asyncio.sleep(1)

            # If we get here, the connection was lost
            try:
                await subscription.unsubscribe()
            except:
                pass

        except Exception as e:
            retry_count += 1
            # Calculate exponential backoff delay (1s, 2s, 4s, 8s, etc.)
            retry_delay = min(base_retry_delay * (2 ** (retry_count - 1)), 30)

            logging.error(f"Subscription error: {e}, retrying in {retry_delay}s (attempt {retry_count}/{max_retries})")

            if retry_count >= max_retries:
                logging.error(f"Reached maximum retry attempts ({max_retries}), waiting longer before next attempt")
                await asyncio.sleep(60)  # Wait a minute before resetting retry count
                retry_count = 0
            else:
                await asyncio.sleep(retry_delay)  # Exponential backoff


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
