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

# Configure root logger if not configured
if not logging.root.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

logger = logging.getLogger("nats-consumer")

CONSUMER_CONFIG = ConsumerConfig(
    filter_subject=SUBJECT,  # Changed from STREAM_SUBJECTS to exact subject
    durable_name=DURABLE_NAME,
    ack_wait=DEFAULT_WAIT_TIME_PER_PROCESS,
    max_deliver=2,
    max_ack_pending=3,
)
STREAM_CONFIG = StreamConfig(name=STREAM_NAME, subjects=[STREAM_SUBJECTS])


async def initialize_nats() -> NATS:
    """
    Initialize the NATS client and connect to the NATS server with retries.
    """
    max_retries = 5
    retry_delay = 2
    retry_count = 0

    print(f"DIRECT LOG: Initializing NATS client with URL: {get_settings().nats__url}", flush=True)

    while retry_count < max_retries:
        try:
            print(f"DIRECT LOG: Connecting to NATS, attempt {retry_count+1}/{max_retries}", flush=True)
            logger.debug(f"Initializing NATS client with URL: {get_settings().nats__url}, attempt {retry_count+1}/{max_retries}")
            nats_client = NATS()
            await nats_client.connect(
                get_settings().nats__url,
                error_cb=error_callback,
                reconnect_time_wait=2,
                max_reconnect_attempts=10,
                connect_timeout=10,
            )
            print("DIRECT LOG: NATS client initialized and connected successfully", flush=True)
            logger.debug("NATS client initialized and connected successfully")
            return nats_client
        except Exception as e:
            retry_count += 1
            print(f"DIRECT LOG: Failed to connect to NATS (attempt {retry_count}/{max_retries}): {str(e)}", flush=True)
            logger.error(f"Failed to connect to NATS (attempt {retry_count}/{max_retries}): {str(e)}")
            if retry_count < max_retries:
                print(f"DIRECT LOG: Retrying in {retry_delay} seconds...", flush=True)
                logger.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print("DIRECT LOG: Max retries reached. Could not connect to NATS server.", flush=True)
                logger.error("Max retries reached. Could not connect to NATS server.")
                raise


def generate_nats_stream_configs() -> list[StreamConfig]:
    """
    Generate the NATS stream configurations.

    Returns:
        list[StreamConfig]:
            A list of StreamConfig objects
                representing the NATS stream configurations.
    """
    logger.debug(f"Generating NATS stream config for {STREAM_NAME}")
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
    logger.debug("Initializing JetStream client")
    jetstream_client = nats_client.jetstream()
    for stream_config in stream_configs:
        await upsert_jetstream_client(
            jetstream_client=jetstream_client,
            stream_config=stream_config,
        )

    logger.debug("JetStream client initialized")
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
    logger.debug(f"Upserting JetStream stream: {stream_config.name}")
    try:
        logger.debug(f"Attempting to update stream {stream_config.name}")
        await jetstream_client.update_stream(config=stream_config)
        logger.debug(f"Stream {stream_config.name} updated successfully")
    except NotFoundError:
        logger.debug(f"Stream {stream_config.name} not found, creating new stream")
        await jetstream_client.add_stream(config=stream_config)
        logger.debug(f"Stream {stream_config.name} created successfully")
    except Exception as err:
        logger.error(f"Error when upserting jetstream client: {err}")

    return jetstream_client


async def error_callback(error: Exception) -> None:
    """
    An asynchronous callback function that handles errors.

    Args:
        error: The error that occurred.
    """
    if isinstance(error, nats.errors.SlowConsumerError):
        logger.warning(f"NATS slow consumer error (can be normal during heavy processing): {error}")
    else:
        logger.warning(f"NATS client got error: {error}")

    # Always sleep a bit to prevent tight error loops
    await asyncio.sleep(DEFAULT_TIMEOUT_INTERVAL)


def create_job_consumer_async_task(
    nats_client: NATS,
    jetstream_client: JetStreamContext,
    consumer_config: ConsumerConfig,
    processing_func: Callable,
    num_of_consumer_instances: int = 1,
    use_push_subscription: bool = False,  # Default to pull now
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
            Whether to use push-based subscription instead of pull-based.

    Returns:
        List[asyncio.Task]:
            A list of asyncio tasks representing the consumer job connections.
    """
    logger.debug(f"Creating {num_of_consumer_instances} job consumer tasks, use_push_subscription={use_push_subscription}")
    nats_consumer_job_connection = []

    # Create a single durable consumer with a delivery group
    # All instances will work as a load-balanced group
    consumer_name = DURABLE_NAME
    logger.debug(f"Creating shared consumer with name: {consumer_name}")

    # Create a single consumer config with a delivery group
    shared_config = ConsumerConfig(
        filter_subject=consumer_config.filter_subject,
        durable_name=consumer_name,
        deliver_group="summarization_workers",  # This is key for load balancing
        ack_wait=consumer_config.ack_wait,
        max_deliver=consumer_config.max_deliver,
        max_ack_pending=consumer_config.max_ack_pending,
    )

    # Use different consumer IDs for each instance to help with logging
    for i in range(num_of_consumer_instances):
        consumer_id = i+1
        logger.debug(f"Creating consumer task #{consumer_id} using shared consumer: {consumer_name}")

        task = asyncio.create_task(
            run_pull_job_consumer_improved(
                nats_client=nats_client,
                jetstream_client=jetstream_client,
                consumer_config=shared_config,
                processing_func=processing_func,
                consumer_id=consumer_id,
            )
        )

        logger.debug(f"Created task for consumer worker {consumer_id}: {task}")
        nats_consumer_job_connection.append(task)

    logger.debug(f"Created {len(nats_consumer_job_connection)} consumer tasks")
    return nats_consumer_job_connection


async def run_pull_job_consumer_improved(
    nats_client: NATS,
    jetstream_client: JetStreamContext,
    consumer_config: ConsumerConfig,
    processing_func: Callable,
    consumer_id: int = 0,
) -> None:
    """
    Run a pull-based job consumer for reliable message processing.

    Args:
        nats_client (NATS): NATS client.
        jetstream_client (JetStreamContext): JetStream context
        consumer_config (ConsumerConfig): The configuration for the consumer.
        processing_func (callable): The function to be executed for each job.
        consumer_id (int): Identifier for this consumer instance

    Returns:
        None
    """
    worker_name = f"worker-{consumer_id}"
    logger.info(f"Starting {worker_name} using pull-based subscription with shared consumer config: {consumer_config.durable_name}")

    # Initialization of local variables
    consumerInfo = None
    max_reconnect_attempts = 5
    reconnect_delay = 2  # seconds
    reconnect_attempts = 0

    while True:
        try:
            if not nats_client.is_connected:
                logger.warning(f"{worker_name} NATS client disconnected, attempting to reconnect...")
                try:
                    # Attempt to reconnect if disconnected
                    if reconnect_attempts < max_reconnect_attempts:
                        reconnect_attempts += 1
                        logger.info(f"{worker_name} Reconnecting to NATS (attempt {reconnect_attempts}/{max_reconnect_attempts})")
                        await nats_client.connect(
                            get_settings().nats__url,
                            error_cb=error_callback,
                            reconnect_time_wait=2,
                            max_reconnect_attempts=10,
                            connect_timeout=10,
                        )
                        # Get a new jetstream client after reconnection
                        jetstream_client = nats_client.jetstream()
                        logger.info(f"{worker_name} Successfully reconnected to NATS")
                        reconnect_attempts = 0  # Reset the counter on successful reconnection
                    else:
                        logger.error(f"{worker_name} Max reconnection attempts reached. Sleeping before retry...")
                        await asyncio.sleep(30)  # Longer delay before trying to reconnect again
                        reconnect_attempts = 0  # Reset counter to try again
                        continue
                except Exception as reconnect_error:
                    logger.error(f"{worker_name} Failed to reconnect to NATS: {reconnect_error}")
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 30)  # Exponential backoff capped at 30 seconds
                    continue

            # Check/add consumer if it doesn't exist
            try:
                consumerInfo = await jetstream_client.consumer_info(
                    stream=consumer_config.filter_subject.split('.')[0],
                    consumer=consumer_config.durable_name,
                )
                logger.debug(f"{worker_name} Shared consumer exists: {consumer_config.durable_name}")
            except nats.js.errors.NotFoundError:
                logger.info(f"{worker_name} Creating shared consumer: {consumer_config.durable_name}")
                consumerInfo = await jetstream_client.add_consumer(
                    stream=consumer_config.filter_subject.split('.')[0],
                    config=consumer_config
                )
                logger.info(f"{worker_name} Created shared consumer: {consumer_config.durable_name}")
            except Exception as consumer_error:
                logger.error(f"{worker_name} Error checking/creating consumer: {consumer_error}")
                await asyncio.sleep(2)
                continue

            # Fetch and process messages with improved error handling
            try:
                # Use pull_subscribe instead of fetch
                logger.debug(f"{worker_name} Setting up pull subscription...")
                subscription = await jetstream_client.pull_subscribe(
                    subject=consumer_config.filter_subject,
                    durable=consumer_config.durable_name,
                    config=consumer_config,  # Pass the full config to ensure deliver_group is used
                )

                # Pull messages in batches
                logger.debug(f"{worker_name} Pulling batch of messages (max 1)...")
                messages = await subscription.fetch(batch=1, timeout=30)

                msg_count = len(messages)
                for msg in messages:
                    # Use a safer way to log the message ID that works across versions
                    # In newer versions, the sequence number is available differently
                    try:
                        if hasattr(msg, 'metadata') and hasattr(msg.metadata, 'sequence'):
                            msg_id = msg.metadata.sequence
                        elif hasattr(msg, 'metadata') and hasattr(msg.metadata, 'stream_seq'):
                            msg_id = msg.metadata.stream_seq
                        else:
                            msg_id = "unknown"
                        logger.info(f"{worker_name} Processing message {msg_id}")
                    except Exception as e:
                        logger.info(f"{worker_name} Processing message (could not determine sequence: {e})")

                    # Process the message with timeout protection
                    try:
                        # We don't use a timeout here because the processing function should handle its own timeouts
                        await processing_func(msg)
                        logger.info(f"{worker_name} Successfully processed message")
                    except Exception as processing_error:
                        logger.error(f"{worker_name} Error processing message: {processing_error}")
                        # The processing function should handle its own ack/nack

                if msg_count == 0:
                    # No messages received, wait briefly before fetching again
                    logger.debug(f"{worker_name} No messages received, waiting before next fetch")
                    await asyncio.sleep(1)

            except nats.errors.TimeoutError:
                logger.debug(f"{worker_name} Timeout waiting for messages, will retry")
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                logger.info(f"{worker_name} Task was cancelled, exiting")
                raise
            except Exception as fetch_error:
                logger.error(f"{worker_name} Error fetching messages: {fetch_error}")
                await asyncio.sleep(3)  # Wait before retry

        except asyncio.CancelledError:
            logger.info(f"{worker_name} Task was cancelled, exiting")
            raise
        except Exception as e:
            logger.error(f"{worker_name} Unexpected error in consumer loop: {e}")
            await asyncio.sleep(5)  # Wait before restarting the loop


async def close_nats_connection(connection_task: asyncio.Task) -> None:
    """
    Closes a NATS connection.

    Args:
        connection_task (asyncio.Task): The task representing the connection.

    Returns:
        None.
    """
    logger.debug(f"Closing NATS connection task: {connection_task}")
    # Gracefully handle cancellation
    connection_task.cancel()
    try:
        await connection_task
    except asyncio.CancelledError:
        logger.debug("Task cancelled successfully")
    except Exception as e:
        logger.error(f"Error during task cancellation: {e}")

    logger.debug("NATS connection closed")
