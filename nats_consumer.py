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
        level=logging.DEBUG,
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
    if not isinstance(error, nats.errors.SlowConsumerError):
        logger.warning(f"NATS client got error: {error}")
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

    # Use different consumer names for each instance to avoid conflicts
    for i in range(num_of_consumer_instances):
        consumer_name = f"{DURABLE_NAME}_{i+1}"
        logger.debug(f"Creating consumer #{i+1} with name: {consumer_name}")

        # Clone consumer config for this instance
        instance_config = ConsumerConfig(
            filter_subject=consumer_config.filter_subject,
            durable_name=consumer_name,
            ack_wait=consumer_config.ack_wait,
            max_deliver=consumer_config.max_deliver,
            max_ack_pending=consumer_config.max_ack_pending,
        )

        task = asyncio.create_task(
            run_pull_job_consumer_improved(
                nats_client=nats_client,
                jetstream_client=jetstream_client,
                consumer_config=instance_config,
                processing_func=processing_func,
                consumer_id=i+1,
            )
        )

        logger.debug(f"Created task for consumer {consumer_name}: {task}")
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
    consumer_name = f"consumer-{consumer_id}"
    logger = logging.getLogger(f"nats-{consumer_name}")

    # Setup subscription with proper error handling
    retry_count = 0
    max_retries = 10
    base_retry_delay = 1

    logger.info(f"Starting NATS {consumer_name} for subject {consumer_config.filter_subject}")

    while True:
        try:
            if not nats_client.is_connected:
                logger.warning(f"{consumer_name}: NATS client disconnected, reconnecting...")
                nats_client = await initialize_nats()
                jetstream_client = nats_client.jetstream()

                # Ensure stream exists
                stream_configs = generate_nats_stream_configs()
                await initialize_jetstream_client(
                    nats_client=nats_client,
                    stream_configs=stream_configs,
                )

            # Create a pull subscription
            logger.info(f"{consumer_name}: Creating pull subscriber for {consumer_config.filter_subject} with durable {consumer_config.durable_name}")

            try:
                # Get existing consumer info
                consumer_info = await jetstream_client.consumer_info(
                    STREAM_NAME, consumer_config.durable_name
                )
                logger.debug(f"{consumer_name}: Found existing consumer: {consumer_info}")
            except Exception as e:
                if "consumer not found" in str(e).lower():
                    logger.debug(f"{consumer_name}: Consumer {consumer_config.durable_name} not found, will be created")
                else:
                    logger.warning(f"{consumer_name}: Error getting consumer info: {e}")

            subscription = await jetstream_client.pull_subscribe(
                subject=consumer_config.filter_subject,
                durable=consumer_config.durable_name,
                config=consumer_config,
            )

            # Reset retry count on success
            retry_count = 0

            logger.info(f"{consumer_name}: Running pull-based subscription for {consumer_config.filter_subject}")

            # Process messages in a loop
            while nats_client.is_connected:
                try:
                    # Fetch messages in batches
                    logger.debug(f"{consumer_name}: Fetching messages...")
                    msgs = await subscription.fetch(batch=10, timeout=1)
                    if msgs:
                        logger.info(f"{consumer_name}: Received {len(msgs)} messages")
                        for msg in msgs:
                            try:
                                logger.debug(f"{consumer_name}: Processing message: {msg.data[:100]}...")
                                await processing_func(msg)
                            except Exception as proc_err:
                                logger.error(f"{consumer_name}: Error processing message: {proc_err}")
                                # Only acknowledge explicitly processed messages
                                await msg.ack()
                    else:
                        logger.debug(f"{consumer_name}: No messages received in fetch call")
                except asyncio.TimeoutError:
                    # This is expected, just continue polling
                    logger.debug(f"{consumer_name}: Fetch timeout (normal)")
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"{consumer_name}: Error in message batch processing: {e}")
                    await asyncio.sleep(1)

            # If we get here, the connection was lost
            logger.warning(f"{consumer_name}: NATS connection lost")
            try:
                await subscription.unsubscribe()
            except:
                pass

        except Exception as e:
            retry_count += 1
            # Calculate exponential backoff delay
            retry_delay = min(base_retry_delay * (2 ** (retry_count - 1)), 30)

            logger.error(f"{consumer_name}: Subscription error: {e}, retrying in {retry_delay}s (attempt {retry_count}/{max_retries})")

            if retry_count >= max_retries:
                logger.error(f"{consumer_name}: Reached maximum retry attempts ({max_retries}), waiting longer")
                await asyncio.sleep(60)
                retry_count = 0
            else:
                await asyncio.sleep(retry_delay)


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
