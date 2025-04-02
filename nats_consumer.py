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
    deliver_group=f"{DURABLE_NAME}_QUEUE",
    deliver_subject=f"{DURABLE_NAME}.PUSH",
    flow_control=True,
    idle_heartbeat=5.0,  # Add 5-second heartbeat for flow control
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
    use_push_subscription: bool = True,  # Default to push-based subscription
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
            Whether to use push-based subscription (True) or pull-based (False).

    Returns:
        List[asyncio.Task]:
            A list of asyncio tasks representing the consumer job connections.
    """
    nats_consumer_job_connection = []
    for _ in range(num_of_consumer_instances):
        if use_push_subscription:
            nats_consumer_job_connection.append(
                asyncio.create_task(
                    run_push_job_consumer(
                        nats_client=nats_client,
                        jetstream_client=jetstream_client,
                        consumer_config=consumer_config,
                        processing_func=processing_func,
                    )
                ),
            )
        else:
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
    fetch_job_batch_size: int = 10,
    wait_time_for_next_fetch: float = DEFAULT_WAIT_TIME_FOR_NEXT_FETCH,
) -> None:
    """
    Run the job consumer to process messages.

    Args:
        nats_client (NATS): NATS client.
        jetstream_client (JetStreamContext): JetStream context
        consumer_config (ConsumerConfig): The configuration for the consumer.
        processing_func (callable): The function to be executed for each job.
        fetch_job_batch_size (int): Number of messages to fetch in a batch.
        wait_time_for_next_fetch (float): Time to wait between fetches.

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
            if msgs:
                # Process messages concurrently
                await asyncio.gather(*[processing_func(msg) for msg in msgs])
            else:
                # No messages, small wait to avoid CPU spin
                await asyncio.sleep(0.01)

        except asyncio.TimeoutError:
            # Just continue to next iteration with minimal delay
            continue

        except Exception as e:
            logging.warning(f"Unknown err: {e}")
            await asyncio.sleep(1)  # Shorter retry delay on errors


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


async def run_push_job_consumer(
    nats_client: NATS,
    jetstream_client: JetStreamContext,
    consumer_config: ConsumerConfig,
    processing_func: Callable,
) -> None:
    """
    Run a push-based job consumer for immediate message processing.

    Args:
        nats_client (NATS): NATS client.
        jetstream_client (JetStreamContext): JetStream context
        consumer_config (ConsumerConfig): The configuration for the consumer.
        processing_func (callable): The function to be executed for each job.

    Returns:
        None
    """
    # Use a queue group to load balance across multiple subscribers
    queue_group = consumer_config.deliver_group

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

            # First, try to delete existing consumer if it exists
            try:
                await jetstream_client.delete_consumer(STREAM_NAME, consumer_config.durable_name)
                logging.info(f"Deleted existing consumer: {consumer_config.durable_name}")
            except Exception as e:
                if "not found" not in str(e).lower():
                    logging.warning(f"Error deleting consumer: {e}")

            # Explicitly create the consumer before subscribing
            try:
                logging.info(f"Creating consumer {consumer_config.durable_name} on stream {STREAM_NAME}")
                await jetstream_client.add_consumer(
                    stream=STREAM_NAME,
                    config=consumer_config
                )
                logging.info(f"Consumer {consumer_config.durable_name} created successfully")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logging.error(f"Error creating consumer: {e}")
                    raise

            # Create a JetStream push consumer with a queue for load balancing
            subscription = await jetstream_client.subscribe(
                subject=consumer_config.filter_subject,
                queue=queue_group,
                durable=consumer_config.durable_name,
                cb=message_handler,
                manual_ack=True,  # We'll handle acknowledgment in the processing function
                config=consumer_config,
            )

            # Reset retry count on success
            retry_count = 0

            print(f"Running push-based subscription for {consumer_config.filter_subject}..")
            sys.stdout.flush()

            # Keep the subscription active
            while nats_client.is_connected:
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

            logging.error(f"Push subscription error: {e}, retrying in {retry_delay}s (attempt {retry_count}/{max_retries})")

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
