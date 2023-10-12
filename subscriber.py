import json
import logging
from typing import Callable, Any

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message

logger = logging.getLogger(__name__)


def pull_json_messages(
        subscription_name: str,
        project_name: str,
        callback: Callable[[dict[str, Any]], None],
        max_messages: int = 1,
        service_account_key_path: str = None,
) -> None:
    """Simple blocking pull subscriber for json messages."""
    if service_account_key_path is not None:
        subscriber = pubsub_v1.SubscriberClient.from_service_account_json(service_account_key_path)
    else:
        subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_name, subscription_name)

    def callback_wrapper(message: Message):
        logger.info(f"Received message {message.message_id}.")
        try:
            callback(json.loads(message.data))
            message.ack()
        except Exception as e:
            message.nack()
            logger.error(f"Message {message.message_id} failed with exception.")
            logger.exception(e)
        logger.info(f"Message {message.message_id} finished successfully.")
        message.ack()

    flow_control = pubsub_v1.types.FlowControl(max_messages=max_messages)
    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback_wrapper,
        flow_control=flow_control,
    )
    streaming_pull_future.result()
