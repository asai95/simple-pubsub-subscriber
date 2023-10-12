import logging
import os
import time

from dotenv import load_dotenv

from subscriber import pull_json_messages

load_dotenv()


logging.basicConfig(level=logging.INFO)


def imitate_load(secs: int) -> None:
    time.sleep(secs)


if __name__ == '__main__':
    project = os.getenv("PROJECT_ID")
    if not project:
        raise ValueError("PROJECT_ID environment variable is not set.")
    if not os.getenv("SUB_NAME"):
        raise ValueError("SUB_NAME environment variable is not set.")
    subscription = os.getenv("SUB_NAME")
    secs = int(os.getenv("PROCESSING_TIME", 10))
    max_messages = int(os.getenv("MAX_MESSAGES", 1))
    key_file_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    pull_json_messages(
        subscription_name=subscription,
        project_name=project,
        callback=lambda _: imitate_load(secs),
        max_messages=max_messages,
        service_account_key_path=key_file_path,
    )