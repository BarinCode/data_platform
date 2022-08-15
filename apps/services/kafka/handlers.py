import json
from typing import Dict

from loguru import logger
from sqlalchemy.orm import Session

from apps.database import get_db_session
from apps.models import Task


def store_task_data_to_mysql(kafka_message: bytes):
    db_session: Session = next(get_db_session())

    kafka_message_dict: Dict = json.loads(kafka_message)
    task_id = kafka_message_dict.get("task_id")
    status = kafka_message_dict.get("status")

    if not task_id or not status:
        return False

    logger.debug(f"kafka: {task_id} -> {status}")
    task = db_session.query(Task)\
        .filter(Task.resource_task_id == task_id)\
        .first()
    if not task:
        return

    # TODO: nested statemachine
    if status == "Pending":
        task.systemically_start()
    elif status == "Running":
        task.run()
    elif status == "Failed":
        task.fail()
    elif status == "Succeeded":
        task.succeed()

    db_session.add(task)
    db_session.commit()
    db_session.refresh(task)
