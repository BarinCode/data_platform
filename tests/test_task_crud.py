import uuid
from datetime import datetime

import pytest

from apps.models.task import Task
from apps.schemas.task import TaskCreateSchema
from apps.services import task as task_crud


@pytest.mark.anyio
async def test_get_tasks(db_session):
    work_id = uuid.uuid4().hex
    for index in range(1, 20):
        payload = TaskCreateSchema(**{
            "name": f"testtask-{index}",
            "work_id": work_id,
            "status": 1,
            "started_at": datetime.now()
        })
        await task_crud.create(payload, db_session)

    tasks, total = await task_crud.list(work_id, 5, 8, db_session)
    for index, task in enumerate(tasks, start=6):
        assert task.name == f"testtask-{index}"
    assert len(tasks) == 8
    assert total == 19


@pytest.mark.anyio
async def test_get_task(db_session):
    work_id = uuid.uuid4().hex
    payload = TaskCreateSchema(**{
        "work_id": work_id,
        "name": "testtask",
        "status": 1,
        "started_at": datetime.now()
    })
    task_1: Task = await task_crud.create(payload, db_session)
    task_2: Task = await task_crud.get(work_id, task_1.task_id, db_session)
    assert isinstance(task_2, Task)
