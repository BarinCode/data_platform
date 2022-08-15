import os
from typing import Dict

from celery.result import AsyncResult
from clickhouse_driver import Client
from httpx import (
    AsyncClient,
    RemoteProtocolError,
    ConnectTimeout,
    ConnectError,
    ReadTimeout,
)
from loguru import logger
from sqlalchemy.orm.session import Session

from apps.config import settings
from apps.core.enums import TaskTypeEnum
from apps.core.errors import errors
from apps.models import Task
from apps.models.analysis import Analysis
from apps.models.connection import Connection
from apps.schemas import GeneralResponse, get_error_response
from apps.schemas.task import TaskCreateSchema
from apps.services.work import get_common_work_by_id


def get_task_by_id(work_id: int, task_id: int, db_session: Session):
    return db_session\
        .query(Task)\
        .filter(Task.work_id == work_id)\
        .filter(Task.task_id == task_id)\
        .first()


async def create(payload: TaskCreateSchema, db_session: Session):
    task = Task(**payload.dict())

    db_session.add(task)
    db_session.commit()
    db_session.refresh(task)

    return GeneralResponse(code=0, data=task)


async def list(
    work_id: int,
    limit: int,
    offset: int,
    task_type: TaskTypeEnum,
    is_temporary: bool,
    db_session: Session,
):
    tasks_query = db_session.query(Task)\
        .filter(Task.work_id == work_id)\
        .filter(Task.task_type == task_type)\
        .filter(Task.is_temporary == is_temporary)\
        .order_by(Task.started_at.desc())

    items = tasks_query.offset(offset).limit(limit).all()
    total = tasks_query.count()

    data = {
        "items": items,
        "total": total,
    }

    return GeneralResponse(code=0, data=data)


async def get(work_id: int, task_id: int, db_session: Session):
    task = get_task_by_id(work_id, task_id, db_session)
    if not task:
        return get_error_response(400208)

    # rebuild nodes
    task.nodes = []

    return GeneralResponse(code=0, data=task)


async def get_log(work_id: int, task_id: int, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    task = get_task_by_id(work_id, task_id, db_session)
    if not task or not work:
        return get_error_response(400209)

    task_log_file = os.path.join(
        work.work_directory,
        task.resource_task_id,
        f"{task.resource_task_id}.log"
    )

    if not os.path.exists(task_log_file):
        return get_error_response(400210)

    all_lines = []
    with open(task_log_file, "r") as file_handler:
        all_lines = file_handler.readlines()

    data = [line.strip() for line in all_lines]

    return GeneralResponse(code=0, data=data)


async def get_all_file(work_id: int, task_id: int, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    task = get_task_by_id(work_id, task_id, db_session)
    if not task or not work:
        return get_error_response(400209)

    task_path = os.path.join(work.work_directory, task.resource_task_id)
    if os.path.exists(task_path):
        return get_error_response(400211)

    data = []
    for file_name in os.listdir(task_path):
        relative_url = f"works/{work_id}/tasks/{task_id}/download"
        download_url = os.path.join(
            settings.work_service,
            relative_url,
            file_name,
        )
        task_result = {
            "file_name": file_name,
            "download_url": download_url,
        }
        data.append(task_result)

    return GeneralResponse(code=0, data=data)


async def get_file(
    work_id: int,
    task_id: int,
    file_name: str,
    db_session: Session
):
    work = get_common_work_by_id(work_id, db_session)
    task = get_task_by_id(work_id, task_id, db_session)
    if not task or not work:
        return get_error_response(400209)

    task_path = os.path.join(work.work_directory, task.resource_task_id)
    if not os.path.exists(task_path):
        return get_error_response(400211)

    task_file = os.path.join(task_path, file_name)
    if not os.path.exists(task_file):
        return get_error_response(400212)

    return task_file


async def start_up(work_id: int, task_id: int, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    task = get_task_by_id(work_id, task_id, db_session)
    if not task or not work:
        return get_error_response(400209)

    try:
        data = {
            "workspace_id": work.workspace_id,
            "work_id": task.work_id,
            "task_id": task.task_id,
            "name": task.name,
            "work_directory": work.work_directory,
            "main_class_name": work.main_class_name,
            "extra_params": work.extra_params,
            "java_version": work.java_version.name,
            "category": work.category.name,
            "excutable_file": work.executable_file_path,
        }

        base_url = settings.resource_service
        async with AsyncClient(base_url=base_url) as client:
            response = await client.post("/tasks", json=data)
            if response.status_code >= 400:
                logger.debug(errors.get(400206))
                logger.debug(response.text)
                return get_error_response(400206)

            json_response = GeneralResponse(**response.json())
            if json_response.code != 0:
                return GeneralResponse(
                    code=json_response.code,
                    msg=json_response.msg
                )

            data: Dict = json_response.data
            resource_task_id = data.get('task_id')
            if resource_task_id:
                task.resource_task_id = resource_task_id
                task.manually_start()

                db_session.add(task)
                db_session.commit()
                db_session.refresh(task)

        result = GeneralResponse(code=0, data=task)

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400300)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def shut_down(work_id: int, task_id: int, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    task = get_task_by_id(work_id, task_id, db_session)
    if not task or not work:
        return get_error_response(400209)

    try:
        if task.task_type == TaskTypeEnum.IMPORT:
            if task.resource_task_id:
                celery_result = AsyncResult(task.resource_task_id)
                celery_result.revoke(terminate=True)

            task.manually_terminate()
            db_session.add(task)
            db_session.commit()
            db_session.refresh(task)

            return GeneralResponse(code=0, data=task)

        base_url = settings.resource_service
        async with AsyncClient(base_url=base_url) as client:
            response = await client.delete(f"/tasks/{task.resource_task_id}")
            if response.status_code >= 400:
                logger.debug(errors.get(400206))
                logger.debug(response.text)
                return get_error_response(400206)

        task.manually_terminate()

        db_session.add(task)
        db_session.commit()
        db_session.refresh(task)

        result = GeneralResponse(code=0, data=task)

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400300)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def shut_down_remote_task(resource_task_id: str):
    try:
        base_url = settings.resource_service
        async with AsyncClient(base_url=base_url) as client:
            response = await client.delete(f"/tasks/{resource_task_id}")
            if response.status_code >= 400:
                logger.debug(errors.get(400206))
                logger.debug(response.text)
                return get_error_response(400206)

            logger.info(f"task-{resource_task_id}: shut down succeeded")
            result = GeneralResponse(code=0, data={"success": True})

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400300)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def post_performance_indicators(
    work_id: int,
    task_uuid: str,
    query_id: str,
    db_session: Session,
):
    """
    根据task_uuid写入对应的sql性能指标
    """
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400209)

    analysis = db_session.query(Analysis).filter(
        Analysis.task_uuid == task_uuid).first()

    if analysis is None:
        return get_error_response(400203)

    connection = db_session.query(Connection).filter(
        Connection.connection_id == analysis.connection_id
    ).first()
    if not connection:
        return get_error_response(400203)

    try:
        client = Client(
            host=connection.host,
            database='system',
            user=connection.username,
            password=connection.password,
            port=connection.port
        )
    except Exception as e:
        logger.exception(e)

    sql = f" select type, read_rows, written_rows, result_rows, memory_usage," \
          f" query_duration_ms, event_time from query_log where query_id='{query_id}'"

    rows = client.execute(sql)

    # 正常情况下会返回两条数据QueryStart、QueryFinish
    if rows:
        if len(rows) == 1:
            analysis.query_state = "STARTING"

        for row in rows:
            if row[0] == "QueryFinish":
                analysis.read_rows = row[1]
                analysis.written_rows = row[2]
                analysis.read_rows = row[3]
                analysis.memory_use = row[4]
                analysis.duration_ms = row[5]
                analysis.query_end_time = row[6]

                analysis.query_state = "END"

        db_session.add(analysis)
        db_session.commit()
        db_session.refresh(analysis)

    return analysis


async def get_performance_indicators(work_id: int, task_id: int, db_session: Session):
    """
    根据task_id 返回对应的sql性能指标
    """
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400209)

    analysis = db_session.query(Analysis).filter(
        Analysis.task_id == task_id).first()

    if analysis is None:
        return get_error_response(400203)

    return GeneralResponse(code=0, data=analysis)


async def get_airflow_tasks(work_id: int, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400209)

    try:
        base_url = settings.airflow_service
        async with AsyncClient(base_url=base_url) as client:
            raw_response = await client.get(
                f"/dags/{work.uuid}/tasks",
                auth=('admin', 'admin')
            )
            if raw_response.status_code >= 400:
                logger.error(errors.get(400218))
                logger.error(raw_response.text)

                return get_error_response(400218)

            json_response: Dict = raw_response.json()
            tasks = json_response.get("tasks")

        result = GeneralResponse(code=0, data=tasks)

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400300)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def get_airflow_dag_runs(
    dag_id: str,
    limit: int,
    offset: int,
):
    try:
        dag_runs, total_entries = None, None
        base_url = settings.airflow_service
        async with AsyncClient(base_url=base_url) as client:
            raw_response = await client.get(
                f"/dags/{dag_id}/dagRuns",
                auth=('admin', 'admin'),
                params={"limit": limit, "offset": offset}
            )
            if raw_response.status_code >= 400:
                logger.error(errors.get(400218))
                logger.error(raw_response.text)

                return get_error_response(400218)

            json_response: Dict = raw_response.json()
            dag_runs = json_response.get("dag_runs")
            total_entries = json_response.get("total_entries")

        data = {
            "items": dag_runs or [],
            "total": total_entries or 0,
        }

        result = GeneralResponse(code=0, data=data)

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400300)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def get_airflow_task_instances(
    dag_id: str,
    dag_run_id: str,
    limit: int,
    offset: int,
):
    try:
        task_instances, total_entries = None, None
        base_url = settings.airflow_service
        async with AsyncClient(base_url=base_url) as client:
            raw_response = await client.get(
                f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
                auth=('admin', 'admin'),
                params={"limit": limit, "offset": offset}
            )
            if raw_response.status_code >= 400:
                logger.error(errors.get(400218))
                logger.error(raw_response.text)

                return get_error_response(400218)

            json_response: Dict = raw_response.json()
            task_instances = json_response.get("task_instances")
            total_entries = json_response.get("total_entries")

        data = {
            "items": task_instances or [],
            "total": total_entries or 0,
        }

        result = GeneralResponse(code=0, data=data)

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400300)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result
