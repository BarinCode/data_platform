from typing import Optional
from fastapi import APIRouter, Depends, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from apps.core.enums import ActionEnum, TaskTypeEnum
from apps.database import get_db_session
from apps.schemas import GeneralResponse
from apps.schemas.task import ActionSchema
from apps.services.task import (
    get, list,
    get_log, get_file, get_all_file,
    start_up, shut_down,
    post_performance_indicators,
    get_performance_indicators,
    get_airflow_tasks,
    get_airflow_dag_runs,
    get_airflow_task_instances,
)


router = APIRouter(tags=["tasks"])


@router.get(
    "/works/{work_id}/tasks",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def get_tasks(
    work_id: int,
    limit: int = 10,
    offset: int = 0,
    task_type: TaskTypeEnum = TaskTypeEnum.COMMON,
    is_temporary: Optional[bool] = False,
    from_airflow: Optional[bool] = False,
    db_session: Session = Depends(get_db_session),
):
    if from_airflow:
        return await get_airflow_tasks(work_id, db_session)

    return await list(
        work_id, limit, offset, task_type, is_temporary, db_session)


@router.get(
    "/works/{work_id}/tasks/{task_id}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def get_task(
    work_id: int,
    task_id: int,
    db_session: Session = Depends(get_db_session),
):
    return await get(work_id, task_id, db_session)


@router.get(
    "/works/{work_id}/tasks/{task_id}/log",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def get_task_log(
    work_id: int,
    task_id: int,
    db_session: Session = Depends(get_db_session),
):
    return await get_log(work_id, task_id, db_session)


@router.get(
    "/works/{work_id}/tasks/{task_id}/result",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def get_task_result(
    work_id: int,
    task_id: int,
    db_session: Session = Depends(get_db_session),
):
    return await get_all_file(work_id, task_id, db_session)


@router.get(
    "/works/{work_id}/tasks/{task_id}/{file_name}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def get_task_file(
    work_id: int,
    task_id: int,
    file_name: str,
    db_session: Session = Depends(get_db_session),
):
    task_file = await get_file(work_id, task_id, file_name, db_session)

    return FileResponse(task_file)


@router.put(
    "/works/{work_id}/tasks/{task_id}/action",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="启动/终止任务",
)
async def update_task_state(
    work_id: int,
    task_id: int,
    payload: ActionSchema,
    db_session: Session = Depends(get_db_session),
):
    if payload.action == ActionEnum.START_UP:
        response = await start_up(work_id, task_id, db_session)
    elif payload.action == ActionEnum.SHUT_DOWN:
        response = await shut_down(work_id, task_id, db_session)

    return response


@router.post(
    "/works/{work_id}/tasks/{task_uuid}/indicators",
    status_code=status.HTTP_201_CREATED,
    description="根据task_uuid获取sql执行性能指标",
)
async def get_task_uuid(
    work_id: int,
    task_uuid: str,
    query_id: str,
    db_session: Session = Depends(get_db_session)
):
    """
    根据任务uuid写入对应sql的性能指标
    """
    return await post_performance_indicators(
        work_id, task_uuid, query_id, db_session)


@router.get(
    "/works/{work_id}/tasks/{task_id}/indicators",
    status_code=status.HTTP_200_OK,
    description="根据task_id获取sql性能指标",
)
async def get_task_id(
    work_id: int,
    task_id: int,
    db_session: Session = Depends(get_db_session)
):
    return await get_performance_indicators(work_id, task_id, db_session)
