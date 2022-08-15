from typing import Optional

from fastapi import APIRouter, Depends, status, Header
from sqlalchemy.orm import Session

from apps.core.enums import ActionEnum, WorkTypeEnum, CategoryEnum, SqlEnum
from apps.database import get_db_session
from apps.schemas import GeneralResponse, get_error_response
from apps.schemas.work import (
    WorkActionSchema,
    WorkBulkDeleteSchema,

    CommonWorkCreateSchema,
    CommonWorkUpdateSchema,

    ImportWorkCreateSchema,
    ImportWorkUpdateSchema,
    SqlSchema,
    SqlLineageSchema,
    SqlLogsSchema,
)
from apps.services.work import (
    create_common_work,
    get_common_work,
    update_common_work,
    submit_common_work,
    delete_common_work,
    list_common_works,
    list_common_work_instances,
    search_common_works,
    search_common_work_instances,
    delete_common_works,
    get_common_work_versions,
    get_tree_of_common_works,

    create_import_work,
    get_import_work,
    update_import_work,
    submit_import_work,
    delete_import_work,
    delete_import_works,
    list_import_works,
    list_import_work_instances,
    search_import_works,
    search_import_work_instances,

    start_up_common_work,
    shut_down_common_work,
    bulk_shut_down_common_work,
    shut_down_import_work,
    bulk_shut_down_import_work,
    delete_sql,
    sql_record,
    get_performance_indicators,
    post_performance_indicators,
    create_sql_logs,
    get_sql_logs,

    get_sqls_lineage,
)
from apps.middlewares.work import verify_work


router = APIRouter()


@router.post(
    "/common-works",
    response_model=GeneralResponse,
    status_code=status.HTTP_201_CREATED,
    description="创建作业",
    tags=["common-works"]
)
async def create_common_item(
    payload: CommonWorkCreateSchema,
    authorization: str = Header(None),
    db_session: Session = Depends(get_db_session),
):
    return await create_common_work(payload, authorization, db_session)


@router.get(
    "/common-works",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="获取作业列表",
    tags=["common-works"]
)
async def list_common_items(
    workspace_id: int,
    offset: Optional[int] = 0,
    limit:  Optional[int] = 10,
    type: Optional[WorkTypeEnum] = None,
    category: Optional[CategoryEnum] = None,
    db_session: Session = Depends(get_db_session),
    is_submitted: Optional[bool] = False,
):
    if is_submitted:
        return await list_common_work_instances(
            db_session, workspace_id, limit, offset, type, category)

    return await list_common_works(
        db_session, workspace_id, limit, offset, type, category)


@router.get(
    "/common-works/search",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="搜索作业",
    tags=["common-works"]
)
async def search_common_items(
    workspace_id: int,
    word: Optional[str],
    limit:  Optional[int] = 10,
    offset: Optional[int] = 0,
    db_session: Session = Depends(get_db_session),
    is_submitted: Optional[bool] = False,
):
    if is_submitted:
        return await search_common_work_instances(
            db_session, workspace_id, limit, offset, word)

    return await search_common_works(
        db_session, workspace_id, limit, offset, word)


@router.get(
    "/common-works/tree",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="搜索作业",
    tags=["common-works"]
)
async def get_workspace_tree(
    workspace_id: int,
    authorization: str = Header(None),
    db_session: Session = Depends(get_db_session),
):
    return await get_tree_of_common_works(
        workspace_id, authorization, db_session)


@router.post(
    "/common-works/lineage",
    status_code=status.HTTP_200_OK,
    description="根据task_id获取sql性能指标",
    tags=["common-works"]
)
async def get_lineage(payload: SqlLineageSchema):
    return await get_sqls_lineage(payload)


@router.post(
    "/common-works/bulk-delete",
    response_model=GeneralResponse,
    status_code=status.HTTP_201_CREATED,
    description="批量删除作业",
    tags=["common-works"],
)
async def delete_common_items(
    payload: WorkBulkDeleteSchema,
    db_session: Session = Depends(get_db_session),
):
    return await delete_common_works(payload, db_session)


@router.get(
    "/common-works/{work_id}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="获取作业详情",
    tags=["common-works"]
)
async def get_common_item(
    work_id: int,
    with_last_task: Optional[bool] = False,
    db_session: Session = Depends(get_db_session),
):
    return await get_common_work(work_id, with_last_task, db_session)


@router.put(
    "/common-works/{work_id}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="更新作业详情",
    tags=["common-works"],
    # dependencies=[Depends(verify_work)]
)
async def update_common_item(
    work_id: int,
    payload: CommonWorkUpdateSchema,
    db_session: Session = Depends(get_db_session),
):
    return await update_common_work(work_id, payload, db_session)


@router.delete(
    "/common-works/{work_id}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="删除作业（软删除）",
    tags=["common-works"]
)
async def delete_common_item(
    work_id: int,
    db_session: Session = Depends(get_db_session),
):
    return await delete_common_work(work_id, db_session)


@router.get(
    "/common-works/{work_id}/versions",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="获取作业提交版本",
    tags=["common-works"]
)
async def get_common_item_version(
    work_id: int,
    limit:  Optional[int] = 10,
    offset: Optional[int] = 0,
    db_session: Session = Depends(get_db_session),
):
    return await get_common_work_versions(work_id, limit, offset, db_session)


@router.put(
    "/common-works/{work_id}/action",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="运行/停止/提交作业",
    tags=["common-works"]
)
async def alter_common_work(
    work_id: int,
    payload: WorkActionSchema,
    limit: Optional[int] = 100,
    authorization: str = Header(None),
    db_session: Session = Depends(get_db_session),
):
    if payload.action == ActionEnum.START_UP:
        response = await start_up_common_work(work_id, limit, db_session)
    elif payload.action == ActionEnum.SHUT_DOWN:
        response = await shut_down_common_work(work_id, db_session)
    elif payload.action == ActionEnum.SHUT_DOWN_ALL:
        response = await bulk_shut_down_common_work(work_id, db_session)
    elif payload.action == ActionEnum.SUBMIT:
        response = await submit_common_work(
            work_id, payload, authorization, db_session)

    return response


@router.post(
    "/common-works/{work_id}/logs",
    status_code=status.HTTP_201_CREATED,
    description="回调添加sql执行的日志信息",
    tags=["common-works"])
async def create_sql_run_state(
    work_id: int,
    payload: SqlLogsSchema,
    db_session: Session = Depends(get_db_session),
):
    return await create_sql_logs(work_id, payload, db_session)


@router.get(
    "/common-works/{work_id}/logs",
    status_code=status.HTTP_200_OK,
    description="获取sql执行的日志信息",
    tags=["common-works"])
async def create_sql_run_state(
    work_id: int,
    db_session: Session = Depends(get_db_session),
):
    return await get_sql_logs(work_id, db_session)


@router.get(
    "/common-works/{work_id}/records",
    status_code=status.HTTP_200_OK,
    description="获取sql历史执行数据",
    tags=["common-works"]
)
async def get_sql_record(
    work_id: int,
    sql_id: Optional[int] = None,
    db_session: Session = Depends(get_db_session)
):
    """
    获取sql历史执行数据
    """

    return await sql_record(work_id, sql_id, db_session)


@router.delete(
    "/common-works/{work_id}/records/{log_id}",
    status_code=status.HTTP_200_OK,
    description="删除对应sql历史数据",
    tags=["common-works"]

)
async def delete_sql_log(
    work_id: int,
    log_id: int,
    db_session: Session = Depends(get_db_session),
):
    """
    删除对应的历史日志数据
    """
    return await delete_sql(work_id, log_id, db_session)


# Data import works

@router.post(
    "/import-works",
    response_model=GeneralResponse,
    status_code=status.HTTP_201_CREATED,
    description="创建数据导入作业",
    tags=["import-works"]
)
async def create_import_item(
    payload: ImportWorkCreateSchema,
    authorization: str = Header(None),
    db_session: Session = Depends(get_db_session),
):
    return await create_import_work(payload, authorization, db_session)


@router.get(
    "/import-works",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="获取数据导入作业列表",
    tags=["import-works"]
)
async def get_import_items(
    workspace_id: int,
    is_submitted: Optional[bool] = False,
    limit:  Optional[int] = 10,
    offset: Optional[int] = 0,
    db_session: Session = Depends(get_db_session),
):
    if is_submitted:
        return await list_import_work_instances(
            db_session, workspace_id, limit, offset)

    return await list_import_works(
        db_session, workspace_id, limit, offset)


@router.post(
    "/import-works/bulk-delete",
    response_model=GeneralResponse,
    status_code=status.HTTP_201_CREATED,
    description="批量删除数据导入作业",
    tags=["import-works"],
)
async def delete_import_items(
    payload: WorkBulkDeleteSchema,
    db_session: Session = Depends(get_db_session),
):
    return await delete_import_works(payload, db_session)


@router.get(
    "/import-works/search",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="搜索数据导入作业",
    tags=["import-works"]
)
async def search_import_items(
    workspace_id: int,
    word: Optional[str],
    is_submitted: Optional[bool] = False,
    limit:  Optional[int] = 10,
    offset: Optional[int] = 0,
    db_session: Session = Depends(get_db_session),
):
    if is_submitted:
        return await search_import_work_instances(
            db_session, workspace_id, limit, offset, word)

    return await search_import_works(
        db_session, workspace_id, limit, offset, word)


@router.get(
    "/import-works/{work_id}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="获取数据导入作业详情",
    tags=["import-works"]
)
async def get_import_item(
    work_id: int,
    db_session: Session = Depends(get_db_session),
):
    return await get_import_work(work_id, db_session)


@router.put(
    "/import-works/{work_id}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="更新数据导入作业",
    tags=["import-works"]
)
async def update_import_item(
    work_id: int,
    payload: ImportWorkUpdateSchema,
    db_session: Session = Depends(get_db_session),
):
    return await update_import_work(work_id, payload, db_session)


@router.delete(
    "/import-works/{work_id}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="删除数据导入作业（软删除）",
    tags=["import-works"]
)
async def delete_import_item(
    work_id: int,
    db_session: Session = Depends(get_db_session),
):
    return await delete_import_work(work_id, db_session)


@router.put(
    "/import-works/{work_id}/submit",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="提交作业",
    tags=["import-works"],
    deprecated=True,
)
async def submit_import_item(
    work_id: int,
    payload: WorkActionSchema,
    authorization: str = Header(None),
    db_session: Session = Depends(get_db_session),
):
    return await submit_import_work(work_id, payload, authorization, db_session)


@router.put(
    "/import-works/{work_id}/action",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
    description="运行/停止/提交作业",
    tags=["import-works"]
)
async def alter_import_work(
    work_id: int,
    payload: WorkActionSchema,
    db_session: Session = Depends(get_db_session),
):
    if payload.action == ActionEnum.SHUT_DOWN:
        response = await shut_down_import_work(work_id, db_session)
    elif payload.action == ActionEnum.SHUT_DOWN_ALL:
        response = await bulk_shut_down_import_work(work_id, db_session)
    elif payload.action == ActionEnum.SUBMIT:
        response = await submit_import_work(work_id, payload, db_session)

    return response
