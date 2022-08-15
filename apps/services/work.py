import asyncio
import time
import json
import os
from copy import deepcopy
from datetime import datetime, timedelta
from os import path
from json.decoder import JSONDecodeError
from typing import Dict, List, Optional
from urllib import parse
from apps.services.dag.dag import Dag

import requests
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
from sqlalchemy import or_
from sqllineage.core.models import Table
from sqllineage.runner import LineageRunner

from apps.utils.gen_uuid import get_uuid
from apps.config import settings
from apps.core.enums import (
    CategoryEnum,
    DataSourceEnum,
    TaskStatusEnum,
    TaskTypeEnum,
    WorkTypeEnum,
    WorkDependEnum,
)
from apps.core.errors import errors
from apps.models import Task, CommonWorkInstance
from apps.models.sql import SqlRecord
from apps.models.sql_log import SqlLog
from apps.models.analysis import Analysis
from apps.models.connection import Connection
from apps.models.work import (
    CommonWork,
    CommonWorkInstance,

    ImportWork,
    ImportWorkInstance,
)
from apps.schemas import GeneralResponse, get_error_response
from apps.schemas.task import TaskCreateSchema
from apps.schemas.work import (
    WorkActionSchema,
    WorkBulkDeleteSchema,

    CommonWorkCreateSchema,
    CommonWorkUpdateSchema,

    ImportWorkCreateSchema,
    ImportWorkUpdateSchema,

    SqlLineageSchema,
)
from apps.services.connection import get_connection_by_id
from apps.services.internal_service.user_management import UserManagementService
from apps.services.internal_service.resource_management import ResourceManagementService
from apps.exceptions import InternalServiceException
from apps.core.transaction import transaction



# ! Will be deprecated in v1.4.0
def walk_nested_list(
    nested_list: List[Dict],
    select_node: Dict,
    value: List[Dict],
):
    for node in nested_list:
        if node["workspace_id"] == select_node["workspace_id"]:
            node["work_children"] = value

        workspace_children = node.get("workspace_children")
        if workspace_children:
            walk_nested_list(workspace_children, select_node, value)

        else:
            if node["workspace_id"] == select_node["workspace_id"]:
                node["work_children"] = value

    return nested_list


# ! Will be deprecated in v1.4.0
def get_work_tree(
    db_session: Session,
    tree: List[Dict],
    total_tree: List[Dict],
):
    """A recursive way to get the whole work tree

    Args:
        db_session (Session): database session
        tree (Dict): the seed of workspace tree
        total_tree (Dict): the current workspace tree

    Returns:
        workspace_tree (Dict): the final whole final workspace tree
    """

    if not tree:
        return total_tree

    total_sub_tree = []

    for node in tree:
        works: List[CommonWork] = db_session.query(CommonWork)\
            .order_by(CommonWork.created_at.desc())\
            .filter(CommonWork.workspace_id == node.get("workspace_id"))\
            .filter(CommonWork.deleted_at == None)\
            .all()

        # if works:
        work_children = []
        for work in works:
            created_at = work.created_at.strftime("%Y-%m-%d %H:%M:%S")
            work_node = {
                "work_id": work.work_id,
                "uuid": work.uuid,
                "name": work.name,
                "description": work.description,
                "category": work.category,
                "created_at": created_at,
                "workspace_id": work.workspace_id,
                "submitted_by": work.submitted_by,
                "executable_sql": work.executable_sql,
                "failed_retry_times": work.failed_retry_times,
                "delay_minutes": work.delay_minutes,
                "is_timeout_enabled": work.is_timeout_enabled,
                "timeout_strategy": work.timeout_strategy,
                "timeout_minutes": work.timeout_minutes,
                "notice_strategy": work.notice_strategy,
                "retry_delta_minutes": work.timeout_template_id,
                "finish_template_id": work.finish_template_id,
                "cron_expression": work.cron_expression,
                "connection_id": work.connection_id,
            }
            work_children.append(work_node)

        node["work_children"] = work_children

        leaf_nodes = node["workspace_children"]

        total_sub_tree.extend(leaf_nodes)

        total_tree = walk_nested_list(total_tree, node, work_children)

    return get_work_tree(db_session, total_sub_tree, total_tree)


# ! Will be deprecated in v1.4.0
async def get_tree_of_common_works(
    workspace_id: int,
    authorization: str,
    db_session: Session,
):
    try:
        base_url = settings.user_service
        async with AsyncClient(base_url=base_url) as client:
            raw_response = await client.get(
                f"/workspaces/{workspace_id}/tree",
                # headers={'authorization': authorization}
            )
            if raw_response.status_code >= 400:
                logger.debug(errors.get(400202))
                logger.debug(raw_response.text)
                return get_error_response(400202)

            json_response = GeneralResponse(**raw_response.json())
            if json_response.code != 0:
                return GeneralResponse(
                    code=json_response.code,
                    msg=json_response.msg
                )

            data = json_response.data
            total_tree = deepcopy(data)

            tree = get_work_tree(db_session, data, total_tree)

        result = GeneralResponse(code=0, data=tree[0])

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400100)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


def get_common_work_by_id(
    work_id: int,
    db_session: Session,
) -> Optional[CommonWork]:
    return db_session.query(CommonWork)\
        .filter(CommonWork.work_id == work_id)\
        .filter(CommonWork.deleted_at == None)\
        .first()


def get_common_work_by_name(
    name: str,
    workspace_id: str,
    db_session: Session,
) -> Optional[CommonWork]:
    return db_session.query(CommonWork)\
        .filter(CommonWork.name == name)\
        .filter(CommonWork.workspace_id == workspace_id)\
        .filter(CommonWork.deleted_at == None)\
        .first()


async def create_common_work(
    payload: CommonWorkCreateSchema,
    authorization: str,
    db_session: Session
):
    is_duplicate = get_common_work_by_name(
        payload.name,
        payload.workspace_id,
        db_session,
    )
    if is_duplicate:
        return get_error_response(400500)
    try:
        work = CommonWork(**payload.dict())
        user_service = UserManagementService()
        workspace_data = await user_service.get_workspace_info(work.workspace_id)
        work.root_workspace_id = workspace_data.get("root_id")
        db_session.add(work)
        db_session.flush()
        data = await user_service.create_workspace_dir(work.workspace_id, workspace_data.get("asset_id"), f"{work.work_id}")
        work.work_directory = data.get("real_dir_path")
        db_session.add(work)

        resource_service = ResourceManagementService()
        data = await resource_service.create_resource_group(work.work_id, work.workspace_id)
        db_session.commit()
        result = GeneralResponse(code=0, data=work)
    except InternalServiceException as e:
        logger.exception(e)
        result = get_error_response(400213)
    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)
    return result


async def get_common_work(
    work_id: int,
    with_last_task: bool,
    db_session: Session
):
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    if with_last_task:
        last_task = db_session.query(Task)\
            .filter(Task.work_id == work_id)\
            .filter(Task.is_temporary == True)\
            .first()

        setattr(work, "last_task", last_task)

    return GeneralResponse(code=0, data=work)


async def update_common_work(
    work_id: int,
    payload: CommonWorkUpdateSchema,
    db_session: Session,
):
    # TODO: move to worker middleware
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    # TODO: It's ugly, we should find out a more elegant implementation
    attrs = (
        "name",
        "description",
        "failed_retry_times",
        "retry_delta_minutes",
        "delay_minutes",
        "is_timeout_enabled",
        "timeout_strategy",
        "timeout_minutes",
        "notice_strategy",
        "timeout_template_id",
        "cron_expression",
        "main_class_name",
        "java_version",
        "extra_params",
        "executable_sql",
        "nodes",
        "started_at",
        "ended_at",
        "submitted_by",
        "connection_id",
        "executable_file_name",
        "executable_file_path",
    )
    for attr in attrs:
        value = getattr(payload, attr)
        if value is not None:
            setattr(work, attr, value)

    db_session.add(work)
    db_session.commit()
    db_session.refresh(work)

    return GeneralResponse(code=0, data=work)


async def delete_common_work(work_id: int, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    work.deleted_at = datetime.now()

    db_session.add(work)
    db_session.commit()
    db_session.refresh(work)

    return GeneralResponse(code=0, data=work)


async def delete_common_works(
    payload: WorkBulkDeleteSchema,
    db_session: Session
):
    for work_id in payload.work_ids:
        work = get_common_work_by_id(work_id, db_session)
        if not work:
            logger.error(errors.get(400203))
            continue

        work.deleted_at = datetime.now()
        db_session.add(work)

    db_session.commit()

    return GeneralResponse(code=0, data=None)


async def list_common_works(
    db_session: Session,
    workspace_id: int,
    limit: int,
    offset: int,
    work_type: Optional[WorkTypeEnum],
    category: Optional[CategoryEnum],
):
    work_query = db_session.query(CommonWork)\
        .filter(CommonWork.deleted_at == None)\
        .filter(CommonWork.root_workspace_id == workspace_id)

    if category:
        work_query = work_query.filter(CommonWork.category == category)

    items = work_query \
        .order_by(CommonWork.created_at.desc()) \
        .limit(limit).offset(offset).all()
    total = work_query.count()

    data = {
        "items": items,
        "total": total
    }

    return GeneralResponse(code=0, data=data)


async def create_instance(
        work: CommonWork,
        payload: WorkActionSchema,
        authorization: str,
        db_session: Session
):
    username = None
    base_url = settings.user_service
    async with AsyncClient(base_url=base_url) as client:
        response = await client.get(
            f"/accounts/{payload.user_id}",
            # headers={'authorization': authorization}
        )
        if response.status_code >= 400:
            logger.error(errors.get(400202))
            logger.error(response.text)
            return get_error_response(400202)

        json_response = GeneralResponse(**response.json())
        if json_response.code != 0:
            return GeneralResponse(
                code=json_response.code,
                msg=json_response.msg
            )

        data: Dict[str, str] = json_response.data
        username = data.get("username")

    # create common work instance
    work_instance = CommonWorkInstance(
        work_id=work.work_id,
        user_id=work.user_id,
        workspace_id=work.workspace_id,
        root_workspace_id=work.root_workspace_id,
        name=work.name,
        description=work.description,
        status=work.status,

        failed_retry_times=work.failed_retry_times,
        retry_delta_minutes=work.retry_delta_minutes,
        delay_minutes=work.delay_minutes,
        is_timeout_enabled=work.is_timeout_enabled,
        timeout_strategy=work.timeout_strategy,
        timeout_minutes=work.timeout_minutes,
        notice_strategy=work.notice_strategy,
        timeout_template_id=work.timeout_template_id,
        finish_template_id=work.finish_template_id,
        cron_expression=work.cron_expression,
        started_at=work.started_at,
        ended_at=work.ended_at,

        category=work.category,
        work_directory=work.work_directory,
        executable_file_name=work.executable_file_name,
        executable_file_path=work.executable_file_path,

        java_version=work.java_version,
        main_class_name=work.main_class_name,
        extra_params=work.extra_params,

        executable_sql=work.executable_sql,
        connection_id=work.connection_id,

        relation=work.relation,
        edges=work.edges,
        nodes=work.nodes,

        submitted_by=username,
        submitted_at=datetime.now(),
    )

    work_instance.execute()
    db_session.add(work_instance)
    db_session.commit()

    return work_instance


async def submit_common_work(
        work_id: int,
        payload: WorkActionSchema,
        authorization: str,
        db_session: Session,
):
    try:
        work = get_common_work_by_id(work_id, db_session)
        work_instance = await create_instance(work, payload, authorization, db_session)
        dag_util = Dag(work_instance, payload, db_session)
        dag = dag_util.from_work()
        dag_util.save(os.environ.get('DAG_FILE_HOME'), dag)
        result = GeneralResponse(code=0, data=work_instance.uuid)
    except Exception as exception:
        logger.exception(exception)
        return get_error_response(400000)

    return result


async def get_common_work_versions(
    work_id: int,
    limit:  int,
    offset: int,
    db_session: Session,
):
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    version_query = db_session.query(CommonWorkInstance)\
        .filter(CommonWorkInstance.work_id == work_id)\
        .filter(CommonWorkInstance.deleted_at == None)\
        .order_by(CommonWorkInstance.submitted_at.desc())

    items = version_query.limit(limit).offset(offset).all()
    total = version_query.count()

    data = {
        "items": items,
        "total": total
    }

    return GeneralResponse(code=0, data=data)


async def search_common_works(
    db_session: Session,
    workspace_id: int,
    limit: int,
    offset: int,
    keyword: str,
):
    work_query = db_session.query(CommonWork)\
        .filter(CommonWork.deleted_at == None)\
        .filter(CommonWork.root_workspace_id == workspace_id)\
        .filter(CommonWork.name.like(f"%{parse.unquote(keyword)}%"))\
        .order_by(CommonWork.created_at.desc())

    items = work_query.limit(limit).offset(offset).all()
    total = work_query.count()

    data = {
        "items": items,
        "total": total,
    }

    return GeneralResponse(code=0, data=data)


async def start_up_sql_work(
    work: CommonWork,
    limit: int,
    db_session: Session,
):
    work = get_common_work_by_id(work.work_id, db_session)
    if not work:
        return get_error_response(400203)

    connection = get_connection_by_id(work.connection_id, db_session)
    if not connection:
        return get_error_response(400215)

    sql_record = SqlRecord(
        work_id=work.work_id,
        user_id=work.user_id,
        sql=work.executable_sql,
        started=datetime.now(),
    )

    kwargs = dict(
        host=connection.host,
        port=connection.port,
        user=connection.username,
    )
    if connection.password:
        kwargs['password'] = connection.password
    if connection.database_name:
        kwargs['database'] = connection.database_name

    data = {
        "status": 1,
        "desc": "ok",
        "column": [],
        "row": [],
        "total": 0,
        "duration": None,
    }
    try:
        client = Client(**kwargs)
        sql: str = sql_record.sql

        if sql.lower().strip().strip(";").startswith('select'):
            sql = f"""
                select * from ({sql.strip(";")}) chuhedb_table limit {limit}
            """
        else:
            sql = sql.strip().strip(";")

        rows = client.execute(sql, with_column_types=True)
        sql_record.rows = len(rows)
        sql_record.result = 1

        data['status'] = 1
        data['column'] = [r[0] for r in rows[1]]
        data['row'] = rows[0]
        data["total"] = sql_record.rows
        data['duration'] = client.last_query.elapsed

        client.disconnect()

    except Exception as e:
        logger.exception(e)
        sql_record.result = 2

        data["status"] = 2
        data['desc'] = str(e)

    sql_record.ended = datetime.now()
    sql_record.durations = data['duration']
    db_session.add(sql_record)

    db_session.commit()

    return GeneralResponse(code=0, data=data)


async def start_up_common_work(work_id: str, limit: int, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    if work.category == CategoryEnum.SQL:
        return await start_up_sql_work(work, limit, db_session)

    try:
        running_conditions = or_(
            Task.status == TaskStatusEnum.STARTING,
            Task.status == TaskStatusEnum.RUNNING,
        )
        is_running = db_session.query(Task)\
            .filter(Task.work_id == work_id)\
            .filter(Task.is_temporary == True)\
            .filter(running_conditions)\
            .first()
        if is_running:
            return get_error_response(400208)

        task = Task(
            work_id=work_id,
            excutable_file=work.executable_file_path,
            is_temporary=True,
            started_at=datetime.now(),
        )

        db_session.add(task)
        db_session.commit()
        db_session.refresh(task)

        data = {
            "workspace_id": work.workspace_id,
            "work_id": work.work_id,
            "task_id": task.task_id,
            "name": work.name,
            "work_directory": work.work_directory,
            "main_class_name": work.main_class_name,
            "extra_params": work.extra_params,
            "java_version": work.java_version.name,
            "category": work.category.name,
            "excutable_file": work.executable_file_path,
        }

        # send request to resource_service to run task
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
                task.started_at = datetime.now()

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


async def shut_down_common_work(work_id: str, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    try:
        # get the last created task with starting or running work
        running_conditions = or_(
            Task.status == TaskStatusEnum.STARTING,
            Task.status == TaskStatusEnum.RUNNING,
        )
        task: Optional[Task] = db_session.query(Task)\
            .filter(Task.work_id == work_id)\
            .filter(Task.is_temporary == True)\
            .filter(running_conditions)\
            .order_by(Task.started_at.desc())\
            .first()
        if not task:
            return get_error_response(400207)

        base_url = settings.resource_service
        async with AsyncClient(base_url=base_url) as client:
            response = await client.delete(f"/tasks/{task.resource_task_id}")
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
        result = get_error_response(400100)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def bulk_shut_down_common_work(work_id: str, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    try:
        work.finish()
        db_session.add(work)

        # get the last created task with running work
        base_url = settings.resource_service
        async with AsyncClient(base_url=base_url) as client:
            response = await client.delete(f"/works/{work_id}")
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

        terminating_tasks = db_session.query(Task) \
            .filter(Task.work_id == work_id) \
            .filter(Task.deleted_at == None) \
            .filter(Task.is_temporary == False)

        for task in terminating_tasks:
            task.manually_terminate()
            db_session.add(task)

        db_session.commit()

        result = GeneralResponse(code=0, data={"success": True})

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400100)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def list_common_work_instances(
    db_session: Session,
    workspace_id: int,
    limit: int,
    offset: int,
    work_type: Optional[WorkTypeEnum],
    category: Optional[CategoryEnum],
):
    work_query = db_session.query(CommonWorkInstance)\
        .filter(CommonWorkInstance.deleted_at == None)\
        .filter(CommonWorkInstance.root_workspace_id == workspace_id)

    if category:
        work_query = work_query.filter(CommonWorkInstance.category == category)

    items = work_query\
        .order_by(CommonWorkInstance.created_at.desc())\
        .limit(limit).offset(offset).all()
    total = work_query.count()

    data = {
        "items": items,
        "total": total
    }

    return GeneralResponse(code=0, data=data)


async def search_common_work_instances(
    db_session: Session,
    workspace_id: int,
    limit: int,
    offset: int,
    keyword: str,
):
    work_query = db_session.query(CommonWorkInstance)\
        .filter(CommonWorkInstance.deleted_at == None)\
        .filter(CommonWorkInstance.root_workspace_id == workspace_id)\
        .filter(CommonWorkInstance.name.like(f"%{parse.unquote(keyword)}%"))\
        .order_by(CommonWorkInstance.created_at.desc())

    items = work_query.limit(limit).offset(offset).all()
    total = work_query.count()

    data = {
        "items": items,
        "total": total,
    }

    return GeneralResponse(code=0, data=data)


# Methods of import-work

def get_import_work_by_id(
    work_id: int,
    db_session: Session
) -> Optional[ImportWork]:
    return db_session.query(ImportWork).get(work_id)


def get_import_work_by_name(
    name: str,
    workspace_id: int,
    db_session: Session,
) -> Optional[ImportWork]:
    return db_session\
        .query(ImportWork)\
        .filter(ImportWork.name == name)\
        .filter(ImportWork.workspace_id == workspace_id)\
        .filter(ImportWork.deleted_at == None)\
        .first()


async def create_import_work(
    payload: ImportWorkCreateSchema,
    authorization: str,
    db_session: Session
):
    is_duplicate = get_import_work_by_name(
        payload.name,
        payload.workspace_id,
        db_session
    )
    if is_duplicate:
        return get_error_response(400500)

    try:
        import_work = payload.dict()
        import_work['pg_identity_field'] = ';'.join(
            import_work['pg_identity_field'] or [])
        work = ImportWork(**payload.dict())

        if not work.timeout_template_id:
            work.timeout_template_id = 0

        if not work.finish_template_id:
            work.finish_template_id = 0

        if not work.source_connection_id:
            work.source_connection_id = 0

        base_url = settings.user_service
        async with AsyncClient(base_url=base_url) as client:
            response = await client.get(
                f"/workspaces/{work.workspace_id}",
                # headers={'authorization': authorization}
            )
            if response.status_code >= 400:
                logger.debug(errors.get(400202))
                logger.debug(response.text)
                return get_error_response(400202)

            json_response = GeneralResponse(**response.json())
            if json_response.code != 0:
                return GeneralResponse(
                    code=json_response.code,
                    msg=json_response.msg
                )

            data: Dict = json_response.data
            root_id = data.get("root_id")
            work.root_workspace_id = root_id

        db_session.add(work)
        db_session.commit()
        db_session.refresh(work)

        result = GeneralResponse(code=0, data=work)

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400100)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def list_import_works(
    db_session: Session,
    workspace_id: int,
    limit: int,
    offset: int,
):
    paged_query = db_session.query(ImportWork)\
        .filter(ImportWork.deleted_at == None)\
        .filter(ImportWork.root_workspace_id == workspace_id)\
        .order_by(ImportWork.created_at.desc())

    items = paged_query.limit(limit).offset(offset).all()
    total = paged_query.count()

    data = {
        "items": items,
        "total": total,
    }

    return GeneralResponse(code=0, data=data)


async def get_import_work(work_id: int, db_session: Session):
    work = get_import_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    return GeneralResponse(code=0, data=work)


async def update_import_work(
    work_id: int,
    payload: ImportWorkUpdateSchema,
    db_session: Session
):
    work = get_import_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    if payload.name:
        is_duplicate = get_import_work_by_name(
            payload.name,
            payload.workspace_id,
            db_session
        )
        if is_duplicate and payload.name != work.name:
            return get_error_response(400500)

        work.name = payload.name

    work.description = payload.description or work.description

    work.data_source = payload.data_source or work.data_source
    work.write_type = payload.write_type or work.write_type
    work.start_line_no = payload.start_line_no or work.start_line_no

    work.data_file_name = payload.data_file_name or work.data_file_name
    work.data_file_path = payload.data_file_path or work.data_file_path
    work.extra_params = payload.extra_params or work.extra_params
    work.ftp_url = payload.ftp_url or work.ftp_url
    work.ftp_file_size = payload.ftp_file_size or work.ftp_file_size

    work.table_info = payload.table_info or work.table_info
    work.started_at = payload.started_at or work.started_at
    work.ended_at = payload.ended_at or work.ended_at

    db_session.add(work)
    db_session.commit()
    db_session.refresh(work)

    return GeneralResponse(code=0, data=work)


async def delete_import_work(work_id: int, db_session: Session):
    work = get_import_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    # soft delete
    work.deleted_at = datetime.now()

    db_session.add(work)
    db_session.commit()
    db_session.refresh(work)

    return GeneralResponse(code=0, data=work)


async def delete_import_works(
        payload: WorkBulkDeleteSchema,
        db_session: Session,
):
    for work_id in payload.work_ids:
        work = get_import_work_by_id(work_id, db_session)
        if not work:
            logger.warning(errors.get(400203))
            continue

        work.deleted_at = datetime.now()
        db_session.add(work)

    db_session.commit()

    return GeneralResponse(code=0, data=None)


async def search_import_works(
    db_session: Session,
    workspace_id: int,
    limit: int,
    offset: int,
    keyword: str,
):
    work_query = db_session.query(ImportWork)\
        .filter(ImportWork.deleted_at == None)\
        .filter(ImportWork.root_workspace_id == workspace_id)\
        .filter(ImportWork.name.like(f"%{parse.unquote(keyword)}%"))\
        .filter(ImportWork.submitted_by != None)\
        .filter(ImportWork.submitted_at != None)\
        .order_by(ImportWork.created_at.desc())\

    items = work_query.limit(limit).offset(offset).all()
    total = work_query.count()

    data = {
        "items": items,
        "total": total,
    }

    return GeneralResponse(code=0, data=data)


async def submit_import_work(
    work_id: int,
    payload: WorkActionSchema,
    authorization: str,
    db_session: Session,
):
    work = get_import_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    try:
        work_instance = ImportWorkInstance()
        for attr in dir(work):
            if not attr.startswith('_') and attr != 'uuid':
                val = getattr(work, attr)
                setattr(work_instance, attr, val)
        db_session.add(work_instance)
        db_session.commit()

        task = Task(
            name=f"import-work-{work_id}",
            work_id=work_instance.instance_id,
            task_type=TaskTypeEnum.IMPORT,
            is_temporary=False,
        )

        base_url = settings.user_service
        async with AsyncClient(base_url=base_url) as client:
            response = await client.get(
                f"/workspaces/{work.workspace_id}",
                # headers={'authorization': authorization}
            )
            if response.status_code >= 400:
                logger.debug(errors.get(400202))
                logger.debug(response.text)

                return get_error_response(400202)

            json_response = GeneralResponse(**response.json())
            if json_response.code != 0:
                return GeneralResponse(
                    code=json_response.code,
                    msg=json_response.msg
                )

            data: Dict[str, str] = json_response.data
            asset_id = data.get("asset_id")
            root_directory = data.get("root_directory")

            if not path.exists(work.data_file_path):
                return get_error_response(400214)

            if work.data_source == DataSourceEnum.FTP and \
                    path.isdir(work.data_file_path):
                upload_dir = work.data_file_path.replace(root_directory, "")
                upload_dir = upload_dir.lstrip("/")
                request_data = {
                    "address": work.ftp_url,
                    "dir_path": upload_dir,
                    "asset_id": asset_id,
                }

                response = await client.post(
                    f"/file-system/download",
                    json=request_data
                )
                if response.status_code >= 400:
                    logger.debug(errors.get(400202))
                    logger.debug(response.text)

                    return get_error_response(400202)

                json_response = GeneralResponse(**response.json())
                if json_response.code != 0:
                    return GeneralResponse(
                        code=json_response.code,
                        msg=json_response.msg
                    )

                data: Dict[str, str] = json_response.data
                work.data_file_path = data.get("full_file_path")

            db_session.add(task)
            db_session.commit()
            db_session.refresh(task)

            # request username by user_id
            response = await client.get(f"/accounts/{payload.user_id}")
            if response.status_code >= 400:
                logger.debug(errors.get(400202))
                logger.debug(response.text)

                return get_error_response(400202)

            json_response = GeneralResponse(**response.json())
            if json_response.code != 0:
                return GeneralResponse(
                    code=json_response.code,
                    msg=json_response.msg
                )

            data: Dict[str, str] = json_response.data
            username = data.get("username")

        work.submitted_by = username
        work.submitted_at = datetime.now()

        work.execute()
        work.started_at = datetime.now()

        db_session.add(work)
        db_session.commit()
        db_session.refresh(work)

        result = GeneralResponse(code=0, data=work)

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        result = get_error_response(400100)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def shut_down_import_work(work_id: str, db_session: Session):
    work = get_import_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    try:
        work.finish()
        db_session.add(work)
        db_session.commit()

        result = GeneralResponse(code=0, data=work)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def bulk_shut_down_import_work(work_id: str, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    try:
        terminating_tasks: List[Task] = db_session.query(Task)\
            .filter(Task.work_id == work_id)\
            .filter(Task.deleted_at == None)\
            .filter(Task.task_type == TaskTypeEnum.IMPORT)\
            .filter(Task.is_temporary == False)\
            .all()

        for task in terminating_tasks:
            celery_result = AsyncResult(task.resource_task_id)
            celery_result.revoke(terminate=True)

            task.manually_terminate()
            db_session.add(task)

        db_session.commit()

        result = GeneralResponse(code=0, data=work)

    except Exception as exception:
        logger.exception(exception)
        result = get_error_response(400000)

    return result


async def list_import_work_instances(
    db_session: Session,
    workspace_id: int,
    limit: int,
    offset: int,
):
    work_query = db_session.query(ImportWorkInstance)\
        .filter(ImportWorkInstance.deleted_at == None)\
        .filter(ImportWorkInstance.root_workspace_id == workspace_id)\
        .order_by(ImportWorkInstance.created_at.desc())

    items = work_query.limit(limit).offset(offset).all()
    total = work_query.count()

    data = {
        "items": items,
        "total": total,
    }

    return GeneralResponse(code=0, data=data)


async def search_import_work_instances(
    db_session: Session,
    workspace_id: int,
    limit: int,
    offset: int,
    keyword: str,
):
    work_query = db_session.query(ImportWorkInstance)\
        .filter(ImportWorkInstance.deleted_at == None)\
        .filter(ImportWorkInstance.root_workspace_id == workspace_id)\
        .filter(ImportWorkInstance.name.like(f"%{parse.unquote(keyword)}%"))\
        .order_by(ImportWorkInstance.created_at.desc())

    items = work_query.limit(limit).offset(offset).all()
    total = work_query.count()

    data = {
        "items": items,
        "total": total,
    }

    return GeneralResponse(code=0, data=data)


# other service

async def get_chuhe_db_info(workspace_id: int, authorization: str):
    try:
        result = {}
        base_url = settings.user_service
        async with AsyncClient(base_url=base_url) as client:
            response = await client.get(
                f"/workspaces/{workspace_id}",
                # headers={'authorization': authorization}
            )
            if response.status_code >= 400:
                logger.debug(errors.get(400202))
                logger.debug(response.text)

                return get_error_response(400202)

            json_response = GeneralResponse(**response.json())
            if json_response.code != 0:
                return GeneralResponse(
                    code=json_response.code,
                    msg=json_response.msg
                )

            data: Dict[str, str] = json_response.data
            result.setdefault("chuhe_db_host", data.get("chuhe_db_host"))

        return GeneralResponse(code=0, data=result)

    except (ConnectTimeout, ReadTimeout) as exception:
        logger.error(exception.request.url)
        return get_error_response(400213)

    except (RemoteProtocolError, ConnectError) as exception:
        logger.error(exception.request.url)
        return get_error_response(400100)

    except Exception as exception:
        logger.exception(exception)
        return get_error_response(400000)


async def sql_record(work_id: int, sql_id: int, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    if sql_id:
        sqls = db_session.query(SqlRecord)\
            .filter(SqlRecord.sql_id == sql_id)\
            .first()
    else:
        sqls = db_session.query(SqlRecord)\
            .filter(SqlRecord.status == 1, SqlRecord.work_id == work_id)\
            .all()

    return GeneralResponse(code=0, data=sqls)


async def delete_sql(work_id: int, log_id: int, db_session: Session):

    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    sql_query: SqlRecord = db_session.query(SqlRecord)\
        .filter(SqlRecord.sql_id == log_id)\
        .first()

    sql_query.status = 2
    db_session.add(sql_query)
    db_session.commit()

    return GeneralResponse(code=0, data="OK")


def str_to_timestamp(time_str: str):
    """
    把时间字符串改为时间戳
    """
    if not isinstance(time_str, str):
        time_str = str(time_str)
    timearray = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(timearray))

    return timestamp


def create_dag_python_file(
    user_id: int,
    sql_string: str,
    work: CommonWork,
    task: Task,
    connection: Connection,
):
    """
    向airfllow 发送调度请求
    """
    _node_id = get_uuid()

    temp_data = {
        "dag_id": task.uuid,
        "user_id": user_id,
        "work_id": work.work_id,
        "dag_name": task.name,
        "start_date": str_to_timestamp(str(work.started_at)) if work.started_at else int(time.time()),
        "end_date": str_to_timestamp(str(work.ended_at)) if work.ended_at else 0,
        "schedule_interval": work.cron_expression,
        "retries": work.failed_retry_times,
        "retry_delay": work.delay_minutes,
        "node_relations": "",
        "nodes": [
            {
                "node_id": _node_id,
                "node_type": 1,
                "node_name": "ch",
                "ip": connection.host,
                "port": connection.port,
                "username": connection.username or "default",
                "password": connection.password or "",
                "database": connection.database_name or "default",
                "sql": sql_string
            }
        ]
    }

    # export_util = ExportDagUtil(temp_data, "ch_template", "ch")
    # path = export_util.create_dag_py()

    return None


async def post_performance_indicators(
    task_uuid: str,
    query_id: str,
    db_session: Session,
):

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


async def get_performance_indicators(task_id: int, db_session: Session):
    analysis = db_session.query(Analysis).filter(
        Analysis.task_id == task_id).first()

    if analysis is None:
        return get_error_response(400203)

    return GeneralResponse(code=0, data=analysis)


async def get_sqls_lineage(payload: SqlLineageSchema):
    try:
        result = LineageRunner(payload.sqls)
        sources: List[Table] = []
        for source_table in result.source_tables:
            sources.append(source_table)

        targets: List[Table] = []
        for target_table in result.target_tables:
            targets.append(target_table)

        data = {
            "sources": [f"{s.schema}.{s.raw_name}" for s in sources],
            "targets": [f"{s.schema}.{s.raw_name}" for s in targets],
        }

        return GeneralResponse(code=0, data=data)

    except Exception as exception:
        logger.exception(exception)
        return GeneralResponse(code=4000000, msg=exception.args)


async def create_sql_logs(work_id: int, payload, db_session: Session):
    work: CommonWork = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    task: Task = db_session.query(Task).filter(
        Task.uuid == payload.task_uuid).first()
    if not task:
        return get_error_response(400203)

    count = 0
    for item in [payload.task_start, payload.task_end]:

        sql_log = SqlLog()
        sql_log.work_id = work_id
        sql_log.task_name = task.name
        sql_log.task_uuid = task.uuid
        sql_log.current_time = datetime.strptime(item, "%Y-%m-%d %H:%M:%S")

        if count == 0:
            sql_log.task_stats = "START"
            count += 1
        else:
            sql_log.task_stats = payload.state

        db_session.add(sql_log)
        db_session.commit()
        db_session.refresh(sql_log)

    return GeneralResponse(code=0, data="OK")


async def get_sql_logs(work_id: int, db_session: Session):
    work = get_common_work_by_id(work_id, db_session)
    if not work:
        return get_error_response(400203)

    sql_logs: SqlLog = db_session.query(SqlLog)\
        .filter(SqlLog.work_id == work_id).all()

    return GeneralResponse(code=0, data=sql_logs)
