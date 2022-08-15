import traceback
import subprocess
from datetime import datetime, timedelta
from os import path, stat
from typing import List, Dict
from dateutil import parser

from asgiref.sync import async_to_sync
from celery import Task as CeleryTask, states
from celery.exceptions import Ignore
from celery.result import AsyncResult
from clickhouse_driver import Client
from csv import DictReader
from loguru import logger
from sqlalchemy import or_
from sqlalchemy.orm import Session

from apps.core.enums import (
    DataSourceEnum,
    TaskStatusEnum,
    TaskTypeEnum,
    WorkStatusEnum,
    WriteTypeEnum,
    ScheduleEngineType,
    CategoryEnum
)
from apps.database import get_db_session
from apps.models import Task, CommonWorkInstance, ImportWorkInstance
from apps.schemas import GeneralResponse
from apps.services.work import (
    get_import_work_by_id,
    get_chuhe_db_info,
)
from apps.worker.celery_app import celery_app as app
from apps.utils.import_csv import row_reader
from apps.utils.airflow import AirflowService
from apps.config import settings
from apps.exceptions import AirflowException


# ! DO NOT CACHE THE SESSION
db_session: Session = next(get_db_session())

# TODO: use SQL template file instead.
drop_table_sql = """
DROP TABLE IF EXISTS {database_name}.{table_name}
"""

create_table_sql = """
CREATE TABLE {database_name}.{table_name} (
    {fields_string}
) ENGINE = {engine}
"""

insert_into_sql = """
INSERT INTO {database_name}.{table_name} VALUES
"""


@app.task(bind=True)
def import_data_from_csv_old(
    self: CeleryTask,
    csv_path: str,
    clickhouse_config: Dict[str, str],
    database_name: str,
    table_info: List,
    table_name: str,
    write_type: WriteTypeEnum,
    engine: str = "Memory",
):
    try:
        client = Client(**clickhouse_config)
        client.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        schema = {row["name"]: row["type"] for row in table_info}

        fields_name_string = ",".join(schema.keys())

        if write_type == WriteTypeEnum.OVERWRITE:
            fields_string = ",".join(f"{k} {v}" for k, v in schema.items())
            drop_table_string = drop_table_sql.format(
                database_name=database_name,
                table_name=table_name,
            )
            create_table_string = create_table_sql.format(
                database_name=database_name,
                table_name=table_name,
                fields_string=fields_string,
                engine=engine
            )

            client.execute(drop_table_string)
            client.execute(create_table_string)

            logger.debug(f"create table {table_name} finished")

        if not path.exists(csv_path):
            logger.error(f"{csv_path} does not exist")
            return False

        with open(csv_path, "r") as fh:
            row_generator = (row_reader(schema, r) for r in DictReader(fh))
            insert_into_string = insert_into_sql.format(
                database_name=database_name,
                table_name=table_name,
            )

            client.execute(insert_into_string, row_generator)

    except Exception as exception:
        logger.exception(exception)
        self.update_state(
            state=states.FAILURE,
            meta={
                'exc_type': type(exception).__name__,
                'exc_message': traceback.format_exc().split('\n'),
                # 'custom': '...'
            },
        )

        # TODO: import work log?
        # log_file_path = path.join(
        #     path.dirname(csv_path),
        #     f"{self.request.id}.log"
        # )
        # with open(log_file_path, "w") as fh:
        #     fh.write(traceback.format_exc())

        raise Ignore()

    return True


@app.task(bind=True)
def import_data_from_csv(
    self: CeleryTask,
    csv_path: str,
    ch_config: Dict[str, str],
    database_name: str,
    table_info: List,
    table_name: str,
    write_type: WriteTypeEnum,
    engine: str = "Memory",
):
    try:
        client = Client(**ch_config)
        client.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        schema = {row["name"]: row["type"] for row in table_info}

        if write_type == WriteTypeEnum.OVERWRITE:
            fields_string = ",".join(f"{k} {v}" for k, v in schema.items())
            drop_table_string = drop_table_sql.format(
                database_name=database_name,
                table_name=table_name,
            )
            create_table_string = create_table_sql.format(
                database_name=database_name,
                table_name=table_name,
                fields_string=fields_string,
                engine=engine
            )

            client.execute(drop_table_string)
            client.execute(create_table_string)

            logger.debug(f"create table {table_name} finished")

        if not path.exists(csv_path):
            logger.error(f"{csv_path} does not exist")
            return False

        host = ch_config.get("host", "local")
        port = ch_config.get("port", 9000)
        user = ch_config.get("user", "default")
        input_format_allow_errors_num = 100

        command = " ".join([
            "chuhe",
            "client",
            f"--host={host}",
            f"--port={port}",
            f"--user={user}",
            f"--database={database_name}",
            f"--input_format_allow_errors_num={input_format_allow_errors_num}",
            f"--query=\"INSERT INTO {table_name} FORMAT CSV\"",
            "<",
            csv_path,
        ])
        bufsize = 10 * 1024 * 1024
        process = subprocess.Popen(command, bufsize=bufsize, shell=True)
        process.communicate()

        if process.returncode != 0:
            logger.error(f"{csv_path} import failed")
            return False

    except Exception as exception:
        logger.exception(exception)
        self.update_state(
            state=states.FAILURE,
            meta={
                'exc_type': type(exception).__name__,
                'exc_message': traceback.format_exc().split('\n'),
                # 'custom': '...'
            },
        )

        raise Ignore()

    return True


@app.task
def sync_celery_task_state():
    unfinished_status = or_(
        Task.status == TaskStatusEnum.WAITTING,
        Task.status == TaskStatusEnum.STARTING,
        Task.status == TaskStatusEnum.RUNNING,
    )
    tasks: List[Task] = db_session.query(Task)\
        .filter(Task.deleted_at == None)\
        .filter(Task.is_temporary == False)\
        .filter(Task.task_type == TaskTypeEnum.IMPORT)\
        .filter(Task.work_id != None)\
        .filter(Task.started_at <= datetime.now())\
        .filter(Task.ended_at == None)\
        .filter(unfinished_status)\
        .all()

    for task in tasks:
        work = get_import_work_by_id(task.work_id, db_session)
        if not work:
            logger.warning(f"task-{task.task_id}: work items does not exist")
            task.fail()

            continue

        if not task.resource_task_id:
            logger.warning(f"task-{task.task_id}: celery task id is invalid")
            task.fail()

            continue

        # if task keeps pending status more than 1 day, mark it as failed
        if task.started_at + timedelta(days=1) <= datetime.now():
            logger.warning(f"task-{task.task_id}: pending too long")
            task.fail()

            continue

        celery_result = AsyncResult(task.resource_task_id)
        celery_status = celery_result.status
        if celery_status == 'PENDING':
            task.systemically_start()

        elif celery_status == 'STARTED':
            task.run()

        elif celery_status == 'SUCCESS':
            task.succeed()

        elif celery_status == 'FAILURE':
            task.fail()

        logger.debug(f"task-{task.task_id}: {celery_status} -> {task.status}")
        db_session.add(task)

    db_session.commit()


@app.task
def start_up_import_task():
    tasks: List[Task] = db_session.query(Task)\
        .filter(Task.deleted_at == None)\
        .filter(Task.is_temporary == False)\
        .filter(Task.task_type == TaskTypeEnum.IMPORT)\
        .filter(Task.work_id != None)\
        .filter(Task.started_at == None)\
        .filter(Task.ended_at == None)\
        .all()

    for task in tasks:
        work = get_import_work_by_id(task.work_id, db_session)
        if not work:
            logger.warning(f"work-{work.work_id}: work items does not exist")
            task.fail()

            continue

        if not path.exists(work.data_file_path):
            logger.warning(f"work-{work.work_id}: data file does not exist")
            task.fail()

            continue

        if work.data_source == DataSourceEnum.FTP:
            if work.ftp_file_size == 0:
                logger.warning(f"work-{work.work_id}: ftp file size is 0")
                task.fail()

                continue

            current_file_info = stat(work.data_file_path)
            current_file_size = current_file_info.st_size
            if current_file_size != work.ftp_file_size:
                continue

        result: GeneralResponse = async_to_sync(
            get_chuhe_db_info
        )(work.workspace_id, "fake_auth")
        if result.code != 0:
            logger.warning(f"get_chuhe_db_info failed: {result.msg}")

            continue

        data: Dict[str, str] = result.data
        clickhouse_config = {
            "host": data.get("chuhe_db_host"),
            # "database": work.database_name,
            # "port": data.get("chuhe_db_http_port"),
            # "user": "",
            # "password": "",
        }
        import_data_task: CeleryTask = import_data_from_csv
        task_result = import_data_task.apply_async(
            args=(
                work.data_file_path,
                clickhouse_config,
                work.database_name,
                work.table_info,
                work.table_name,
                work.write_type,
            ),
        )

        task.systemically_start()
        task.started_at = datetime.now()
        task.resource_task_id = task_result.task_id
        db_session.add(task)

    db_session.commit()


@app.task
def sync_import_work_state():
    work_query = db_session.query(ImportWorkInstance)\
        .filter(ImportWorkInstance.deleted_at == None)\
        .filter(ImportWorkInstance.status != WorkStatusEnum.FINISHED)

    unfinished_status = or_(
        Task.status == TaskStatusEnum.WAITTING,
        Task.status == TaskStatusEnum.STARTING,
        Task.status == TaskStatusEnum.RUNNING,
    )
    task_query = db_session.query(Task)\
        .filter(Task.deleted_at == None)\
        .filter(Task.is_temporary == False)\
        .filter(Task.task_type == TaskTypeEnum.IMPORT)\
        .filter(unfinished_status)

    for work in work_query.all():
        if task_query.filter(Task.work_id == work.work_id).count() == 0:
            work.to_FINISHED()
        else:
            work.to_EXECUTING()

        db_session.add(work)

    db_session.commit()


async def _sync_airflow_task_runs(db_session: Session):
    # TODO: using db_session as parameter is not a good way
    # fetch the sql and dag jobs
    pending_instances_query = db_session.query(CommonWorkInstance)\
        .filter(CommonWorkInstance.deleted_at == None)\
        .filter(CommonWorkInstance.started_at <= datetime.now())\
        .filter(or_(
            CommonWorkInstance.status == WorkStatusEnum.WAITTING,
            CommonWorkInstance.status == WorkStatusEnum.EXECUTING
        ))

    common_work_instances = pending_instances_query.all()
    dags = {}
    for instance in common_work_instances:
        dags[instance.uuid] = instance
   
    try:
        airflow_service = AirflowService(base_url=settings.airflow_service)
        # get dags
        raw_dags = await airflow_service.list_dags()
        inactive_dag_ids = [dag["dag_id"] for dag in raw_dags if dag["is_paused"]]
        inactive_instances = pending_instances_query\
            .filter(CommonWorkInstance.uuid.in_(inactive_dag_ids))\
            .all()
        for instance in inactive_instances:
            await airflow_service.resume_dag(instance.uuid)

        # get dag runs
        data = await airflow_service.list_dag_runs_batch(list(dags.keys()))
        dag_runs = {}
        for item in data:
            engine_id = f"airflow::{item['dag_id']}::{item['dag_run_id']}"
            dag_runs[engine_id] = item
        # find out existing tasks
        existing_tasks = db_session.query(Task).filter(Task.schedule_engine_id.in_(list(dag_runs.keys()))).all()
        existing_keys = [i.schedule_engine_id for i in existing_tasks]

        # insert
        pending_tasks = []
        pending_keys = set(dag_runs.keys()) - set(existing_keys)
        for key in pending_keys:
            dag_run = dag_runs[key]
            instance = dags[dag_run["dag_id"]]
            task_type = TaskTypeEnum.COMMON
            if instance.category == CategoryEnum.SQL:
                task_type = TaskTypeEnum.SQL
            elif instance.category == CategoryEnum.DAG:
                task_type = TaskTypeEnum.DAG

            # create nodes
            nodes = ""

            task = Task(
                work_id=instance.instance_id,
                task_type=task_type,
                name=key,
                schedule_engine_id=key,
                schedule_engine_type=ScheduleEngineType.AIRFLOW,
                nodes=nodes)

            pending_tasks.append(task)

        # update
        for task in existing_tasks+pending_tasks:
            dag_run = dag_runs[task.schedule_engine_id]
            task.schedule_engine_started_at = parser.parse(dag_run["start_date"])
            task.schedule_engine_ended_at = parser.parse(dag_run["end_date"])
            task.schedule_engin_state = dag_run["state"]
            # TODO: auto trigger in model
            task.status = TaskStatusEnum.WAITTING
            if dag_run["state"] == "queued":
                task.status = TaskStatusEnum.WAITTING
            elif dag_run["state"] == "running":
                task.status = TaskStatusEnum.RUNNING
            elif dag_run["state"] == "success":
                task.status = TaskStatusEnum.SUCCEEDED
            elif dag_run["state"] == "failed":
                task.status = TaskStatusEnum.FAILED
            else:
                logger.warnning("Unknow state: {} found in airflow.".format(dag_run["state"]))

            db_session.add(task)
        db_session.commit() 

            
    except AirflowException as e:
        logger.warning(e)
        return

@app.task
def sync_airflow_task_runs():
    async_to_sync(_sync_airflow_task_runs(db_session)) 
