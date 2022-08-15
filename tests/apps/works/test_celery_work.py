#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import pytest
from apps.models.work import CommonWork, CommonWorkInstance
from apps.worker.celery_worker import _sync_airflow_task_runs
from apps.core.enums import WorkStatusEnum
from tests.factories import SQLWorkInstanceFactory, DAGWorkInstanceFactory


@pytest.mark.anyio
async def test_sync_airflow_task_runs(client, db_session):
    sql_work_instance = SQLWorkInstanceFactory(
        started_at=datetime.now() - timedelta(days=1),
        status=WorkStatusEnum.WAITTING,
        uuid="ba48dfcb70f942f9a83185fd06e88a65"
    )
    dag_work_instance = DAGWorkInstanceFactory(
        started_at=datetime.now() - timedelta(days=1),
        status=WorkStatusEnum.EXECUTING
    )
    await _sync_airflow_task_runs(db_session)
    assert True
