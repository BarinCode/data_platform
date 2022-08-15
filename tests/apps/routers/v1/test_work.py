#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from apps.models.work import CommonWork
from apps.config import settings
from apps.services.internal_service.user_management import UserManagementService
from apps.services.internal_service.resource_management import ResourceManagementService
from apps.exceptions import InternalServiceException
from tests.factories import ConnectionFactory, SQLWorkFactory


@pytest.mark.anyio
async def test_create_works(client, db_session, mocker):
    user_service = UserManagementService()
    resource_service = ResourceManagementService() 
    mocker.patch.object(user_service, "get_workspace_info", autospec=True, return_value={"code": 0, "data": {"root_id": 1, "asset_id": 1}})
    mocker.patch.object(user_service, "create_workspace_dir", autospec=True, return_value={"code": 0, "data": {"real_dir_path": "dir"}})
    mocker.patch.object(resource_service, "create_resource_group", autospec=True, return_value={"code": 0, "data": {"real_dir_path": "dir"}})

    connection = ConnectionFactory()
    sql = "SELECT 1"
    payload = {
        "workspace_id": 1,
        "user_id": 1,
        "name": "work name",
        "description": "work description",
        "category": "SQL",
        "connection_id": connection.connection_id,
        "executable_sql": sql
    }
    response = await client.post("/v1/common-works", json=payload)
    work = db_session.query(CommonWork).first()

    assert response.status_code == 201
    assert work.category == "SQL"
    assert work.executable_sql == sql
    assert db_session.query(CommonWork).count() == 1


@pytest.mark.anyio
async def test_create_works_failed(client, db_session, mocker):
    user_service = UserManagementService()
    resource_service = ResourceManagementService()
    mocker.patch.object(user_service, "get_workspace_info", autospec=True, return_value={"code": 0, "data": {"root_id": 1, "asset_id": 1}})
    mocker.patch.object(user_service, "create_workspace_dir", autospec=True, side_effect=InternalServiceException())
    mocker.patch.object(resource_service, "create_resource_group", autospec=True, return_value={"code": 0, "data": {"real_dir_path": "dir"}})

    connection = ConnectionFactory()
    sql = "SELECT * FROM tables"
    payload = {
        "workspace_id": 1,
        "user_id": 1,
        "name": "work name",
        "description": "work description",
        "category": "SQL",
        "connection_id": connection.connection_id,
        "executable_sql": sql
    }
    response = await client.post("/v1/common-works", json=payload)

    assert response.status_code == 201
    assert response.json()["code"] != 0
    assert db_session.query(CommonWork).count() == 0


@pytest.mark.anyio
async def test_put_common_work(client, db_session):
    work = SQLWorkFactory()
    payload = {
        "description": "new description",
        "name": "new name",
        "workspace_id": work.workspace_id
    }
    response = await client.put("/v1/common-works/{}".format(work.work_id), json=payload)
    data = response.json()

    assert response.status_code == 200
    assert db_session.query(CommonWork).count() == 1
    assert data["code"] == 0
    assert data["data"]["name"] == payload["name"]
    assert data["data"]["description"] == payload["description"]


@pytest.mark.anyio
async def test_put_common_work_with_duplicated_name(client, db_session):
    workspace_id = 1
    duplicated_work = SQLWorkFactory(workspace_id=workspace_id)
    work = SQLWorkFactory(workspace_id=workspace_id)
    payload = {
        "description": "new description",
        "name": duplicated_work.name,
        "workspace_id": workspace_id
    }

    response = await client.put("/v1/common-works/{}".format(work.work_id), json=payload)
    data = response.json()
    import ipdb; ipdb.set_trace()

    assert response.status_code == 200
    assert data["code"] == 400203


@pytest.mark.anyio
async def test_submit_sql_work():
    pass


@pytest.mark.anyio
async def test_submit_dag_work():
    pass


