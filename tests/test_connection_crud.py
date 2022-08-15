import pytest
from typing import Dict, List

from apps.core.enums import ConnectionTypeEnum
from apps.models.connection import Connection
from apps.schemas import GeneralResponse
from apps.schemas.connection import (
    ConnectionCreateSchema,
    ConnectionUpdateSchema,
    ConnectionBulkDeleteSchema,
)
from apps.services.connection import (
    create_connection,
    get_connection,
    list_connections,
    update_connection,
    search_connections,
    delete_connection,
    delete_connections,
)


@pytest.mark.anyio
async def test_create_connection(db_session):
    payload = ConnectionCreateSchema(
        name="test",
        workspace_id=1,
        created_by="admin"
    )
    result_1: GeneralResponse = await create_connection(payload, db_session)

    connection: Connection = result_1.data
    result_2: GeneralResponse = await get_connection(
        connection.connection_id, db_session)

    assert result_1.data == result_2.data


@pytest.mark.anyio
async def test_get_connections(db_session):
    for index in range(1, 20):
        payload = ConnectionCreateSchema(
            name=f"test-{index}",
            workspace_id=1,
            created_by="admin",
            connection_type=ConnectionTypeEnum.CHUHEDB
        )
        await create_connection(payload, db_session)

    workspace_id, limit, offset = 1, 8, 5
    connection_type = ConnectionTypeEnum.CHUHEDB
    result: GeneralResponse = await list_connections(
        workspace_id, limit, offset, connection_type, db_session)
    data: Dict[str, str] = result.data
    items: List[Connection] = data.get("items")
    total: int = data.get("total")
    for index, connection in enumerate(items, start=6):
        assert connection.name == f"test-{index}"

    assert len(items) == 8
    assert total == 19


@pytest.mark.anyio
async def test_search_connections(db_session):
    for index in range(1, 20):
        payload = ConnectionCreateSchema(
            name=f"test-{index}",
            workspace_id=1,
            created_by="admin",
            connection_type=ConnectionTypeEnum.CHUHEDB
        )
        await create_connection(payload, db_session)

    workspace_id, limit, offset = 1, 8, 5
    result: GeneralResponse = await search_connections(
        workspace_id, limit, offset, "test-1", db_session)
    data: Dict[str, str] = result.data
    items: List[Connection] = data.get("items")
    total: int = data.get("total")

    assert len(items) == 6
    assert total == 11


@pytest.mark.anyio
async def test_get_connection(db_session):
    payload = ConnectionCreateSchema(
        name="test",
        workspace_id=1,
        created_by="admin"
    )
    result_1: GeneralResponse = await create_connection(payload, db_session)
    connection: Connection = result_1.data
    result_2: GeneralResponse = await get_connection(
        connection.connection_id, db_session)
    assert isinstance(result_2.data, Connection)


@pytest.mark.anyio
async def test_update_connection(db_session):
    payload_1 = ConnectionCreateSchema(
        name="test",
        workspace_id=1,
        created_by="admin"
    )
    result_1: GeneralResponse = await create_connection(payload_1, db_session)
    conn_1: Connection = result_1.data

    payload_2 = ConnectionUpdateSchema(name="test2")
    result_2: GeneralResponse = await update_connection(
        conn_1.connection_id, payload_2, db_session)
    conn_2: Connection = result_2.data

    assert conn_2.name == "test2"
    assert conn_2.updated_at is not None


@pytest.mark.anyio
async def test_delete_connection(db_session):
    payload = ConnectionCreateSchema(
        name="test",
        workspace_id=1,
        created_by="admin"
    )
    result_1: GeneralResponse = await create_connection(payload, db_session)
    conn_1: Connection = result_1.data
    result_2: GeneralResponse = await delete_connection(
        conn_1.connection_id, db_session)
    conn_2: Connection = result_2.data

    assert conn_2.deleted_at is not None


@pytest.mark.anyio
async def test_delete_connections(db_session):
    for index in range(1, 20):
        payload = ConnectionCreateSchema(
            name=f"test-{index}",
            workspace_id=1,
            created_by="admin",
            connection_type=ConnectionTypeEnum.CHUHEDB
        )
        await create_connection(payload, db_session)

    connection_ids = [n for n in range(1, 10)]
    payload = ConnectionBulkDeleteSchema(connection_ids=connection_ids)

    result_2: GeneralResponse = await delete_connections(payload, db_session)
    result: GeneralResponse = await list_connections(1, 10, 0, None, db_session)
    data: Dict[str, str] = result.data
    items: List[Connection] = data.get("items")
    total: int = data.get("total")

    assert len(items) == 10
    assert total == 10
