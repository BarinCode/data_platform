from typing import Optional
from fastapi import APIRouter, Depends, status
from loguru import logger
from sqlalchemy.orm import Session
from apps.core.enums import ConnectionTypeEnum

from apps.database import get_db_session
from apps.schemas import GeneralResponse
from apps.schemas.connection import (
    ConnectionBulkDeleteSchema,
    ConnectionCreateSchema,
    ConnectionTestSchema,
    ConnectionUpdateSchema,
)
from apps.services.connection import (
    create_connection,
    list_connections,
    get_connection,
    update_connection,
    search_connections,
    delete_connection,
    delete_connections,
    test_connection,
    get_tables_or_topics,
)


router = APIRouter(tags=["connections"])


@router.post(
    "/connections",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def create_item(
    payload: ConnectionCreateSchema,
    db_session: Session = Depends(get_db_session),
):
    return await create_connection(payload, db_session)


@router.get(
    "/connections",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def get_items(
    workspace_id: int,
    limit: int = 10,
    offset: int = 0,
    connection_type: Optional[ConnectionTypeEnum] = None,
    db_session: Session = Depends(get_db_session),
):
    return await list_connections(
        workspace_id, limit, offset, connection_type, db_session)


@router.get(
    "/connections/search",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def search_items(
    workspace_id: int,
    limit: int = 10,
    offset: int = 0,
    keyword: str = None,
    db_session: Session = Depends(get_db_session),
):
    return await search_connections(
        workspace_id, limit, offset, keyword, db_session)


@router.put(
    "/connections/bulk-delete",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_items(
    payload: ConnectionBulkDeleteSchema,
    db_session: Session = Depends(get_db_session),
):
    return await delete_connections(payload, db_session)


@router.put(
    "/connections/test",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def test_item(
    payload: ConnectionTestSchema,
    connection_id: int = None,
    db_session: Session = Depends(get_db_session),
):
    return await test_connection(payload, connection_id, db_session)


@router.get(
    "/connections/{connection_id}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def get_item(
    connection_id: int,
    db_session: Session = Depends(get_db_session),
):
    return await get_connection(connection_id, db_session)


@router.put(
    "/connections/{connection_id}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def update_item(
    connection_id: int,
    payload: ConnectionUpdateSchema,
    db_session: Session = Depends(get_db_session),
):
    return await update_connection(connection_id, payload, db_session)


@router.delete(
    "/connections/{connection_id}",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_item(
    connection_id: int,
    db_session: Session = Depends(get_db_session),
):
    return await delete_connection(connection_id, db_session)


@router.get(
    "/connections/{connection_id}/entrypoints",
    response_model=GeneralResponse,
    status_code=status.HTTP_200_OK,
)
async def get_entrypoints(
    connection_id: int,
    database_name: str = None,
    schema_name: str = None,
    table_name: str = None,
    db_session: Session = Depends(get_db_session),
):
    return await get_tables_or_topics(
        connection_id, database_name, schema_name, table_name, db_session)
