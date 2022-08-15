from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from urllib import parse

from clickhouse_driver import Client
from clickhouse_driver.errors import UnexpectedPacketFromServerError
from confluent_kafka.avro import AvroConsumer
from loguru import logger
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from apps.core.enums import ConnectionTypeEnum, ConnectionStatusEnum
from apps.core.errors import errors
from apps.models.connection import Connection
from apps.schemas import GeneralResponse, get_error_response
from apps.schemas.connection import (
    ConnectionBulkDeleteSchema,
    ConnectionCreateSchema,
    ConnectionTestSchema,
    ConnectionUpdateSchema,
)


def get_connection_by_id(
    connection_id: int,
    db_session: Session,
) -> Optional[Connection]:
    return db_session.query(Connection)\
        .filter(Connection.connection_id == connection_id)\
        .filter(Connection.deleted_at == None)\
        .first()


def get_connection_by_name(name: str, workspace_id: int, db_session: Session):
    return db_session.query(Connection)\
        .filter(Connection.name == name)\
        .filter(Connection.workspace_id == workspace_id)\
        .filter(Connection.deleted_at == None)\
        .first()


async def create_connection(
    payload: ConnectionCreateSchema,
    db_session: Session,
):
    is_duplicate = get_connection_by_name(
        payload.name,
        payload.workspace_id,
        db_session,
    )
    if is_duplicate:
        return get_error_response(400500)

    try:
        auth_file_path_string = None
        if payload.auth_file_path:
            auth_file_path_string = ";".join(payload.auth_file_path)
            payload.dict().pop("auth_file_path")

        connection = Connection(**payload.dict())
        connection.is_enabled = True
        connection.auth_file_path = auth_file_path_string

        db_session.add(connection)
        db_session.commit()
        db_session.refresh(connection)

        return GeneralResponse(code=0, data=connection)

    except Exception as exception:
        logger.exception(exception)
        return get_error_response(400000)


async def list_connections(
    workspace_id: int,
    limit: int,
    offset: int,
    connection_type: ConnectionTypeEnum,
    db_session: Session,
):
    connection_query = db_session.query(Connection)\
        .filter(Connection.deleted_at == None)\
        .filter(Connection.workspace_id == workspace_id)\

    if connection_type:
        connection_query = connection_query\
            .filter(Connection.connection_type == connection_type)

    items = connection_query\
        .order_by(Connection.created_at.desc())\
        .limit(limit).offset(offset).all()
    total = connection_query.count()

    data = {
        "items": items,
        "total": total
    }

    return GeneralResponse(code=0, data=data)


async def search_connections(
    workspace_id: int,
    limit: int,
    offset: int,
    keyword: str,
    db_session: Session,
):
    work_query = db_session.query(Connection)\
        .filter(Connection.deleted_at == None)\
        .filter(Connection.workspace_id == workspace_id)\
        .filter(Connection.name.like(f"%{parse.unquote(keyword)}%"))\
        .order_by(Connection.created_at.desc())

    items = work_query.limit(limit).offset(offset).all()
    total = work_query.count()

    data = {
        "items": items,
        "total": total,
    }

    return GeneralResponse(code=0, data=data)


async def get_connection(
    connection_id: int,
    db_session: Session,
):
    connection = get_connection_by_id(connection_id, db_session)
    if not connection:
        return get_error_response(400215)

    return GeneralResponse(code=0, data=connection)


async def update_connection(
    connection_id: int,
    payload: ConnectionUpdateSchema,
    db_session: Session,
):
    connection = get_connection_by_id(connection_id, db_session)
    if not connection:
        return get_error_response(400215)

    if payload.name and payload.workspace_id:
        is_duplicate = get_connection_by_name(
            payload.name,
            payload.workspace_id,
            db_session
        )
        if is_duplicate and payload.name != connection.name:
            return get_error_response(400500)

    connection.name = (
        payload.name or connection.name
    )

    connection.connection_type = (
        payload.connection_type
        or connection.connection_type
    )
    connection.connection_info = (
        payload.connection_info
        or connection.connection_info
    )
    #connection.status = payload.status or connection.status
    connection.status = ConnectionStatusEnum.SUCCEED
    connection.host = payload.host or connection.host
    connection.port = payload.port or connection.port
    connection.username = payload.username or connection.username
    connection.password = payload.password or connection.password
    connection.zookeeper_host = (
        payload.zookeeper_host
        or connection.zookeeper_host
    )
    connection.zookeeper_port = (
        payload.zookeeper_port
        or connection.zookeeper_port
    )

    connection.auth_file_path = (
        ";".join(payload.auth_file_path or [])
        or connection.auth_file_path
    )
    connection.created_by = payload.created_by or connection.created_by

    db_session.add(connection)
    db_session.commit()
    db_session.refresh(connection)

    return GeneralResponse(code=0, data=connection)


async def delete_connection(
    connection_id: int,
    db_session: Session,
):
    connection = get_connection_by_id(connection_id, db_session)
    if not connection:
        return get_error_response(400215)

    connection.deleted_at = datetime.now()

    db_session.add(connection)
    db_session.commit()
    db_session.refresh(connection)

    return GeneralResponse(code=0, data=connection)


async def delete_connections(
    payload: ConnectionBulkDeleteSchema,
    db_session: Session,
):
    for connection_id in payload.connection_ids:
        connection = get_connection_by_id(connection_id, db_session)
        if not connection:
            continue

        connection.deleted_at = datetime.now()
        db_session.add(connection)

    db_session.commit()

    return GeneralResponse(code=0, data=None)


async def test_connection(
    payload: ConnectionTestSchema,
    connection_id: Optional[int],
    db_session: Session,
):
    if connection_id:
        connection = get_connection_by_id(connection_id, db_session)
        if not connection:
            return get_error_response(400215)

        if connection.connection_type == ConnectionTypeEnum.CHUHEDB:
            kwargs = {
                "host": connection.host,
                "port": connection.port,
                "user": connection.username,
            }
            if connection.password:
                kwargs['password'] = connection.password
            if connection.database_name:
                kwargs['database'] = connection.database_name

            client = Client(**kwargs)

            client.connection.force_connect()
            result = client.connection.connected
            if not result:
                return get_error_response(400216)
            client.disconnect()

        elif connection.connection_type == ConnectionTypeEnum.POSTGRESQL:
            schema = "postgresql"
            engine = create_engine(
                "{schema}://{user}:{password}@{host}:{port}/{db_name}".format(
                    schema=schema,
                    user=connection.username,
                    password=connection.password,
                    host=connection.host,
                    port=connection.port,
                    db_name=connection.database_name,
                )
            )
            try:
                client = engine.connect()
                result = client.execute("SELECT 1")
                if not result:
                    return get_error_response(400216)

            except OperationalError as exception:
                logger.error(exception.args)
                return get_error_response(400216)

            except Exception as exception:
                logger.exception(exception)
                return get_error_response(400000)

        elif connection.connection_type == ConnectionTypeEnum.KAFKA:
            client = AvroConsumer()
            pass

        connection.last_connected_at = datetime.now()
        db_session.add(connection)
        db_session.commit()

        return GeneralResponse(code=0, data=None)

    if payload.connection_type == ConnectionTypeEnum.CHUHEDB:
        kwargs = {
            "host": payload.host,
            "port": payload.port,
            "user": payload.username,
        }
        if payload.password:
            kwargs['password'] = payload.password
        if payload.database_name:
            kwargs['database'] = payload.database_name

        client = Client(**kwargs)
        client.connection.force_connect()
        result = client.connection.connected
        if not result:
            return get_error_response(400216)
        client.disconnect()

    elif payload.connection_type == ConnectionTypeEnum.POSTGRESQL:
        schema = "postgresql"
        engine = create_engine(
            "{schema}://{user}:{password}@{host}:{port}/{db_name}".format(
                schema=schema,
                user=payload.username,
                password=payload.password,
                host=payload.host,
                port=payload.port,
                db_name=payload.database_name,
            )
        )
        try:
            client = engine.connect()
            result = client.execute("SELECT 1")
            if not result:
                return get_error_response(400216)

        except OperationalError as exception:
            logger.error(exception.args)
            return get_error_response(400216)

        except Exception as exception:
            logger.exception(exception)
            return get_error_response(400216)

    elif payload.connection_type == ConnectionTypeEnum.KAFKA:
        # client = AvroConsumer()
        pass

    return GeneralResponse(code=0, data=None)


async def get_tables(
    connection: Connection,
    database_name: str,
    schema_name: str,
    table_name: str,
):
    schema = "postgresql"
    engine = create_engine(
        "{schema}://{user}:{password}@{host}:{port}/{db_name}".format(
            schema=schema,
            user=connection.username,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            db_name=connection.database_name or database_name,
        )
    )

    try:
        inspector: Inspector = inspect(engine)

        if not database_name and not table_name:
            result_proxy = engine.execute("SELECT datname FROM pg_database")
            result: List[str] = [row[0] for row in result_proxy.fetchall()]

            return GeneralResponse(code=0, data=result)

        if not schema_name and not table_name:
            result_proxy = engine.execute(
                "SELECT table_name FROM information_schema.tables "
                f"where table_catalog = '{database_name}'"
            )
            result: List[str] = [row[0] for row in result_proxy.fetchall()]
            return GeneralResponse(code=0, data=result)

        if schema_name and not table_name:
            result_proxy = engine.execute(
                "SELECT table_name FROM information_schema.tables "
                f"where table_catalog = '{database_name}' "
                f"and table_schema = '{schema_name}' "
            )
            result: List[str] = [row[0] for row in result_proxy.fetchall()]
            return GeneralResponse(code=0, data=result)

        result_proxy = engine.execute(f"""
            select 
                a.attname as column_name,
                format_type(a.atttypid,a.atttypmod) as data_type,
                (case when (
                        select count(*) from pg_constraint 
                        where conrelid = a.attrelid
                        and conkey[1]=attnum and contype='p'
                    ) > 0 then true else false end
                ) as P,
                (case when a.attnotnull = true then true else false end) as nullable
            from pg_attribute a
            where attstattarget=-1 
            and attrelid = (select oid from pg_class where relname ='{table_name}')
            """)
        result: List[Any] = result_proxy.fetchall()
        return GeneralResponse(code=0, data=result)

    except OperationalError as exception:

        logger.error(exception.args)
        return get_error_response(400216)

    except Exception as exception:
        logger.exception(exception)
        return get_error_response(400000)


async def get_topics(connection: Connection):
    pass


async def get_chuhe_tables(
    connection: Connection,
    database_name: str,
    table_name: str,
):
    kwargs = {
        "host": connection.host,
        "port": connection.port,
        "user": connection.username,
    }
    if connection.password:
        kwargs['password'] = connection.password
    # if connection.database_name:
    #    kwargs['database'] = connection.database_name

    try:
        client = Client(**kwargs)
        databases_proxy = client.execute("show databases")
        databases: List[str] = [row[0] for row in databases_proxy]

    except UnexpectedPacketFromServerError as exception:
        logger.error(errors.get(400219))
        return get_error_response(400219)

    except Exception as exception:
        logger.exception(exception)
        return get_error_response(400000)

    if not database_name and not table_name:
        return GeneralResponse(code=0, data=databases)

    if not table_name:
        if database_name not in databases:
            result = []
        else:
            result_proxy = client.execute(f"show tables in {database_name}")
            result: List[str] = [row[0] for row in result_proxy]

        return GeneralResponse(code=0, data=result)

    table_info: List[Set] = client.execute(
        f"desc {database_name}.{table_name}")
    fields = []
    for field_info in table_info:
        field = {
            "name": field_info[0],
            "type": field_info[1],
            "default_type": field_info[2],
            "default_expression": field_info[3],
            "comment": field_info[4],
            "codec_expression": field_info[5],
            "ttl_expression":  field_info[6],
        }
        fields.append(field)

    return GeneralResponse(code=0, data=fields)


async def get_tables_or_topics(
    connection_id: int,
    database_name: str,
    schema_name: str,
    table_name: str,
    db_session: Session,
):
    connection = get_connection_by_id(connection_id, db_session)
    if not connection:
        return get_error_response(400215)

    if connection.connection_type == ConnectionTypeEnum.POSTGRESQL:
        return await get_tables(connection, database_name, schema_name, table_name)

    elif connection.connection_type == ConnectionTypeEnum.KAFKA:
        # entrypoints = await get_topics(connection)
        entrypoints = ["topic1", "topic2"]

    elif connection.connection_type == ConnectionTypeEnum.CHUHEDB:
        return await get_chuhe_tables(connection, database_name, table_name)

    else:
        entrypoints = []

    return GeneralResponse(code=0, data=entrypoints)
