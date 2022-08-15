import os
import inspect
from typing import Any, Generator

import factory
import inflection
import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from pytest_factoryboy import register
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
import factory

from apps import app as asgi_app
from apps.database import Base, get_db_session
from . import factories
# from .utils.async_mock import setup_async_stub

# setup_async_stub()


# Default to using sqlite in memory for fast tests.
# Can be overridden by environment variable for testing in CI against other
# database engines
SQLALCHEMY_DATABASE_URL = os.getenv('TEST_DATABASE_URL', "sqlite://")

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={
        "check_same_thread": False
    }
)


@pytest.fixture()
def anyio_backend():
    return 'asyncio'


@pytest.fixture(autouse=True)
def app() -> Generator[FastAPI, Any, None]:
    """
    Create a fresh database on each test case.
    """
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # yield an asgi application
    yield asgi_app


@pytest.fixture()
def db_session() -> Generator[Session, Any, None]:
    """
    Creates a fresh sqlalchemy session for each test that operates in a
    transaction. The transaction is rolled back at the end of each test 
    ensuring a clean state.
    """

    # connect to the database
    connection = engine.connect()

    # begin a non-ORM transaction
    transaction = connection.begin()

    # bind an individual Session to the connection
    session = Session(bind=connection)

    # use the session in tests.
    yield session

    session.close()

    # rollback - everything that happened with the Session above (including
    # calls to commit()) is rolled back.
    transaction.rollback()

    # return connection to the Engine
    connection.close()


@pytest.fixture()
async def client(
    app: FastAPI,
    db_session: Session
) -> Generator[AsyncClient, Any, None]:
    """
    Create a new FastAPI TestClient that uses the `db_session` fixture to 
    override the `get_db` dependency that is injected into routes.
    """

    def _get_test_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db_session] = _get_test_db
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


@pytest.fixture(autouse=True)
def register_factories(db_session):
    for name in dir(factories):
        if name.startswith("__"):
            continue

        factory_cls = getattr(factories, name)
        if inspect.isclass(factory_cls) and issubclass(factory_cls, factory.alchemy.SQLAlchemyModelFactory):
            name = inflection.underscore(factory_cls.__name__)
            if name.endswith("_factory"):
                name = name[:-8]
            factory_cls._meta.sqlalchemy_session = db_session
            factory_cls._meta.sqlalchemy_session_persistence = "flush"
            register(factory_cls, name)

