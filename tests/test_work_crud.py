from typing import List
import pytest

from apps.models.work import CommonWork
from apps.schemas import GeneralResponse
from apps.schemas.work import CommonWorkCreateSchema
from apps.services import work as work_crud


@pytest.mark.anyio
async def test_create_work(db_session):
    payload = CommonWorkCreateSchema(**{
        "name": "test",
        "description": "tests",
        "user_id": 1,
        "workspace_id": 1,
    })
    work_1: CommonWork = await work_crud.create_common_work(
        payload, "dfdfd", db_session)
    work_2: CommonWork = await work_crud.get_common_work(
        work_1.work_id, db_session)
    assert work_1 == work_2


@pytest.mark.anyio
async def test_get_works(db_session):
    for index in range(1, 20):
        payload = CommonWorkCreateSchema(**{
            "name": f"testwork-{index}",
            "description": "tests",
            "user_id": 1,
            "type": "STREAM",
            "category": "FLINK_SQL",
        })
        await work_crud.create_common_work(payload, db_session)

    limit, offset, type, category = 8, 5, "STREAM", "FLINK_SQL"
    response: GeneralResponse = await work_crud.list_common_works(
        db_session, 1, limit, offset, type, category, False)

    items: List[CommonWork] = response.data["items"]
    total: int = response.data["total"]
    for index, work in enumerate(items, start=6):
        assert work.name == f"testwork-{index}"
    assert len(items) == 8
    assert total == 19


@pytest.mark.anyio
async def test_get_work(db_session):
    payload = CommonWorkCreateSchema(**{
        "name": "test",
        "description": "tests",
        "user_id": 1,
    })
    response_1: GeneralResponse = await work_crud.create_common_work(
        payload, db_session)
    work_1: CommonWork = response_1.data

    response_2: GeneralResponse = await work_crud.get_common_work(
        work_1.work_id, db_session)
    work_2: CommonWork = response_2.data

    assert isinstance(work_2, CommonWork)


@pytest.mark.anyio
async def test_update_work(db_session):
    payload_1 = CommonWorkCreateSchema(**{
        "name": "test",
        "description": "tests",
        "user_id": "fdfdf",
        "lambda_id": "fdfdfd",
    })
    work: CommonWork = await work_crud.create(payload_1, db_session)
    payload_2 = CommonWorkCreateSchema(**{
        "name": "test2",
        "description": "tests",
        "user_id": "fdfdf",
        "lambda_id": "fdfdfd",
    })
    work: CommonWork = await work_crud.update(
        work.work_id, payload_2, db_session
    )
    assert work.name == "test2"
    assert work.updated_at is not None


@pytest.mark.anyio
async def test_delete_work(db_session):
    payload = CommonWorkCreateSchema(**{
        "name": "test",
        "description": "tests",
        "user_id": "fdfdf",
        "lambda_id": "fdfdfd",
    })
    work: CommonWork = await work_crud.create(payload, db_session)
    work: CommonWork = await work_crud.delete(work.work_id, db_session)
    assert work.deleted_at is not None
