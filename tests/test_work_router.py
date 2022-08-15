import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_create_work(client: AsyncClient):
    payload = {
        "name": "test",
        "description": "tests",
        "user_id": "fdfdf",
        "lambda_id": "fdfdfd",
    }
    response = await client.post("/v1/works", json=payload)
    json_response = response.json()
    assert response.status_code == 201
    assert "code" in json_response
    assert json_response["code"] == 0


@pytest.mark.anyio
async def test_get_works(client: AsyncClient):
    response = await client.get(
        "/v1/works", params={
            "user_id": "fdfdf",
            "limit": 20,
            "offset": 0,
        }
    )
    json_response = response.json()
    assert response.status_code == 200
    assert "code" in json_response
    assert json_response["code"] == 0


@pytest.mark.anyio
async def test_get_work(client: AsyncClient):
    response = await client.get("/v1/works/100")
    json_response = response.json()
    assert response.status_code == 200
    assert "code" in json_response
    assert json_response["code"] == 0


@pytest.mark.anyio
async def test_update_work(client: AsyncClient):
    payload = {
        "name": "test",
        "description": "tests",
        "user_id": "fdfdf",
        "lambda_id": "fdfdfd",
    }
    response_create = await client.post("/v1/works", json=payload)
    json_response_create = response_create.json()
    work_id = json_response_create["data"]["work_id"]
    response_update = await client.put(
        f"/v1/works/{work_id}",
        json={"name": "test2"}
    )
    json_response_update = response_update.json()
    assert response_update.status_code == 200
    assert "code" in json_response_update
    assert "data" in json_response_update
    assert json_response_update["code"] == 0
    assert json_response_update["data"]["name"] == "test2"
    assert json_response_update["data"]["updated_at"] is not None


@pytest.mark.anyio
async def test_delete_work(client: AsyncClient):
    payload = {
        "name": "test",
        "description": "tests",
        "user_id": "fdfdf",
        "lambda_id": "fdfdfd",
    }
    response_create = await client.post("/v1/works", json=payload)
    json_response_create = response_create.json()
    work_id = json_response_create["data"]["work_id"]

    response_delete = await client.delete(f"/v1/works/{work_id}")
    json_response_delete = response_delete.json()
    assert response_delete.status_code == 200
    assert "code" in json_response_delete
    assert "data" in json_response_delete
    assert json_response_delete["code"] == 0
    assert json_response_delete["data"]["deleted_at"] is not None
