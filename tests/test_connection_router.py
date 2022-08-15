import pytest
from httpx import AsyncClient

from apps.schemas import GeneralResponse


@pytest.mark.anyio
async def test_create_connection(client: AsyncClient):
    payload = {
        "name": "test",
        "workspace_id": 1,
    }
    response = await client.post("/v1/connections", json=payload)
    json_response = response.json()

    assert response.status_code == 200
    assert "code" in json_response
    assert json_response["code"] == 0


@pytest.mark.anyio
async def test_get_connections(client: AsyncClient):
    response = await client.get(
        "/v1/connections", params={
            "workspace_id": 1,
            "limit": 20,
            "offset": 0,
        }
    )
    json_response = response.json()
    assert response.status_code == 200
    assert "code" in json_response
    assert json_response["code"] == 0


@pytest.mark.anyio
async def test_search_connections(client: AsyncClient):
    response = await client.get(
        "/v1/connections/search", params={
            "workspace_id": 1,
            "limit": 20,
            "offset": 0,
            "keyword": "test",
        }
    )
    json_response = response.json()
    assert response.status_code == 200
    assert "code" in json_response
    assert json_response["code"] == 0


@pytest.mark.anyio
async def test_get_connection(client: AsyncClient):
    response = await client.get("/v1/connections/100")
    json_response = response.json()
    assert response.status_code == 200
    assert "code" in json_response
    assert json_response["code"] == 400215


@pytest.mark.anyio
async def test_update_connection(client: AsyncClient):
    payload = {
        "name": "test",
        "workspace_id": 1,
        "auth_file_path": ["/example/path"],
    }
    response = await client.post("/v1/connections", json=payload)
    response = GeneralResponse(**response.json())

    connection_id = response.data["connection_id"]
    response_update = await client.put(
        f"/v1/connections/{connection_id}",
        json={"name": "test2"}
    )
    json_response_update = response_update.json()
    assert response_update.status_code == 200
    assert "code" in json_response_update
    assert "data" in json_response_update
    assert json_response_update["code"] == 0
    assert json_response_update["data"]["name"] == "test2"


@pytest.mark.anyio
async def test_delete_connection(client: AsyncClient):
    payload = {
        "name": "test",
        "workspace_id": 1,
    }
    response_create = await client.post("/v1/connections", json=payload)
    json_response_create = response_create.json()
    connection_id = json_response_create["data"]["connection_id"]

    response_delete = await client.delete(f"/v1/connections/{connection_id}")
    json_response_delete = response_delete.json()
    assert response_delete.status_code == 200
    assert "code" in json_response_delete
    assert "data" in json_response_delete
    assert json_response_delete["code"] == 0
