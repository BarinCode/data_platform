import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_get_tasks(client: AsyncClient):
    payload = {
        "name": "test",
        "description": "tests",
        "user_id": "fdfdf",
        "lambda_id": "fdfdfd",
    }
    work_response = await client.post("/v1/works", json=payload)
    work_json_repsonse = work_response.json()
    assert work_response.status_code == 201
    assert "code" in work_json_repsonse
    assert "data" in work_json_repsonse
    assert "work_id" in work_json_repsonse["data"]
    assert work_json_repsonse["code"] == 0

    work_id = work_response.json()["data"]["work_id"]
    task_response = await client.get(
        f"/v1/works/{work_id}/tasks",
        params={"limit": 20, "offset": 0}
    )
    task_json_response = task_response.json()
    assert task_response.status_code == 200
    assert "code" in task_json_response
    assert task_json_response["code"] == 0


@pytest.mark.anyio
async def test_get_task(client: AsyncClient):
    payload = {
        "name": "test",
        "description": "tests",
        "user_id": "fdfdf",
        "lambda_id": "fdfdfd",
    }
    work_response = await client.post("/v1/works", json=payload)
    work_id = work_response.json()["data"]["work_id"]

    task_response = await client.get(f"/v1/works/{work_id}/tasks/1")
    task_json_response = task_response.json()
    assert task_response.status_code == 200
    assert "code" in task_json_response
    assert task_json_response["code"] == 0
