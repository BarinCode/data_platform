"""
A base service for client requests.
"""


from typing import Any, Dict, List, Optional, Set
import asyncio

from httpx import AsyncClient
from loguru import logger

from apps.config import settings


class BaseClient:
    """A simple client for requests."""

    base_url = None
    default_auth = ("admin", "admin")
    default_params = {"limit": 10, "offset": 0}

    def __init__(self, *args, **kwargs) -> None:
        self.client = AsyncClient(
            *args,
            base_url=self.base_url,
            auth=self.default_auth,
            params=self.default_params,
            **kwargs,
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            logger.error(f"{self.base_url} error: {exc_val}")

        await self.client.aclose()


class AirflowService(BaseClient):
    base_url = settings.airflow_service
    dag_url = "/dags"
    dag_task_url = "/dags/{dag_id}/tasks"
    dag_run_url = "/dags/{dag_id}/dagRuns"
    dag_task_instance_url = "/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    async def get_dags(self, *args, **kwargs) -> List[Dict]:
        raw_response = await self.client.get(self.dag_url, *args, **kwargs)
        json_response: Dict = raw_response.json()

        return json_response.get("dags", [])

    async def get_dag_tasks(self, dag_id: str,  *args, **kwargs) -> List[Dict]:
        request_url = self.dag_task_url.format(dag_id=dag_id)
        raw_response = await self.client.get(request_url, *args, **kwargs)
        json_response: Dict = raw_response.json()

        return json_response.get("dag_tasks", [])

    async def get_dag_runs(
        self, dag_id: str, *args, **kwargs
    ) -> List[Dict]:
        request_url = self.dag_run_url.format(dag_id=dag_id)
        raw_response = await self.client.get(request_url, *args, **kwargs)
        json_response: Dict = raw_response.json()

        return json_response.get("dag_runs", [])

    async def get_dag_task_instances(
        self, dag_id: str, dag_run_id: str, *args, **kwargs
    ) -> List[Dict]:
        request_url = self.dag_task_instance_url.format(
            dag_id=dag_id, dag_run_id=dag_run_id)
        raw_response = await self.client.get(request_url, *args, **kwargs)
        json_response: Dict = raw_response.json()

        return json_response.get("task_instances", [])


class UserManagementService(BaseClient):
    base_url = settings.user_service

    def __init__(self, url: str, auth: Set) -> None:
        super().__init__(url, auth)

    async def get_workspace(data: Dict):
        pass

    async def get_workspace_tree(data: Dict):
        pass

    async def get_username(data: Dict):
        pass

    async def create_directory(data: Dict):
        pass

    async def create_directory(data: Dict):
        pass


class WorkManagementService(BaseClient):
    base_url = settings.work_service

    def __init__(self, url: str, auth: Set) -> None:
        super().__init__(url, auth)


class ResourceManagementService(BaseClient):
    base_url = settings.resource_service

    def __init__(self, url: str, auth: Set) -> None:
        super().__init__(url, auth)

    async def get_resources(data: Dict):
        pass

    async def create_resource(data: Dict):
        pass


async def main():
    async with AirflowService() as service:
        print(await service.get_dags())


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
