#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from typing import Any
from httpx import AsyncClient, Response
from loguru import logger
from apps.exceptions import AirflowException, AirflowAuthException, AirflowUnknowException, AirflowForbiddenException


class AirflowService:
    def __init__(self, base_url: str, username: str = "admin", password: str = "admin") -> None:
        self.base_url = base_url
        self.username = username
        self.password = password

    async def _request(self, method: str, url: str, params: dict = {}, payload: dict = {}):
        url = os.path.join(self.base_url, url)
        try:
            async with AsyncClient(base_url=self.base_url, auth=(self.username, self.password)) as client:
                response = await client.request(
                    method,
                    url,
                    params=params,
                    json=payload
                )

                if response.status_code == 401:
                    raise AirflowAuthException(response.json()["title"])

                if response.status_code == 403:
                    raise AirflowForbiddenException(response.json()["title"])

                if response.status_code != 200:
                    raise AirflowUnknowException(response.json()["title"])

                return response.json()
        except AirflowException as e:
            logger.warning("Request to airflow servic failure: {}".format(e))
            raise

    async def list_dags(self) -> list:
        url = "/dags"
        data = await self._request("GET", url=url, params={"limit": 1000, "only_active": False})
        return data["dags"]

    async def resume_dag(self, dag_id: str) -> None:
        url = "/dags/{}".format(dag_id)
        data = await self._request("PATCH", url=url, payload={"is_paused": False})

        if data and data["is_paused"] is False and data["is_active"] is True:
            return
        raise AirflowUnknowException("Can not resume dag: {}".format(dag_id))

    async def list_dag_runs(self, dag_id: str) -> dict:
        # TODO: using infinity request
        url = "/dags/{}/dagRuns".format(dag_id)
        data = await self._request("GET", url, params={"limit": 1000})
        return data["dag_runs"]

    async def get_dag_run_info(self, dag_id: str, dag_run_id: str) -> dict:
        url = "/dags/{}/dagRuns/{}/taskInstances".format(dag_id, dag_run_id)
        data = await self._request("GET", url, params={"limit": 1000})
        return data

    async def list_dag_runs_batch(self, dag_ids: list) -> dict:
        url = "/dags/~/dagRuns/list"
        data = await self._request("POST", url, payload={"page_limit": 1000, "dag_ids": dag_ids})
        return data["dag_runs"]
