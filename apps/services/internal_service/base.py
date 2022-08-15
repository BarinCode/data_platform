#!/usr/bin/env python
# -*- coding: utf-8 -*-

from httpx import AsyncClient
from loguru import logger
from apps.exceptions import InternalServiceException
from apps.core.singleton import Singleton


class InternalBaseServcie(metaclass=Singleton):
    async def request(self, url, method, params={}, payload={}):
        async with AsyncClient(base_url=self.BASE_URL) as client:
            try:
                response = await client.request(
                    method,
                    url,
                    params=params,
                    json=payload
                )
            except Exception as e:
                logger.exception(e)
                raise

            if response.status_code > 299:
                message = f"Unexpected response with code: {response.status_code}"
                logger.error(message)

                raise InternalServiceException(
                    code=response.status_code,
                    url=url,
                    method=method,
                    message=message
                )

            return response.json()["data"]

