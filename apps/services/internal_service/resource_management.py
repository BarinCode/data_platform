#!/usr/bin/env python
# -*- coding: utf-8 -*-

from apps.config import settings
from .base import InternalBaseServcie


class ResourceManagementService(InternalBaseServcie):
    BASE_URL = settings.resource_service


    async def create_resource_group(self, work_id, workspace_id):
        url = "/resource_groups"
        payload = {
            "work_id": work_id,
            "workspace_id": workspace_id
        }
        return await self.request(url, "POST", payload=payload)

