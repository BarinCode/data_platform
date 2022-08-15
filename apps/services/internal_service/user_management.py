#!/usr/bin/env python
# -*- coding: utf-8 -*-

from apps.config import settings
from apps.exceptions import UserManagementServiceException
from .base import InternalBaseServcie


class UserManagementService(InternalBaseServcie):
    BASE_URL = settings.user_service


    async def get_workspace_info(self, workspace_id):
        url = "/workspaces/{}".format(workspace_id)
        return await self.request(url, "GET")


    async def create_workspace_dir(self, workspace_id, asset_id, dir_path):
        url = "/create-dir"
        payload = {
            "workspace_id": workspace_id,
            "asset_id": asset_id,
            "dir_path": dir_path
        }

        return await self.request(url, "POST", payload=payload)

