#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from tests.factories import ConnectionFactory


@pytest.mark.anyio
async def test_get_connections(client):
    connection = ConnectionFactory()

    response = await client.post("/v1/connections", json={
        "name": connection.name,
        "workspace_id": connection.workspace_id
    })

    data = response.json()

    assert response.status_code == 200
    assert data["code"] == 0
