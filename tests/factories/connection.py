#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid
import factory
from apps.models.connection import Connection
from apps.core.enums import ConnectionTypeEnum


class ConnectionFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Connection

    uuid = factory.LazyAttribute(lambda _: str(uuid.uuid4()))
    workspace_id = 1
    name = "connection name"
    host = "localhost"
    port = "9000"
    username = "username"
    password = "password"
    database_name = "default"
    connection_type = ConnectionTypeEnum.CHUHEDB
