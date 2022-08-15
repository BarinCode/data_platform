#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid
import json
import factory
from apps.models.work import CommonWork
from apps.core.enums import WorkStatusEnum, CategoryEnum, JavaVersionEnum
from .connection import ConnectionFactory


class CommonWorkFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = CommonWork
        abstract = True

    uuid = factory.LazyAttribute(lambda _: str(uuid.uuid4()))
    user_id = 1
    workspace_id = 1
    root_workspace_id = 1
    name = factory.LazyAttribute(lambda _: str(uuid.uuid4()))
    description = "work description"
    status = WorkStatusEnum.WAITTING


class SparkJarWorkFactory(CommonWorkFactory):
    category = CategoryEnum.SPARK_JAR
    java_version = JavaVersionEnum.JAVA_SE_11
    executable_file_path = "/mnt/run.jar"
    executable_file_name = "run.jar"
    main_class_name = "Main"


class SQLWorkFactory(CommonWorkFactory):
    category = CategoryEnum.SQL
    executable_sql = "select 1;"
    connection_id = factory.LazyAttribute(lambda _: ConnectionFactory().connection_id)


def create_nodes(work, count=1):
    nodes = []
    for idx in range(count):
        sql_work = SQLWorkFactory(
            user_id=work.user_id,
            workspace_id=work.workspace_id,
            root_workspace_id=work.root_workspace_id
        )
        nodes.append({
            "position": {"top": 0, "left": 0},
            "nodeId": idx,
            "nodeName": "node_{}".format(idx),
            "description": "",
            "workId": sql_work.work_id,
            "parentIds": [] if idx == 0 else [idx-1],
            "failedRetryTimes": 1,
            "retryDeltaMinutes": 0,
            "delayMinutes": 0,
            "isTimeoutEnabled": False,
            "timeoutStrategy": ["SEND_MESSAGE"],
            "timeoutMinutes": 0,
            "timeoutTemplateId": ""
        })

    return nodes

class DAGWorkFactory(CommonWorkFactory):
    category = CategoryEnum.DAG
    nodes = factory.LazyAttribute(lambda o: json.dumps(create_nodes(o, 4)))


