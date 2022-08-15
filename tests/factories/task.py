#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid
import factory
from apps.models.task import Task
from apps.core.enums import TaskTypeEnum, TaskStatusEnum
from .common_work_instance import DAGWorkInstanceFactory


class TaskFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Task

    uuid = factory.LazyAttribute(lambda _: str(uuid.uuid4()))
    work_id = factory.SubFactory(DAGWorkInstanceFactory)
    is_temporary = False
    status = TaskStatusEnum.WAITTING
    task_type = TaskTypeEnum.DAG
    name = "task name"

