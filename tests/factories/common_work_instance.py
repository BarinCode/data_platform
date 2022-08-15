#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid
import factory
from apps.models.work import CommonWorkInstance
from apps.core.enums import WorkStatusEnum, CategoryEnum, JavaVersionEnum
from .common_work import SQLWorkFactory, DAGWorkFactory, SparkJarWorkFactory


class CommonWorkInstanceFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = CommonWorkInstance
        abstract = True

    uuid = factory.LazyAttribute(lambda _: str(uuid.uuid4()))
    user_id = factory.LazyAttribute(lambda o: o.work.user_id)
    workspace_id = factory.LazyAttribute(lambda o: o.work.workspace_id)
    root_workspace_id = factory.LazyAttribute(lambda o: o.work.root_workspace_id)
    name = factory.LazyAttribute(lambda o: o.work.name)
    description = factory.LazyAttribute(lambda o: o.work.description)
    status = WorkStatusEnum.WAITTING
    submitted_at = factory.LazyAttribute(lambda o: o.work.submitted_at)
    submitted_by = factory.LazyAttribute(lambda o: o.work.submitted_by)
    failed_retry_times = factory.LazyAttribute(lambda o: o.work.failed_retry_times)
    retry_delta_minutes = factory.LazyAttribute(lambda o: o.work.retry_delta_minutes)
    delay_minutes = factory.LazyAttribute(lambda o: o.work.delay_minutes)
    is_timeout_enabled = factory.LazyAttribute(lambda o: o.work.is_timeout_enabled)
    timeout_strategy = factory.LazyAttribute(lambda o: o.work.timeout_strategy)
    timeout_minutes = factory.LazyAttribute(lambda o: o.work.timeout_minutes)
    notice_strategy = factory.LazyAttribute(lambda o: o.work.notice_strategy)
    timeout_template_id = factory.LazyAttribute(lambda o: o.work.timeout_template_id)
    finish_template_id = factory.LazyAttribute(lambda o: o.work.finish_template_id)
    cron_expression = factory.LazyAttribute(lambda o: o.work.cron_expression)
    started_at = factory.LazyAttribute(lambda o: o.work.started_at)
    ended_at = factory.LazyAttribute(lambda o: o.work.ended_at)
    category = factory.LazyAttribute(lambda o: o.work.category)
    java_version = factory.LazyAttribute(lambda o: o.work.java_version)
    executable_file_path = factory.LazyAttribute(lambda o: o.work.executable_file_path)
    executable_file_name = factory.LazyAttribute(lambda o: o.work.executable_file_name)
    extra_params = factory.LazyAttribute(lambda o: o.work.extra_params)
    main_class_name = factory.LazyAttribute(lambda o: o.work.main_class_name)
    executable_sql = factory.LazyAttribute(lambda o: o.work.executable_sql)
    connection_id = factory.LazyAttribute(lambda o: o.work.connection_id)
    interval = factory.LazyAttribute(lambda o: o.work.interval)
    nodes = factory.LazyAttribute(lambda o: o.work.nodes)


class SQLWorkInstanceFactory(CommonWorkInstanceFactory):
    work = factory.SubFactory(SQLWorkFactory)


class DAGWorkInstanceFactory(CommonWorkInstanceFactory):
    work = factory.SubFactory(DAGWorkFactory)


class SparkJarWorkInstanceFactory(CommonWorkInstanceFactory):
    work = factory.SubFactory(SparkJarWorkFactory)

