from datetime import datetime

from loguru import logger
from sqlalchemy import Column, String, DateTime, Enum, Integer, Boolean, Text, JSON
from sqlalchemy.orm import reconstructor
from sqlalchemy.sql import func
from sqlalchemy.event import listen
from transitions import Machine

from apps.core.enums import TaskStatusEnum, TaskTypeEnum, ScheduleEngineType
from apps.database import Base
from apps.utils import get_uuid


class Task(Base):
    __tablename__ = "tasks"

    # All states of work finite-state-machine
    fsm_states = [
        TaskStatusEnum.WAITTING,
        TaskStatusEnum.STARTING,
        TaskStatusEnum.RUNNING,
        TaskStatusEnum.SUCCEEDED,
        TaskStatusEnum.FAILED,
        TaskStatusEnum.TERMINATING,
        TaskStatusEnum.MANUAL_TERMINATED,
        TaskStatusEnum.SYSTEM_TERMINATED,
    ]

    task_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="任务ID")
    uuid = Column(
        String(32), unique=True, index=True, nullable=False,
        default=get_uuid, comment="任务UUID")

    work_id = Column(
        Integer, index=True, nullable=False, comment="作业ID")

    resource_task_id = Column(
        String(36), index=True, nullable=True, comment="第三方任务ID")
    is_temporary = Column(
        Boolean, nullable=True, default=False, comment="是否临时任务")

    status = Column(
        Enum(TaskStatusEnum), nullable=True,
        default=TaskStatusEnum.WAITTING, comment="任务状态")

    task_type = Column(
        Enum(TaskTypeEnum), nullable=True,
        default=TaskTypeEnum.COMMON, comment="任务类型")

    name = Column(
        String(36), nullable=True, comment="任务名称")
    excutable_file = Column(
        String(1023), nullable=True, comment="任务可执行文件")
    excutable_sql = Column(
        Text, nullable=True, comment="任务可执行sql")

    started_at = Column(
        DateTime(timezone=True), nullable=True, comment="开始时间")
    ended_at = Column(
        DateTime(timezone=True), nullable=True, comment="结束时间")

    schedule_engine_type = Column(
        Enum(ScheduleEngineType), nullable=False,
        default=ScheduleEngineType.AIRFLOW, comment="调度引擎种类")
    schedule_engine_id = Column(
        String(255), nullable=True, index=True, comment="调度引擎ID")
    schedule_engine_started_at = Column(
        DateTime(timezone=True), nullable=True, comment="调度引擎开始时间")
    schedule_engine_ended_at = Column(
        DateTime(timezone=True), nullable=True, comment="调度引擎结束时间")
    schedule_engine_state = Column(
        String(31), nullable=True, comment="调度引擎状态")
    nodes = Column(
        Text, nullable=True, comment="DAG节点")

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(
        DateTime(timezone=True), onupdate=func.now(), comment="更新时间")
    deleted_at = Column(
        DateTime(timezone=True), nullable=True, comment="删除时间")


    @property
    def state(self):
        return self.status

    @state.setter
    def state(self, value):
        if self.status != value:
            self.status = value

    def update_ended_at(self):
        self.ended_at = datetime.now()

    @classmethod
    def init_fsm(cls, instance, *args, **kwargs):
        initial = instance.status or TaskStatusEnum.WAITTING
        machine = Machine(
            model=instance,
            states=cls.fsm_states,
            initial=initial,
            ignore_invalid_triggers=True,
        )
        machine.add_transition(
            'manually_start',
            source=TaskStatusEnum.WAITTING,
            dest=TaskStatusEnum.STARTING,
        )
        machine.add_transition(
            'systemically_start',
            source=TaskStatusEnum.WAITTING,
            dest=TaskStatusEnum.STARTING,
        )
        machine.add_transition(
            'run',
            source=TaskStatusEnum.STARTING,
            dest=TaskStatusEnum.RUNNING,
        )
        machine.add_transition(
            'manually_terminate',
            source=[
                TaskStatusEnum.WAITTING,
                TaskStatusEnum.STARTING,
                TaskStatusEnum.RUNNING,
            ],
            dest=TaskStatusEnum.MANUAL_TERMINATED,
            after="update_ended_at",
        )
        machine.add_transition(
            'systemically_terminate',
            source=[
                TaskStatusEnum.WAITTING,
                TaskStatusEnum.STARTING,
                TaskStatusEnum.RUNNING,
            ],
            dest=TaskStatusEnum.SYSTEM_TERMINATED,
            after="update_ended_at",
        )
        machine.add_transition(
            'succeed',
            source=[
                TaskStatusEnum.STARTING,
                TaskStatusEnum.RUNNING,
            ],
            dest=TaskStatusEnum.SUCCEEDED,
            after="update_ended_at",
        )
        machine.add_transition(
            'fail',
            source=[
                TaskStatusEnum.WAITTING,
                TaskStatusEnum.STARTING,
                TaskStatusEnum.RUNNING,
            ],
            dest=TaskStatusEnum.FAILED,
            after="update_ended_at",
        )


listen(Task, 'init', Task.init_fsm, restore_load_context=True)
listen(Task, 'load', Task.init_fsm, restore_load_context=True)
