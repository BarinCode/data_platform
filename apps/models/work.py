from datetime import datetime

from sqlalchemy import Column, String, DateTime, Integer, Enum, JSON, Boolean, Text, UniqueConstraint, ForeignKey
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.event import listen
from sqlalchemy.sql import func
from sqlalchemy.orm import aliased, relationship
from transitions import Machine

from apps.core.enums import (
    NoticeStrategyEnum,
    ReadTypeEnum,
    TimeoutStrategyEnum,
    WorkTypeEnum,
    CategoryEnum,
    WorkDependEnum,
    WorkStatusEnum,
    JavaVersionEnum,
    FrequencyEnum,

    OffsetTypeEnum,
    SyncTypeEnum,
    DataSourceEnum,
    WriteTypeEnum,
)
from apps.database import Base
from apps.utils import get_uuid


class WorkMixin:

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    fsm_states = [
        WorkStatusEnum.WAITTING,
        WorkStatusEnum.EXECUTING,
        WorkStatusEnum.FINISHED,
    ]

    uuid = Column(
        String(32), unique=True, index=True, nullable=False,
        default=get_uuid, comment="作业UUID")
    user_id = Column(
        Integer, index=True, nullable=False, default=0, comment="用户ID")
    workspace_id = Column(
        Integer, index=True, nullable=False, default=0, comment="项目ID")
    root_workspace_id = Column(
        Integer, index=True, nullable=False, default=0, comment="根项目ID")
    name = Column(
        String(100), index=True, nullable=False, comment="作业名称")
    description = Column(
        String(1023), nullable=True, comment="作业描述")
    status = Column(
        Enum(WorkStatusEnum), nullable=True,
        default=WorkStatusEnum.WAITTING, comment="作业状态")

    submitted_at = Column(
        DateTime(timezone=True), nullable=True, comment="提交时间")
    submitted_by = Column(
        String(100), nullable=True, comment="提交人")

    failed_retry_times = Column(
        Integer, nullable=True, default=0, comment="重试次数")
    retry_delta_minutes = Column(
        Integer, nullable=True, default=0, comment="重试间隔分钟数")
    delay_minutes = Column(
        Integer, nullable=True, default=0, comment="延迟分钟数")
    is_timeout_enabled = Column(
        Boolean, nullable=True, default=True, comment="超时判断")
    timeout_strategy = Column(
        Enum(TimeoutStrategyEnum), nullable=True,
        default=TimeoutStrategyEnum.SEND_MESSAGE, comment="超时策略")
    timeout_minutes = Column(
        Integer, nullable=True, default=0, comment="超时分钟数")
    notice_strategy = Column(
        Enum(NoticeStrategyEnum), nullable=True,
        default=NoticeStrategyEnum.ALL, comment="通知策略")
    timeout_template_id = Column(
        Integer, nullable=True, default=0, comment="超时通知模版ID")
    finish_template_id = Column(
        Integer, nullable=True, default=0, comment="完成通知模版ID")
    cron_expression = Column(
        String(1023), nullable=True, comment="cron表达式")

    started_at = Column(
        DateTime(timezone=True), nullable=True, comment="开始时间")
    ended_at = Column(
        DateTime(timezone=True), nullable=True, comment="结束时间")
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(
        DateTime(timezone=True), onupdate=func.now(), comment="更新时间")
    deleted_at = Column(
        DateTime(timezone=True), nullable=True, comment="删除时间")

    UniqueConstraint("workspace_id", "name", deleted_at != None)

    @property
    def state(self):
        return self.status

    @state.setter
    def state(self, value):
        if self.status != value:
            self.status = value


class CommonWork(WorkMixin, Base):
    __tablename__ = "common_works"

    work_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="作业ID")

    category = Column(
        Enum(CategoryEnum), nullable=True,
        default=CategoryEnum.SQL, comment="作业类别")
    java_version = Column(
        Enum(JavaVersionEnum), nullable=True,
        default=JavaVersionEnum.JAVA_SE_11, comment="JAVA版本")

    # ! Will be deprecated in v1.4.0 >>>
    type = Column(
        Enum(WorkTypeEnum), nullable=True,
        default=WorkTypeEnum.BATCH, comment="作业类型")
    run_frequency = Column(
        Enum(FrequencyEnum), nullable=True,
        default=FrequencyEnum.ONCE, comment="运行频率")
    run_dependent = Column(
        Enum(WorkDependEnum), nullable=True,
        default=WorkDependEnum.ON_NOTHING, comment="运行依赖")
    interval = Column(
        Integer, nullable=True, comment="运行间隔")
    finished_batch_count = Column(
        Integer, nullable=True, comment="已运行任务批次数量", default=0)
    # ! Will be deprecated in v1.4.0 <<<

    work_directory = Column(
        String(1023), nullable=True, comment="工作目录")
    executable_file_name = Column(
        String(1023), nullable=True, comment="可执行文件名称")
    executable_file_path = Column(
        String(1023), nullable=True, comment="可执行文件路径")

    # fields for SPARK_JAR work
    main_class_name = Column(
        String(1023), nullable=True, comment="主类名称")
    extra_params = Column(
        String(1023), nullable=True, comment="自定义参数")

    # fields for SQL work
    executable_sql = Column(
        Text, nullable=True, comment="可执行sql")
    connection_id = Column(
        Integer, index=True, nullable=False, default=0, comment="连接数据库id")

    # just for dag work
    relation = Column(
        Text, nullable=True, comment="节点关系")
    edges = Column(
        Text, nullable=True, comment="DAG边")
    nodes = Column(
        Text, nullable=True, comment="DAG节点")

    instances = relationship("CommonWorkInstance")

    @classmethod
    def init_fsm(cls, instance, *args, **kwargs):
        initial = instance.status or WorkStatusEnum.WAITTING
        machine = Machine(
            model=instance,
            states=cls.fsm_states,
            initial=initial,
            ignore_invalid_triggers=True,
        )
        machine.add_transition(
            'execute',
            source=[
                WorkStatusEnum.WAITTING,
                WorkStatusEnum.EXECUTING,
            ],
            dest=WorkStatusEnum.EXECUTING,
        )
        machine.add_transition(
            'finish',
            source=[
                WorkStatusEnum.WAITTING,
                WorkStatusEnum.EXECUTING,
            ],
            dest=WorkStatusEnum.FINISHED,
        )


listen(CommonWork, 'init', CommonWork.init_fsm, restore_load_context=True)
listen(CommonWork, 'load', CommonWork.init_fsm, restore_load_context=True)


class CommonWorkInstance(WorkMixin, Base):
    __tablename__ = "common_work_instances"

    instance_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="作业实例ID")
    work_id = Column(
        Integer, ForeignKey("common_works.work_id"), index=True, nullable=False, default=0, comment="作业ID")

    category = Column(
        Enum(CategoryEnum), nullable=True,
        default=CategoryEnum.SQL, comment="作业类别")
    java_version = Column(
        Enum(JavaVersionEnum), nullable=True,
        default=JavaVersionEnum.JAVA_SE_11, comment="JAVA版本")

    # ! Will be deprecated in v1.4.0 >>>
    type = Column(
        Enum(WorkTypeEnum), nullable=True,
        default=WorkTypeEnum.BATCH, comment="作业类型")
    run_frequency = Column(
        Enum(FrequencyEnum), nullable=True,
        default=FrequencyEnum.ONCE, comment="运行频率")
    run_dependent = Column(
        Enum(WorkDependEnum), nullable=True,
        default=WorkDependEnum.ON_NOTHING, comment="运行依赖")
    interval = Column(
        Integer, nullable=True, comment="运行间隔")
    finished_batch_count = Column(
        Integer, nullable=True, comment="已运行任务批次数量", default=0)
    # ! Will be deprecated in v1.4.0 <<<

    work_directory = Column(
        String(1023), nullable=True, comment="工作目录")
    executable_file_name = Column(
        String(1023), nullable=True, comment="可执行文件名称")
    executable_file_path = Column(
        String(1023), nullable=True, comment="可执行文件路径")

    # fields for SPARK_JAR work
    main_class_name = Column(
        String(1023), nullable=True, comment="主类名称")
    extra_params = Column(
        String(1023), nullable=True, comment="自定义参数")

    # fields for SQL work
    executable_sql = Column(
        Text, nullable=True, comment="可执行sql")
    connection_id = Column(
        Integer, index=True, nullable=False, default=0, comment="连接数据库id")

    # fields for DAG work
    relation = Column(
        Text, nullable=True, comment="节点关系")
    edges = Column(
        Text, nullable=True, comment="DAG边")
    nodes = Column(
        Text, nullable=True, comment="DAG节点")

    work = relationship("CommonWork", back_populates="instances")

    @classmethod
    def init_fsm(cls, instance, *args, **kwargs):
        initial = instance.status or WorkStatusEnum.WAITTING
        machine = Machine(
            model=instance,
            states=cls.fsm_states,
            initial=initial,
            ignore_invalid_triggers=True,
        )
        machine.add_transition(
            'execute',
            source=[
                WorkStatusEnum.WAITTING,
                WorkStatusEnum.EXECUTING,
            ],
            dest=WorkStatusEnum.EXECUTING,
        )
        machine.add_transition(
            'finish',
            source=[
                WorkStatusEnum.WAITTING,
                WorkStatusEnum.EXECUTING,
            ],
            dest=WorkStatusEnum.FINISHED,
        )


listen(
    CommonWorkInstance, 'init', CommonWorkInstance.init_fsm,
    restore_load_context=True)
listen(
    CommonWorkInstance, 'load', CommonWorkInstance.init_fsm,
    restore_load_context=True)


class ImportWork(WorkMixin, Base):
    __tablename__ = "import_works"

    work_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="作业ID")

    sync_type = Column(
        Enum(SyncTypeEnum), nullable=True,
        default=SyncTypeEnum.OFFLINE, comment="同步类型")

    data_source = Column(
        Enum(DataSourceEnum), nullable=True,
        default=DataSourceEnum.ASSET, comment="导入作业来源")

    source_connection_id = Column(
        Integer, index=True, nullable=False, default=0, comment="数据源连接ID")
    pg_read_type = Column(
        Enum(ReadTypeEnum), nullable=True,
        default=ReadTypeEnum.DELTA, comment="读取方式")
    pg_identity_field = Column(
        String(100), nullable=True, comment="增量识别字段")
    pg_where_clause = Column(
        String(1023), nullable=True, comment="where条件")

    kafka_topic = Column(
        String(1023), nullable=True, comment="要同步的topic")
    kafka_offset_type = Column(
        Enum(OffsetTypeEnum), nullable=True,
        default=OffsetTypeEnum.LATEST, comment="开始offset")
    kafka_started_at = Column(
        DateTime(timezone=True), nullable=True, comment="数据开始时间")

    start_line_no = Column(
        Integer, nullable=True, default=2, comment="导入开始行")
    data_file_name = Column(
        String(1023), nullable=True, comment="导入数据文件名")
    data_file_path = Column(
        String(1023), nullable=True, comment="导入数据文件路径")
    extra_params = Column(
        String(1023), nullable=True, comment="自定义参数")
    ftp_url = Column(
        String(1023), nullable=True, comment="FTP地址")
    ftp_file_size = Column(
        Integer, nullable=True, default=0, comment="FTP文件大小")

    source_database_name = Column(
        String(100), nullable=True, comment="数据源库名称")
    source_table_name = Column(
        String(100), nullable=True, comment="数据源表名称")
    source_table_info = Column(
        JSON, nullable=True, comment="数据源表信息")

    dest_connection_id = Column(
        Integer, index=True, nullable=False, default=0, comment="目标连接ID")
    write_type = Column(
        Enum(WriteTypeEnum), nullable=True,
        default=WriteTypeEnum.OVERWRITE, comment="写入方式")
    database_name = Column(
        String(100), nullable=True, comment="数据库名称")
    table_name = Column(
        String(100), nullable=True, comment="表名称")
    table_info = Column(
        JSON, nullable=True, comment="表DESC信息")
    create_table_params = Column(
        String(1023), nullable=True, comment="建表参数")

    def should_later_than_now(self):
        return datetime.now() <= self.started_at if self.started_at else False

    @ classmethod
    def init_fsm(cls, instance, *args, **kwargs):
        initial = instance.status or WorkStatusEnum.WAITTING
        machine = Machine(
            model=instance,
            states=cls.fsm_states,
            initial=initial,
            ignore_invalid_triggers=True,
        )
        machine.add_transition(
            'execute',
            source=[
                WorkStatusEnum.WAITTING,
                WorkStatusEnum.EXECUTING,
            ],
            dest=WorkStatusEnum.EXECUTING,
            conditions="should_later_than_now",
        )
        machine.add_transition(
            'finish',
            source=[
                WorkStatusEnum.WAITTING,
                WorkStatusEnum.EXECUTING,
            ],
            dest=WorkStatusEnum.FINISHED,
        )


listen(ImportWork, 'init', ImportWork.init_fsm, restore_load_context=True)
listen(ImportWork, 'load', ImportWork.init_fsm, restore_load_context=True)


class ImportWorkInstance(WorkMixin, Base):
    __tablename__ = "import_work_instances"

    instance_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="作业实例ID")
    work_id = Column(
        Integer, index=True, nullable=False, default=0, comment="作业ID")

    sync_type = Column(
        Enum(SyncTypeEnum), nullable=True,
        default=SyncTypeEnum.OFFLINE, comment="同步类型")

    data_source = Column(
        Enum(DataSourceEnum), nullable=True,
        default=DataSourceEnum.ASSET, comment="导入作业来源")

    source_connection_id = Column(
        Integer, index=True, nullable=False, default=0, comment="数据源连接ID")
    pg_read_type = Column(
        Enum(ReadTypeEnum), nullable=True,
        default=ReadTypeEnum.DELTA, comment="读取方式")
    pg_identity_field = Column(
        String(100), nullable=True, comment="增量识别字段")
    pg_where_clause = Column(
        String(1023), nullable=True, comment="where条件")

    kafka_topic = Column(
        String(1023), nullable=True, comment="要同步的topic")
    kafka_offset_type = Column(
        Enum(OffsetTypeEnum), nullable=True,
        default=OffsetTypeEnum.LATEST, comment="开始offset")
    kafka_started_at = Column(
        DateTime(timezone=True), nullable=True, comment="数据开始时间")

    start_line_no = Column(
        Integer, nullable=True, default=2, comment="导入开始行")
    data_file_name = Column(
        String(1023), nullable=True, comment="导入数据文件名")
    data_file_path = Column(
        String(1023), nullable=True, comment="导入数据文件路径")
    extra_params = Column(
        String(1023), nullable=True, comment="自定义参数")
    ftp_url = Column(
        String(1023), nullable=True, comment="FTP地址")
    ftp_file_size = Column(
        Integer, nullable=True, default=0, comment="FTP文件大小")

    source_database_name = Column(
        String(100), nullable=True, comment="数据源库名称")
    source_table_name = Column(
        String(100), nullable=True, comment="数据源表名称")
    source_table_info = Column(
        JSON, nullable=True, comment="数据源表信息")

    dest_connection_id = Column(
        Integer, index=True, nullable=False, default=0, comment="目标连接ID")
    write_type = Column(
        Enum(WriteTypeEnum), nullable=True,
        default=WriteTypeEnum.OVERWRITE, comment="写入方式")
    database_name = Column(
        String(100), nullable=True, comment="数据库名称")
    table_name = Column(
        String(100), nullable=True, comment="表名称")
    table_info = Column(
        JSON, nullable=True, comment="表DESC信息")
    create_table_params = Column(
        String(1023), nullable=True, comment="建表参数")

    def should_later_than_now(self):
        return datetime.now() <= self.started_at if self.started_at else False

    @ classmethod
    def init_fsm(cls, instance, *args, **kwargs):
        initial = instance.status or WorkStatusEnum.WAITTING
        machine = Machine(
            model=instance,
            states=cls.fsm_states,
            initial=initial,
            ignore_invalid_triggers=True,
        )
        machine.add_transition(
            'execute',
            source=[
                WorkStatusEnum.WAITTING,
                WorkStatusEnum.EXECUTING,
            ],
            dest=WorkStatusEnum.EXECUTING,
            conditions="should_later_than_now",
        )
        machine.add_transition(
            'finish',
            source=[
                WorkStatusEnum.WAITTING,
                WorkStatusEnum.EXECUTING,
            ],
            dest=WorkStatusEnum.FINISHED,
        )


listen(
    ImportWorkInstance, 'init', ImportWorkInstance.init_fsm,
    restore_load_context=True)
listen(
    ImportWorkInstance, 'load', ImportWorkInstance.init_fsm,
    restore_load_context=True)


# ! Will be deprecated in v1.4.0
class WorkVersion(Base):
    __tablename__ = "work_versions"

    version_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="版本ID")
    uuid = Column(
        String(32), unique=True, index=True, nullable=False,
        default=get_uuid, comment="版本UUID")
    work_id = Column(
        Integer, index=True, nullable=False, comment="作业ID")

    submitted_by = Column(
        String(32), nullable=False, comment="提交人")
    submitted_at = Column(
        DateTime(timezone=True), nullable=True, comment="提交时间")

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(
        DateTime(timezone=True), onupdate=func.now(), comment="更新时间")
    deleted_at = Column(
        DateTime(timezone=True), nullable=True, comment="删除时间")
