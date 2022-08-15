from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, Json, root_validator

from apps.core.enums import (
    SyncTypeEnum,
    DataSourceEnum,
    OffsetTypeEnum,
    ReadTypeEnum,
    WriteTypeEnum,
    TimeoutStrategyEnum,
    NoticeStrategyEnum,

    WorkTypeEnum,
    CategoryEnum,
    WorkDependEnum,
    WorkStatusEnum,
    JavaVersionEnum,
    FrequencyEnum,
    ActionEnum,
    SqlRunState,
)


class WorkActionSchema(BaseModel):
    action: Optional[ActionEnum] = Field(
        None, description="作业动作")
    user_id: Optional[int] = Field(
        None, description="提交人 ID，action 为 submit 的时候需要")

    sql: Optional[str] = Field(
        None, description="sql的语句"
    )
    work_name: Optional[str] = Field(None, description="作业名称")

    class Config:
        orm_mode = True

    @root_validator
    def validate_submitted_by(cls, values: Dict):
        action = values.get("action")
        submitted_by = values.get("user_id")
        if action == ActionEnum.SUBMIT and submitted_by is None:
            raise ValueError('提交作业必须提供提交人ID')

        return values


class WorkBulkDeleteSchema(BaseModel):
    work_ids: List[int] = Field(
        None, description="待删除作业的ID列表")

    class Config:
        orm_mode = True


class WorkBase(BaseModel):
    failed_retry_times: Optional[int] = Field(
        None, description="重试次数")
    retry_delta_minutes: Optional[int] = Field(
        None, description="重试间隔分钟数")
    delay_minutes: Optional[int] = Field(
        None, description="延迟分钟数")
    is_timeout_enabled: Optional[bool] = Field(
        None, description="超时判断")
    timeout_strategy: Optional[TimeoutStrategyEnum] = Field(
        None, description="超时策略")
    timeout_minutes: Optional[int] = Field(
        None, description="超时分钟数")
    notice_strategy: Optional[NoticeStrategyEnum] = Field(
        None, description="通知策略")
    timeout_template_id: Optional[Union[int, str]] = Field(
        None, description="超时通知模版ID")
    finish_template_id: Optional[Union[int, str]] = Field(
        None, description="完成通知模版ID")
    cron_expression: Optional[str] = Field(
        None, description="cron表达式")
    started_at: Optional[datetime] = Field(
        None, description="开始时间")
    ended_at: Optional[datetime] = Field(
        None, description="结束时间")


class CommonWorkBase(WorkBase):
    executable_file_name: Optional[str] = Field(
        None, description="可执行文件名称")
    executable_file_path: Optional[str] = Field(
        None, description="可执行文件路径")
    main_class_name: Optional[str] = Field(
        None, description="主类名称")
    extra_params: Optional[str] = Field(
        None, description="自定义参数")
    submitted_by: Optional[str] = Field(
        None, description="提交人")

    category: Optional[CategoryEnum] = Field(
        None, description="作业类别")
    java_version: Optional[JavaVersionEnum] = Field(
        JavaVersionEnum.JAVA_SE_11, description="JAVA版本")

    # ! Will be deprecated in v1.4.0
    interval: Optional[int] = Field(
        None, description="运行间隔")
    # ! Will be deprecated in v1.4.0
    type: Optional[WorkTypeEnum] = Field(
        None, description="作业类型")
    # ! Will be deprecated in v1.4.0
    run_frequency: Optional[FrequencyEnum] = Field(
        None, description="运行频率")
    # ! Will be deprecated in v1.4.0
    run_dependent: Optional[WorkDependEnum] = Field(
        None, description="运行依赖")

    relation: Optional[str] = Field(
        None, description="自定义参数")
    edges: Optional[str] = Field(
        None, description="自定义参数")
    nodes: Optional[str] = Field(
        None, description="自定义参数")
    connection_id: Optional[int] = Field(
        None,  description="数据库连接id")


class CommonWorkCreateSchema(CommonWorkBase):
    workspace_id: int = Field(
        None, description="项目ID")
    user_id: int = Field(
        None, description="用户ID")
    name: str = Field(
        None, description="作业名称")
    description: Optional[str] = Field(
        None, description="作业描述")
    executable_sql: Optional[str] = Field(
        None, description="SQL作业的可执行SQL"
    )

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "workspace_id": 1,
                "user_id": 1,
                "name": "作业名称，必填",
                "description": "这个字段可选",
                "executable_sql": "SQL，选填"
            }
        }


class CommonWorkUpdateSchema(CommonWorkBase):
    workspace_id: int = Field(
        None, description="项目ID")
    name: Optional[str] = Field(
        None, description="作业名称")
    description: Optional[str] = Field(
        None, description="作业描述")
    executable_sql: Optional[str] = Field(
        None, description="sql语句")

    class Config:
        orm_mode = True


class ImportWorkBase(WorkBase):
    status: Optional[WorkStatusEnum] = Field(
        None, description="作业状态")
    data_source: Optional[DataSourceEnum] = Field(
        None, description="数据来源")
    write_type: Optional[WriteTypeEnum] = Field(
        None, description="写入方式")

    sync_type: Optional[SyncTypeEnum] = Field(
        None, description="同步类型")
    source_connection_id: Optional[Union[int, str]] = Field(
        None, description="数据源连接ID")
    dest_connection_id: Optional[int] = Field(
        None, description="目标连接ID")
    pg_read_type: Optional[ReadTypeEnum] = Field(
        None, description="读取方式")
    pg_identity_field: Optional[Union[str, Json]] = Field(
        None, description="增量识别字段")
    pg_where_clause: Optional[str] = Field(
        None, description="where条件")

    kafka_topic: Optional[str] = Field(
        None, description="要同步的topic")
    kafka_offset_type: Optional[OffsetTypeEnum] = Field(
        None, description="开始offset")
    kafka_started_at: Optional[datetime] = Field(
        None, description="数据开始时间")

    start_line_no: Optional[int] = Field(
        None, description="导入起始行")
    data_file_name: Optional[str] = Field(
        None, description="数据文件名称")
    data_file_path: Optional[str] = Field(
        None, description="数据文件路径")
    extra_params: Optional[str] = Field(
        None, description="自定义参数")
    ftp_url: Optional[str] = Field(
        None, description="FTP地址")
    ftp_file_size: Optional[int] = Field(
        None, description="FTP文件大小")

    source_database_name: Optional[str] = Field(
        None, description="数据源库名称")
    source_table_name: Optional[str] = Field(
        None, description="数据源表名称")
    source_table_info: Optional[Json] = Field(
        None, description="数据源表信息")

    database_name: Optional[str] = Field(
        None, description="数据库名称")
    table_name: Optional[str] = Field(
        None, description="表名称")
    table_info: Optional[Json] = Field(
        None, description="表DESC信息")
    create_table_params: Optional[str] = Field(
        None, description="建表参数")
    submitted_by: Optional[str] = Field(
        None, description="提交人")


class ImportWorkCreateSchema(ImportWorkBase):
    workspace_id: int = Field(
        None, description="项目ID")
    user_id: int = Field(
        None, description="用户ID")
    name: str = Field(
        None, description="作业名称")
    description: Optional[str] = Field(
        None, description="作业描述")

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "workspace_id": 1,
                "user_id": 1,
                "name": "作业名称，必填",
                "description": "这个字段可选",
            }
        }


class ImportWorkUpdateSchema(ImportWorkBase):
    workspace_id: Optional[int] = Field(
        None, description="项目ID")
    name: Optional[str] = Field(
        None, description="作业名称")
    description: Optional[str] = Field(
        None, description="作业描述")

    class Config:
        orm_mode = True


class SqlSchema(BaseModel):
    user_id: int = Field(None, description="用户id")
    work_id: int = Field(None, description="作业id")
    work_name: str = Field(None, description="作业名称")
    connection_id: int = Field(None, description="数据库连接id")
    connection_name: str = Field(None, description="数据库名称")

    sql: str = Field(
        None, description="sql的语句"
    )

    class Config:
        orm_mode = True


class SqlLineageSchema(BaseModel):
    sqls: str = Field(None, description="SQL语句，多句用半角分号分隔")

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "sqls": "insert into db1.table1 select * from db2.table2;" +
                "insert into db3.table3 select * from db1.table1;",
            }
        }


class SqlLogsSchema(BaseModel):
    task_uuid: str = Field(None, description="作业任务的uuid")
    task_start: str = Field(None, description="作业开始的时间")
    task_end: str = Field(None, description="作业结束的时间")
    state: Optional[SqlRunState] = Field(None, description="作业任务运行的状态")

    class Config:
        orm_mode = True
