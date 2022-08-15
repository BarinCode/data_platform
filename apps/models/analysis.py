from sqlalchemy import Column, String, DateTime, Enum, Integer, Boolean, Text, Float
from sqlalchemy.sql import func

from apps.core.enums import TaskTypeEnum, AnalysisState
from apps.database import Base


class Analysis(Base):
    __tablename__ = "analysis"

    analysis_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="指标ID")

    task_id = Column(
        Integer, comment="任务id")
    task_uuid = Column(
        String(32), unique=True, index=True, comment="任务UUID")

    task_type = Column(
        Enum(TaskTypeEnum), nullable=True,
        default=TaskTypeEnum.SQL, comment="任务类型")

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(
        DateTime(timezone=True), onupdate=func.now(), comment="更新时间")
    deleted_at = Column(
        DateTime(timezone=True), nullable=True, comment="删除时间")

    read_rows = Column(Integer, comment="读的行数")

    written_rows = Column(Integer, comment="写的行数")

    result_rows = Column(Integer, comment="结果的行数")

    memory_use = Column(Float, comment="内存使用情况")

    duration_ms = Column(Integer, comment="执行时间")

    query_id = Column(String(64), comment="对应sql执行id")

    query_end_time = Column(
        DateTime(timezone=True), comment="执行结束时间")

    query_state = Column(
        Enum(AnalysisState), nullable=True,
        default=AnalysisState.TO_START, comment="指标状态")

    connection_id = Column(Integer, comment="数据库连接id")
