from sqlalchemy import JSON, Column, String, DateTime, Enum, Integer, Boolean, Text, Float
from sqlalchemy.sql import func

from apps.core.enums import ConnectionTypeEnum, ConnectionStatusEnum
from apps.database import Base
from apps.utils import get_uuid
from apps.core.enums import SqlStateEnum


class SqlRecord(Base):
    __tablename__ = "sqlrecord"

    sql_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="sql的ID")

    user_id = Column(
        Integer, index=True, nullable=False, default=0, comment="用户ID")

    work_id = Column(
        Integer, index=True, nullable=False, comment="作业ID")

    sql = Column(Text, nullable=False, comment="sql语句")

    stats = Column(
        Enum(SqlStateEnum), nullable=False,
        default=SqlStateEnum.SUCCEED, comment="sql状态")

    status = Column(
        Integer, default=1, nullable=False,
        comment="数据是否有用，1表示有用，2表示数据删除")

    started = Column(
        DateTime(timezone=True), nullable=True, comment="开始时间")

    ended = Column(
        DateTime(timezone=True), nullable=True, comment="结束时间")

    rows = Column(
        Integer, comment="数据库行数")

    result = Column(
        Integer, default=1, comment="sql运行的结果， 其中1：表示成功， 2表示失败")

    work_name = Column(
        String(36), nullable=True, comment="作业的名称")

    durations = Column(Float, nullable=True, comment="持续时间")
