from sqlalchemy import Column, String, DateTime, Enum, Integer
from apps.database import Base
from apps.core.enums import SqlRunState


class SqlLog(Base):
    __tablename__ = "sql_log"

    sql_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="sql的ID")

    work_id = Column(
        Integer, index=True, nullable=False, comment="作业ID")

    task_name = Column(String(32), comment="作业任务的名称")

    task_id = Column(Integer, comment="作业任务的id")

    task_uuid = Column(String(32), comment="作业任务的uuid")

    task_stats = Column(
        Enum(SqlRunState), nullable=False,
        default=SqlRunState.START, comment="sql作业状态")

    current_time = Column(
        DateTime(timezone=True), nullable=True, comment="当前的时间")
