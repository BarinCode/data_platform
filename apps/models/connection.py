from sqlalchemy import JSON, Column, String, DateTime, Enum, Integer, Boolean
from sqlalchemy.sql import func

from apps.core.enums import ConnectionTypeEnum, ConnectionStatusEnum
from apps.database import Base
from apps.utils import get_uuid


class Connection(Base):
    __tablename__ = "connections"

    connection_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="连接ID")
    uuid = Column(
        String(32), unique=True, index=True, nullable=False,
        default=get_uuid, comment="任务UUID")
    workspace_id = Column(
        Integer, index=True, nullable=False, comment="项目ID")
    name = Column(
        String(100), nullable=True, comment="连接名称")

    host = Column(
        String(100), nullable=True, comment="服务器地址")
    port = Column(
        Integer, nullable=True, comment="服务器端口")
    username = Column(
        String(100), nullable=True, comment="用户名")
    password = Column(
        String(100), nullable=True, comment="密码")
    database_name = Column(
        String(100), nullable=True, comment="数据库名称")

    # A better way is store those info JSON field
    zookeeper_host = Column(
        String(100), nullable=True, comment="zookeeper地址")
    zookeeper_port = Column(
        Integer, nullable=True, comment="zookeeper端口")

    connection_type = Column(
        Enum(ConnectionTypeEnum), nullable=True,
        default=ConnectionTypeEnum.KAFKA, comment="连接类型")
    connection_info = Column(
        JSON, nullable=True, comment="连接参数")
    status = Column(
        Enum(ConnectionStatusEnum), nullable=True,
        default=ConnectionStatusEnum.SUCCEED, comment="连接状态")
    auth_file_path = Column(
        String(1023), nullable=True, comment="连接认证文件路径")

    created_by = Column(
        String(100), nullable=True, comment="创建人")

    is_enabled = Column(
        Boolean, nullable=True, default=False, comment="是否启用")
    last_connected_at = Column(
        DateTime(timezone=True), nullable=True, comment="最近连接时间")

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(
        DateTime(timezone=True), onupdate=func.now(), comment="更新时间")
    deleted_at = Column(
        DateTime(timezone=True), nullable=True, comment="删除时间")
