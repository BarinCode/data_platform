from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, Json

from apps.core.enums import ConnectionStatusEnum, ConnectionTypeEnum


class ConnectionSchema(BaseModel):
    connection_type: Optional[ConnectionTypeEnum] = Field(
        None, description="连接类型")
    host: Optional[str] = Field(
        None, description="服务器地址")
    port: Optional[int] = Field(
        None, description="服务器端口")
    username: Optional[str] = Field(
        None, description="用户名")
    password: Optional[str] = Field(
        None, description="密码")
    database_name: Optional[str] = Field(
        None, description="数据库名称")

    zookeeper_host: Optional[str] = Field(
        None, description="zookeeper地址")
    zookeeper_port: Optional[int] = Field(
        None, description="zookeeper端口")

    connection_info: Optional[Json] = Field(
        None, description="连接参数")
    status: Optional[ConnectionStatusEnum] = Field(
        None, description="连接状态")
    auth_file_path: Optional[List[str]] = Field(
        None, description="连接认证文件路径")
    created_by: Optional[str] = Field(
        None, description="创建人")


class ConnectionCreateSchema(ConnectionSchema):
    name: str = Field(
        None, description="连接名称")
    workspace_id: int = Field(
        None, description="项目ID")
    created_by: str = Field(
        None, description="创建人")

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "name": "test",
                "workspace_id": 1,
                "created_by": "admin",
                "connection_type": ConnectionTypeEnum.KAFKA,
                "host": "localhost",
                "port": 9092,
                "username": "admin",
                "password": "admin",
                "database_name": "test",
                "zookeeper_host": "localhost",
                "zookeeper_port": 2181,
                "auth_file_path": ["/test", "/test2"],
            }
        }


class ConnectionUpdateSchema(ConnectionSchema):
    name: Optional[str] = Field(
        None, description="连接名称")
    workspace_id: int = Field(
        None, description="项目ID")

    class Config:
        orm_mode = True


class ConnectionTestSchema(BaseModel):
    connection_type: ConnectionTypeEnum = Field(
        default=ConnectionTypeEnum.CHUHEDB, description="连接类型")
    host: str = Field(
        default="localhost", description="服务器地址")
    port: int = Field(
        default=8000, description="服务器端口")
    username: Optional[str] = Field(
        default=None, description="用户名")
    password: Optional[str] = Field(
        default=None, description="密码")
    database_name: Optional[str] = Field(
        default=None, description="数据库名称")

    zookeeper_host: Optional[str] = Field(
        default=None, description="zookeeper地址")
    zookeeper_port: Optional[int] = Field(
        default=None, description="zookeeper端口")

    auth_file_path: Optional[List[str]] = Field(
        default=None, description="连接认证文件路径")

    class Config:
        orm_mode = True


class ConnectionBulkDeleteSchema(BaseModel):
    connection_ids: List[int] = Field(
        None, description="待删除连接ID列表")

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "connection_ids": [1, 2, 3],
            }
        }
