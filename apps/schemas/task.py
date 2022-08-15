from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from apps.core.enums import TaskStatusEnum, ActionEnum, TaskTypeEnum, TaskNodeStatusEnum


class TaskBase(BaseModel):
    name: str
    work_id: int
    status: Optional[TaskStatusEnum]
    task_type: Optional[TaskTypeEnum] = TaskTypeEnum.COMMON

    started_at: Optional[datetime]
    ended_at: Optional[datetime]


class TaskCreateSchema(TaskBase):

    class Config:
        orm_mode = True


class TaskSchema(TaskBase):
    task_id: int

    class Config:
        orm_mode = True


class ActionSchema(BaseModel):
    action: ActionEnum

    class Config:
        orm_mode = True


