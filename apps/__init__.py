__version__ = '1.1.0'


from fastapi import FastAPI

from apps.routers import v1_router
from apps.utils.get_consumer import (
    get_sync_consumer,
    get_async_consumer,
)


tags_metadata = [
    {
        "name": "system",
        "description": "获取系统信息接口",
    },
    {
        "name": "common-works",
        "description": "批/流处理作业相关接口",
    },
    {
        "name": "import-works",
        "description": "数据导入作业相关接口",
    },
    {
        "name": "tasks",
        "description": "任务接口",
    },
    {
        "name": "connections",
        "description": "连接管理接口",
    },
]


# consumer = get_sync_consumer()
# consumer = get_async_consumer()


app = FastAPI(
    title="work-management-service",
    version=__version__,
    openapi_tags=tags_metadata,
)
app.include_router(v1_router)


# @app.on_event("startup")
# async def startup_event():
#     try:
#         await consumer.start()

#     except KafkaConnectionError as error:
#         logger.error(error)

#     except Exception as exception:
#         logger.exception(exception)


# @app.on_event("shutdown")
# async def shutdown_event():
#     await consumer.stop()


# @app.on_event("startup")
# async def startup_event():
#     consumer.start()
#
#
# @app.on_event("shutdown")
# async def shutdown_event():
#     consumer.close()
