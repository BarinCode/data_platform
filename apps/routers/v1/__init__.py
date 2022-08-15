
from fastapi import APIRouter

from apps.routers.v1.system import router as system_router
from apps.routers.v1.task import router as task_router
from apps.routers.v1.work import router as work_router
from apps.routers.v1.connection import router as connection_router


router = APIRouter(prefix='/v1')
router.include_router(system_router)
router.include_router(task_router)
router.include_router(work_router)
router.include_router(connection_router)
