from typing import Dict

from fastapi import APIRouter
from loguru import logger

from apps.core import errors
from apps.schemas import GeneralResponse
from apps.utils.http_client import AirflowService


router = APIRouter(tags=["system"])


@router.get('/ping', response_model=str)
def ping():
    return 'pong'


@router.get('/errors', response_model=Dict[int, str])
def get_errors():
    return errors


@router.get('/test-airflow-service', deprecated=True)
async def test_airflow_service():
    result = []
    async with AirflowService() as service:
        result = await service.get_dags()
        logger.debug(result)

    return GeneralResponse(code=0, data=result)
