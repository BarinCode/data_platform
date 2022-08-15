import logging
from datetime import timedelta

from celery import Celery
from celery.signals import setup_logging
from loguru import logger

from apps.config import settings, celery_config


celery_app = Celery()
celery_app.config_from_object(celery_config)


class InterceptHandler(logging.Handler):

    def emit(self, record: logging.LogRecord):
        # Retrieve context where the logging call occurred, this happens
        # to be in the 6th frame upward
        mapper = {
            10: "DEBUG",
            20: "INFO",
            30: "WARNING",
            40: "ERROR",
            50: "CRITICAL",
        }
        logger_opt = logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(mapper.get(record.levelno), record.getMessage())


handler = InterceptHandler()


@setup_logging.connect
def setup_logging(*args, **kwargs):
    logging.basicConfig(
        handlers=[handler],
        level="WARNING",
    )


celery_app.conf.beat_schedule = {
    # import work task
    'sync_import_work_state': {
        'task': 'apps.worker.celery_worker.sync_import_work_state',
        'schedule': timedelta(seconds=settings.work_start_up_timedelta),
    },
    'sync_celery_task_state': {
        'task': 'apps.worker.celery_worker.sync_celery_task_state',
        'schedule': timedelta(seconds=settings.work_start_up_timedelta),
    },
    'start_up_import_task': {
        'task': 'apps.worker.celery_worker.start_up_import_task',
        'schedule': timedelta(seconds=settings.work_start_up_timedelta),
    },
    'sync_dag_work_state': {
        'task': 'apps.worker.celery_worker.sync_airflow_task_runs',
        'schedule': timedelta(seconds=settings.work_start_up_timedelta)
    }
}
