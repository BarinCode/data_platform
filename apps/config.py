from functools import lru_cache
from os import environ

from pydantic import BaseSettings, RedisDsn, AnyHttpUrl


class ServiceBaseSettings(BaseSettings):
    # mysql config
    mysql_url: str

    # redis config
    redis_url: RedisDsn

    # celery config
    celery_broker_url: str
    celery_quene_name: str
    celery_worker_max_tasks_per_child: int

    # work checker config
    work_next_run_refresh_timedelta: int
    work_start_up_timedelta: int

    # kafka config
    kafka_bootstrap_server: str
    kafka_task_group_id: str
    kafka_task_topic: str

    # jwt_secret_key default from `openssl rand -hex 32`
    jwt_secret_key: str
    jwt_algorithm: str
    jwt_access_token_expire_minutes: int

    # other service
    airflow_service: AnyHttpUrl
    work_service: AnyHttpUrl
    resource_service: AnyHttpUrl
    user_service: AnyHttpUrl

    task_count_per_batch: int = 1


class ProdSettings(ServiceBaseSettings):
    # mysql config
    mysql_url: str = "mysql+pymysql://root:@mysql.default/work_service"

    # redis config
    redis_url: RedisDsn = "redis://redis:redis@redis.default/1"

    # celery config
    celery_broker_url: str = "amqp://amqp.default:5672"
    celery_quene_name: str = "work-service-tasks-prod"
    celery_worker_max_tasks_per_child: int = 128

    # work checker config
    work_next_run_refresh_timedelta: int = 10
    work_start_up_timedelta: int = 5

    # kafka config
    kafka_bootstrap_server: str = "kafka.default"
    kafka_task_group_id: str = "work_service_task_group_prod"
    kafka_task_topic: str = "work_service_task_event_prod"

    # jwt_secret_key default from `openssl rand -hex 32`
    jwt_secret_key: str = "de110f6c2dff5aa2aad7f288c6d310ef0d39b1ad370b3750d69a9dbf43221278"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 15

    # airflow server
    airflow_service: AnyHttpUrl = "http://airflow.qa-chuhe.xyz10.com/api/v1"

    # other service
    work_service: AnyHttpUrl = "http://work-service.default/api/v1"
    resource_service: AnyHttpUrl = "http://resource-service.default/api/v1"
    user_service: AnyHttpUrl = "http://user-service.default/v1"

    task_count_per_batch: int = 1

    class Config:
        env_file = ".env.prod"


class QaSettings(ServiceBaseSettings):
    # mysql config
    mysql_url: str = "mysql+pymysql://xyz:D!ks84sef@124.71.162.61:13312/work_service"

    # redis config
    redis_url: RedisDsn = "redis://redis:redis@redis.qa/1"

    # celery config
    celery_broker_url: str = "amqp://amqp.qa:5672"
    celery_quene_name: str = "work-service-tasks-qa"
    celery_worker_max_tasks_per_child: int = 64

    # work checker config
    work_next_run_refresh_timedelta: int = 10
    work_start_up_timedelta: int = 5

    # airflow server
    airflow_service: AnyHttpUrl = "http://airflow.qa-chuhe.xyz10.com/api/v1"

    # kafka config
    kafka_bootstrap_server: str = "kafka.qa"
    kafka_task_group_id: str = "work_service_task_group_qa"
    kafka_task_topic: str = "work_service_task_event_qa"

    # jwt_secret_key default from `openssl rand -hex 32`
    jwt_secret_key: str = "de110f6c2dff5aa2aad7f288c6d310ef0d39b1ad370b3750d69a9dbf43221278"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 15

    # other service
    work_service: AnyHttpUrl = "http://work-service.qa/api/v1"
    resource_service: AnyHttpUrl = "http://resource-service.qa/api/v1"
    user_service: AnyHttpUrl = "http://user-service.qa/v1"

    task_count_per_batch: int = 1

    class Config:
        env_file = ".env.qa"


class LocalSettings(ServiceBaseSettings):
    # mysql config
    mysql_url: str = "mysql+pymysql://root:@localhost/work_service"
    # mysql_url: str = "mysql+pymysql://root:1234@localhost/work_service"
    # redis config
    redis_url: RedisDsn = "redis://redis:redis@localhost:6379/1"

    # celery config
    celery_broker_url: str = "amqp://localhost:5672"
    celery_quene_name: str = "work-service-tasks-local"
    celery_worker_max_tasks_per_child: int = 64

    # work checker config
    work_next_run_refresh_timedelta: int = 10
    work_start_up_timedelta: int = 5

    # airflow server
    airflow_service: AnyHttpUrl = "http://airflow.qa-chuhe.xyz10.com/api/v1"

    # kafka config
    kafka_bootstrap_server: str = "localhost:9092"
    kafka_task_group_id: str = "work_service_task_group_local"
    kafka_task_topic: str = "work_service_task_event_local"

    # jwt_secret_key default from `openssl rand -hex 32`
    jwt_secret_key: str = "de110f6c2dff5aa2aad7f288c6d310ef0d39b1ad370b3750d69a9dbf43221278"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 15

    # other service
    work_service: AnyHttpUrl = "http://localhost:3072/api/v1"
    resource_service: AnyHttpUrl = "http://localhost:4096/v1"
    user_service: AnyHttpUrl = "http://localhost:2048/v1"

    task_count_per_batch: int = 1

    class Config:
        env_file = ".env.local"


@lru_cache()
def get_settings() -> ServiceBaseSettings:
    env_name = environ.get("ENV_NAME", "local")
    settings = {
        "qa": QaSettings(),
        "prod": ProdSettings(),
        "local": LocalSettings()
    }.get(env_name)

    return settings


# * Cache settings
settings = get_settings()


# * celery
celery_config = {
    'broker_url': settings.celery_broker_url,
    'result_backend': settings.redis_url,
    'result_expires': 10,
    'task_serializer': 'json',

    # Ignore other content
    'accept_content': ['json', 'pickle'],
    'result_serializer': 'json',
    'timezone': 'Asia/Shanghai',
    'enable_utc': 'True',
    'include': ['apps.worker.celery_worker'],
    'task_default_queue': settings.celery_quene_name,
    'broker_login_method': 'PLAIN',

    # Workaround for fix memory leak issue
    'worker_max_tasks_per_child': settings.celery_worker_max_tasks_per_child,
}
