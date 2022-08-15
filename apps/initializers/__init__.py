from fastapi.applications import FastAPI

from apps.config import settings
from apps.services.kafka.consumer import KafkaConsumer
from apps.services.kafka.handlers import store_task_data_to_mysql


def setup_initializers(app: FastAPI):
    """Initialize fastapi app"""

    consumer_conf = {
        'bootstrap.servers': settings.kafka_bootstrap_server,
        'group.id': settings.kafka_task_group_id,
        'auto.offset.reset': 'latest'
    }
    consumer = KafkaConsumer(consumer_conf)
    consumer.init(settings.kafka_task_topic, store_task_data_to_mysql)

    @app.on_event("startup")
    async def startup_event():
        consumer.start()

    @app.on_event("shutdown")
    async def shutdown_event():
        consumer.close()
