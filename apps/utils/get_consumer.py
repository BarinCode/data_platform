from aiokafka import AIOKafkaConsumer

from apps.config import settings
from apps.services.kafka.consumer import KafkaConsumer
from apps.services.kafka.handlers import store_task_data_to_mysql


def get_sync_consumer():
    consumer_conf = {
        'bootstrap.servers': settings.kafka_bootstrap_server,
        'group.id': settings.kafka_task_group_id,
        'auto.offset.reset': 'latest'
    }
    consumer = KafkaConsumer(consumer_conf)
    consumer.init(settings.kafka_task_topic, store_task_data_to_mysql)

    return consumer


def get_async_consumer():
    consumer = AIOKafkaConsumer(
        'my_topic', 'my_other_topic',
        bootstrap_servers='localhost:9092',
        group_id="my-group"
    )

    return consumer
