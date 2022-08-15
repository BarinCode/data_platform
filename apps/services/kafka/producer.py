from confluent_kafka import Producer
from fastapi.logger import logger

from apps.config import settings


producer = Producer({'bootstrap.servers': settings.kafka_bootstrap_server})


class KafkaProducer:
    """A simple producer of kafka. alpha version"""

    def __init__(self, producer_conf) -> None:
        self.producer = Producer(producer_conf)
        self.topic = None
        self.logger = logger

    def produce(self, topic: str, message: str, *args, call_back=None, **kwargs):
        self.producer.poll(2)
        self.producer.produce(
            topic, message, *args, call_back=self.delivery_report, **kwargs
        )
        self.producer.flush()

    def delivery_report(self, error: str, kafka_message):
        """
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). 
        """
        if error is not None:
            self.logger.debug('Message delivery failed: {}'.format(error))
        else:
            topic, partition = kafka_message.topic(), kafka_message.partition()
            self.logger.debug(
                'Message delivered to {} [{}]'.format(topic, partition)
            )
