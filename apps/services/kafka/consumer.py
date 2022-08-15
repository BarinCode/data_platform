import threading
import traceback
from typing import Callable, Dict

from confluent_kafka import Consumer
from loguru import logger


class KafkaConsumer:
    """A simple kafka consumer"""

    def __init__(self, consumer_conf: Dict):
        self.consumer = Consumer(consumer_conf)
        self.handlers = {}
        self.running = False
        self.topic: str = None
        self.handler: Callable = None
        self.logger = logger

    def run_handler(self, kafka_message, handler: Callable):
        try:
            handler(kafka_message.value())
            self.consumer.commit()
        except Exception as exception:
            self.logger.info(traceback.format_exc())
            self.logger.critical(str(exception), exc_info=1)

    def _start(self):
        self.consumer.subscribe(topics=[self.topic])

        while self.running:
            kafka_message = self.consumer.poll(1) or []
            if kafka_message:
                self.run_handler(kafka_message, self.handler)

                # topic: str = kafka_message.topic()
                # payload: bytes = kafka_message.value()
                # self.logger.debug(f"{topic} -> {payload.decode('utf-8')}")

            if not self.running:
                self.consumer.close()

    def start(self):
        self.logger.info(f"KAFKA: starting...")
        self.logger.info(f"KAFKA: subscribe on: {self.topic}")
        self.running = True

        # Would this thread block the main thread/coroutine of fastapi?
        thread = threading.Thread(target=self._start)
        thread.start()

    def close(self):
        self.logger.info("KAFKA: closing...")
        self.running = False

    def init(self, topic: str, handler: Callable):
        self.topic = topic
        self.handler = handler
