from aiokafka import AIOKafkaProducer
import json

class KafkaProducer:
    def __init__(self, brokers: list[str], topic: str):
        self.brokers = brokers
        self.topic = topic
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.brokers,
            acks='all',
        )
        await self.producer.start()

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def send_outbox_message(self, msg: dict):
        payload = json.dumps(msg).encode()
        await self.producer.send_and_wait(self.topic, payload, key=msg.get('scenario_id', '').encode()) 