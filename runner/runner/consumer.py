import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
import json
from datetime import datetime

logger = logging.getLogger('kafka_consumer')

KAFKA_BROKERS = ["kafka:9092"]
SCENARIO_TOPIC = "video-scenarios"
GROUP_ID = "video-runner-group"

class KafkaConsumer:
    def __init__(self, brokers, topic, group_id):
        self.brokers = brokers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        logger.info("Kafka consumer initialized - brokers: %s, topic: %s, group: %s", brokers, topic, group_id)

    async def start(self):
        logger.info("Starting Kafka consumer...")
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self.consumer.start()
        logger.info("Kafka consumer started successfully")

    async def stop(self):
        if self.consumer:
            logger.info("Stopping Kafka consumer...")
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")

    async def get_message(self):
        logger.debug("Waiting for Kafka message...")
        async for msg in self.consumer:
            receive_time = datetime.utcnow()
            logger.info("Received Kafka message - topic: %s, partition: %d, offset: %d, key: %s", 
                      msg.topic, msg.partition, msg.offset, msg.key)
            
            try:
                cmd = json.loads(msg.value)
                logger.info("Parsed message command - action: %s, scenario_id: %s", 
                          cmd.get('action'), cmd.get('scenario_id', 'unknown'))
                # Подтверждаем только после успешной обработки (Runner сам вызовет commit)
                return {"cmd": cmd, "msg": msg}
            except json.JSONDecodeError as e:
                logger.error("Failed to parse JSON message: %s", e)
                continue
            except Exception as e:
                logger.error("Error processing message: %s", e, exc_info=True)
                continue

    async def commit(self, msg):
        start_time = datetime.utcnow()
        logger.debug("Committing message - topic: %s, partition: %d, offset: %d", 
                   msg.topic, msg.partition, msg.offset)
        
        try:
            tp = TopicPartition(msg.topic, msg.partition)
            await self.consumer.commit({tp: msg.offset + 1})
            
            commit_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info("Message committed successfully in %.2f seconds - topic: %s, partition: %d, offset: %d", 
                      commit_time, msg.topic, msg.partition, msg.offset)
            
        except Exception as e:
            logger.error("Error committing message: %s", e, exc_info=True)
            raise
