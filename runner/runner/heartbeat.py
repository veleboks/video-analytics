from aiokafka import AIOKafkaProducer
import json
import logging
from datetime import datetime

logger = logging.getLogger('heartbeat')

class HeartbeatProducer:
    def __init__(self, brokers, topic):
        self.brokers = brokers
        self.topic = topic
        self.producer = None
        logger.info("Heartbeat producer initialized - brokers: %s, topic: %s", brokers, topic)

    async def start(self):
        logger.info("Starting heartbeat producer...")
        self.producer = AIOKafkaProducer(bootstrap_servers=self.brokers)
        await self.producer.start()
        logger.info("Heartbeat producer started successfully")

    async def stop(self):
        if self.producer:
            logger.info("Stopping heartbeat producer...")
            await self.producer.stop()
            logger.info("Heartbeat producer stopped")

    async def send_heartbeat(self, heartbeat: dict):
        start_time = datetime.utcnow()
        scenario_id = heartbeat.get('scenario_id', 'unknown')
        action = heartbeat.get('action', 'unknown')
        
        logger.debug("Sending heartbeat - scenario: %s, action: %s", scenario_id, action)
        
        try:
            payload = json.dumps(heartbeat).encode()
            payload_size = len(payload)
            
            await self.producer.send_and_wait(self.topic, payload, key=scenario_id.encode())
            
            send_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info("Heartbeat sent successfully in %.2f seconds - scenario: %s, action: %s (%d bytes)", 
                      send_time, scenario_id, action, payload_size)
            
        except Exception as e:
            logger.error("Error sending heartbeat for scenario %s, action %s: %s", scenario_id, action, e, exc_info=True)
            raise 