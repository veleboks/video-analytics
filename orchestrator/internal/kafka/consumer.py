from aiokafka import AIOKafkaConsumer
import json
import logging
from datetime import datetime
from ..database.heartbeats import write_heartbeat
from ..database.scenario import get_scenario_by_id, update_scenario_status
from ..models.models import ScenarioStatus, CommandAction, Heartbeat

logger = logging.getLogger('orchestrator_consumer')

class KafkaConsumer:
    def __init__(self, brokers: list[str], group_id: str, topic: str):
        self.brokers = brokers
        self.group_id = group_id
        self.topic = topic
        self.consumer = None
        logger.info("Kafka consumer initialized - brokers: %s, group: %s, topic: %s", brokers, group_id, topic)

    async def start(self, session):
        logger.info("Starting heartbeat consumer...")
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
        )
        await self.consumer.start()
        logger.info("Heartbeat consumer started successfully")
        
        try:
            async for msg in self.consumer:
                try:
                    logger.debug("Received heartbeat message: %s", msg.value)
                    data = json.loads(msg.value)
                    
                    # Преобразуем timestamp из строки в datetime
                    if 'timestamp' in data and isinstance(data['timestamp'], str):
                        data['timestamp'] = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                    
                    # Обрабатываем frame поле
                    if 'frame' in data and data['frame'] is None:
                        data['frame'] = None
                    
                    heartbeat = Heartbeat(**data)
                    
                    logger.info("Processing heartbeat - scenario: %s, action: %s", heartbeat.scenario_id, heartbeat.action)
                    
                    scenario = await get_scenario_by_id(session, heartbeat.scenario_id)
                    if not scenario:
                        logger.warning("Scenario %s not found, skipping heartbeat", heartbeat.scenario_id)
                        continue
                    
                    logger.info("Found scenario %s with status: %s", scenario.id, scenario.status)
                    
                    # Проверяем, что сценарий был обработан outbox dispatcher'ом
                    if heartbeat.action == CommandAction.START:
                        if scenario.status == ScenarioStatus.INIT_STARTUP:
                            logger.warning("Race condition detected: scenario %s still has INIT_STARTUP status, outbox dispatcher hasn't processed it yet. Skipping heartbeat.", heartbeat.scenario_id)
                            continue
                        elif scenario.status not in [ScenarioStatus.IN_STARTUP_PROCESSING, ScenarioStatus.ACTIVE]:
                            logger.warning("Unexpected status for START heartbeat: scenario %s has status %s, skipping", heartbeat.scenario_id, scenario.status)
                            continue
                    elif heartbeat.action == CommandAction.STOP:
                        if scenario.status == ScenarioStatus.INIT_SHUTDOWN:
                            logger.warning("Race condition detected: scenario %s still has INIT_SHUTDOWN status, outbox dispatcher hasn't processed it yet. Skipping heartbeat.", heartbeat.scenario_id)
                            continue
                        elif scenario.status not in [ScenarioStatus.IN_SHUTDOWN_PROCESSING, ScenarioStatus.ACTIVE]:
                            logger.warning("Unexpected status for STOP heartbeat: scenario %s has status %s, skipping", heartbeat.scenario_id, scenario.status)
                            continue
                    
                    # Аналог логики из Go
                    status_updated = False
                    if heartbeat.action == CommandAction.START and scenario.status == ScenarioStatus.IN_STARTUP_PROCESSING:
                        logger.info("Updating scenario %s status from IN_STARTUP_PROCESSING to ACTIVE", heartbeat.scenario_id)
                        await update_scenario_status(session, heartbeat.scenario_id, ScenarioStatus.ACTIVE)
                        status_updated = True
                        logger.info("Status updated successfully for scenario %s", heartbeat.scenario_id)
                    elif heartbeat.action == CommandAction.STOP and scenario.status in [ScenarioStatus.IN_SHUTDOWN_PROCESSING, ScenarioStatus.ACTIVE]:
                        logger.info("Updating scenario %s status from %s to INACTIVE", heartbeat.scenario_id, scenario.status)
                        await update_scenario_status(session, heartbeat.scenario_id, ScenarioStatus.INACTIVE)
                        status_updated = True
                        logger.info("Status updated successfully for scenario %s", heartbeat.scenario_id)
                    else:
                        logger.info("No status update needed - action: %s, current status: %s", heartbeat.action, scenario.status)
                        logger.info("Conditions not met for status update")
                    
                    await write_heartbeat(session, heartbeat)
                    logger.info("Heartbeat saved to database for scenario %s, status_updated: %s", heartbeat.scenario_id, status_updated)
                    
                except json.JSONDecodeError as e:
                    logger.error("Failed to parse heartbeat message: %s", e)
                except Exception as e:
                    logger.error("Error processing heartbeat message: %s", e, exc_info=True)
                    # Откатываем транзакцию при ошибке
                    await session.rollback()
        finally:
            await self.consumer.stop()
            logger.info("Heartbeat consumer stopped") 