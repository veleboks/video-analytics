import asyncio
import logging
import os
import yaml
from datetime import datetime
from internal.database.db import get_session
from internal.database.outbox import get_pending_outbox_messages, mark_outbox_message_as_processed
from internal.database.scenario import update_scenario_status, get_scenario_by_id
from internal.kafka.producer import KafkaProducer
from internal.models.models import ScenarioStatus, CommandAction

OUTBOX_DISPATCH_INTERVAL = 5

logger = logging.getLogger("outbox_dispatcher")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(handler)

def get_kafka_topic():
    config_path = os.environ.get("CONFIG_PATH", "docker.yaml")
    with open(f"/app/internal/config/{config_path}", "r") as f:
        config = yaml.safe_load(f)
    return config["kafka"]["scenario_topic"]

async def outbox_dispatcher():
    logger.info("Outbox dispatcher started")
    brokers = ["kafka:9092"]
    topic = get_kafka_topic()
    producer = KafkaProducer(brokers, topic)
    await producer.start()
    try:
        while True:
            logger.info("Checking for pending outbox messages...")
            async for session in get_session():
                messages = await get_pending_outbox_messages(session, limit=10)
                logger.info(f"Found {len(messages)} pending outbox messages")
                for msg in messages:
                    logger.info(f"Processing outbox message: id={msg.id}, scenario_id={msg.scenario_id}, action={msg.action}")
                    
                    # Проверяем текущий статус сценария
                    scenario = await get_scenario_by_id(session, msg.scenario_id)
                    if scenario:
                        logger.info(f"Current scenario status: {scenario.status}")
                    else:
                        logger.warning(f"Scenario {msg.scenario_id} not found")
                    
                    try:
                        await producer.send_outbox_message({
                            "id": msg.id,
                            "scenario_id": msg.scenario_id,
                            "action": msg.action,
                            "created_at": msg.created_at.isoformat(),
                            "video_source": msg.video_source,
                        })
                        logger.info(f"Sent message to Kafka for scenario_id={msg.scenario_id}")
                    except Exception as e:
                        logger.error(f"Failed to send message to Kafka: {e}")
                        continue
                    
                    # Объединяем маркировку сообщения и обновление статуса в одну транзакцию
                    try:
                        if msg.action == CommandAction.START:
                            new_status = ScenarioStatus.IN_STARTUP_PROCESSING
                        else:
                            new_status = ScenarioStatus.IN_SHUTDOWN_PROCESSING
                        
                        logger.info(f"Starting transaction: marking message as processed and updating scenario {msg.scenario_id} status to {new_status}")
                        logger.info(f"Outbox message timing: created_at={msg.created_at}, processing_delay={(datetime.utcnow() - msg.created_at).total_seconds():.2f}s")
                        
                        # Выполняем обе операции в одной транзакции
                        await mark_outbox_message_as_processed(session, msg.id)
                        await update_scenario_status(session, msg.scenario_id, new_status)
                        
                        # Коммитим транзакцию
                        await session.commit()
                        
                        logger.info(f"Transaction completed successfully: message {msg.id} marked as processed, scenario {msg.scenario_id} status updated to {new_status}")
                        logger.info(f"Scenario {msg.scenario_id} is now ready to receive heartbeat messages")
                        
                    except Exception as e:
                        logger.error(f"Failed to process transaction for message {msg.id}: {e}", exc_info=True)
                        # Откатываем транзакцию при ошибке
                        await session.rollback()
                        logger.error(f"Transaction rolled back for message {msg.id}")
            await asyncio.sleep(OUTBOX_DISPATCH_INTERVAL)
    except Exception as e:
        logger.error(f"Outbox dispatcher crashed: {e}", exc_info=True)
    finally:
        await producer.stop()
        logger.info("Outbox dispatcher stopped") 