import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from internal.api.routes import router
from internal.database.db import init_db, get_session
from internal.outbox.dispatcher import outbox_dispatcher
from internal.kafka.consumer import KafkaConsumer
from internal.watchdog.watchdog import start_watchdog
import asyncio
import yaml
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('orchestrator_main')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

async def start_heartbeat_consumer():
    try:
        # Загружаем конфигурацию
        config_path = os.getenv('CONFIG_PATH', '/app/internal/config/docker.yaml')
        logger.info("Loading configuration from: %s", config_path)
        
        # Проверяем существование файла
        if not os.path.exists(config_path):
            logger.error("Config file not found: %s", config_path)
            # Попробуем альтернативный путь
            alt_config_path = '/app/internal/config/docker.yaml'
            if os.path.exists(alt_config_path):
                config_path = alt_config_path
                logger.info("Using alternative config path: %s", config_path)
            else:
                raise FileNotFoundError(f"Config file not found at {config_path} or {alt_config_path}")
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        kafka_config = config['kafka']
        logger.info("Kafka config loaded: %s", kafka_config)
        
        consumer = KafkaConsumer(
            brokers=kafka_config['brokers'],
            group_id=kafka_config['group_id'],
            topic=kafka_config['heartbeat_topic']
        )
        
        logger.info("Starting heartbeat consumer for topic: %s", kafka_config['heartbeat_topic'])
        async for session in get_session():
            await consumer.start(session)
            
    except Exception as e:
        logger.error("Error starting heartbeat consumer: %s", e, exc_info=True)
        raise

@app.on_event("startup")
async def on_startup():
    logger.info("=== Orchestrator Service Starting ===")
    await init_db()
    logger.info("Database initialized")
    
    # Запуск outbox-диспетчера как фоновой задачи
    logger.info("Starting outbox dispatcher...")
    asyncio.create_task(outbox_dispatcher())
    
    # Запуск heartbeat consumer'а как фоновой задачи
    logger.info("Starting heartbeat consumer...")
    asyncio.create_task(start_heartbeat_consumer())
    
    # Запуск watchdog как фоновой задачи
    logger.info("Starting watchdog...")
    asyncio.create_task(start_watchdog())
    
    logger.info("=== Orchestrator Service Started ===")

if __name__ == "__main__":
    uvicorn.run("internal.api.main:app", host="0.0.0.0", port=8000, reload=True) 