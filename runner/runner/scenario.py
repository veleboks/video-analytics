import asyncio
import logging
from collections import defaultdict
from db import init_db, get_session, create_scenario, get_scenario_by_id, update_scenario_status, update_scenario_timestamp, get_inactive_scenarios
from s3 import S3Client
from detection_client import DetectionClient
from heartbeat import HeartbeatProducer
from consumer import KafkaConsumer
from models import Scenario
from datetime import datetime
import uuid

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('runner')

class Runner:
    def __init__(self, db, s3_client, detection_client, consumer, heartbeat, max_scenarios=10):
        self.db = db
        self.s3_client = s3_client
        self.detection_client = detection_client
        self.consumer = consumer
        self.heartbeat = heartbeat
        self.max_scenarios = max_scenarios
        self.active_runners = {}
        self.lock = asyncio.Lock()
        logger.info("Runner initialized with max_scenarios=%d", max_scenarios)

    @classmethod
    async def create(cls, config):
        logger.info("Creating runner instance...")
        await init_db()
        logger.info("Database initialized")
        
        s3_cfg = config['minio']
        s3_client = S3Client(
            endpoint=s3_cfg['endpoint'],
            access_key=s3_cfg['access_key'],
            secret_key=s3_cfg['secret_key'],
            bucket='frames',
        )
        logger.info("S3 client initialized with endpoint: %s", s3_cfg['endpoint'])
        
        detection_client = DetectionClient(config['detection']['endpoint'])
        logger.info("Detection client initialized with endpoint: %s", config['detection']['endpoint'])
        
        kafka_cfg = config['kafka']
        consumer = KafkaConsumer(
            brokers=kafka_cfg['brokers'],
            topic=kafka_cfg['scenario_topic'],
            group_id=kafka_cfg['group_id']
        )
        await consumer.start()
        logger.info("Kafka consumer started for topic: %s", kafka_cfg['scenario_topic'])
        
        heartbeat = HeartbeatProducer(
            brokers=kafka_cfg['brokers'],
            topic=kafka_cfg['heartbeat_topic']
        )
        await heartbeat.start()
        logger.info("Heartbeat producer started for topic: %s", kafka_cfg['heartbeat_topic'])
        
        logger.info("Runner instance created successfully")
        return cls(None, s3_client, detection_client, consumer, heartbeat)

    async def listen_and_run(self):
        logger.info("Starting to listen for Kafka commands...")
        while True:
            try:
                msg_obj = await self.consumer.get_message()
                cmd = msg_obj['cmd']
                msg = msg_obj['msg']
                action = cmd.get('action')
                logger.info("Received Kafka message - action: %s, scenario_id: %s", action, cmd.get('scenario_id', 'unknown'))
                
                if action == 'start':
                    await self.start(cmd)
                elif action == 'stop':
                    await self.register_stop_event(cmd['scenario_id'])
                else:
                    logger.warning("Unknown command action: %s", action)
                
                await self.consumer.commit(msg)
                logger.debug("Message committed successfully")
            except Exception as e:
                logger.error("Error processing Kafka message: %s", e, exc_info=True)

    async def start(self, cmd):
        scenario_id = cmd['scenario_id']
        logger.info("Starting scenario processing for ID: %s", scenario_id)
        
        # Проверить, не запущен ли уже сценарий
        async for session in get_session():
            exist = await get_scenario_by_id(session, scenario_id)
            if exist:
                logger.warning("Runner for scenario %s already running, skipping", scenario_id)
                return
            
            scenario = Scenario(
                id=scenario_id,
                action=cmd['action'],
                video_source=cmd['video_source'],
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            await create_scenario(session, scenario)
            logger.info("Scenario %s created in database", scenario_id)
        
        # Отправить heartbeat (start)
        await self.heartbeat.send_heartbeat({
            "scenario_id": scenario_id,
            "action": "start",
            "timestamp": datetime.utcnow().isoformat()
        })
        logger.info("Start heartbeat sent for scenario %s", scenario_id)
        
        # Запуск обработки сценария в отдельной задаче
        task = asyncio.create_task(self.process_scenario(cmd))
        async with self.lock:
            self.active_runners[scenario_id] = task
        logger.info("Scenario %s processing task created, active runners: %d", scenario_id, len(self.active_runners))

    async def process_scenario(self, cmd):
        scenario_id = cmd['scenario_id']
        video_source = cmd['video_source']
        start_time = datetime.utcnow()
        
        logger.info("=== Starting scenario processing ===")
        logger.info("Scenario ID: %s", scenario_id)
        logger.info("Video source: %s", video_source)
        logger.info("Processing started at: %s", start_time)
        
        try:
            # Скачиваем кадры из S3
            logger.info("Downloading frames from S3...")
            download_start = datetime.utcnow()
            frames = await self.s3_client.download_files_from_url(f"{video_source}/frames")
            download_time = (datetime.utcnow() - download_start).total_seconds()
            logger.info("Downloaded %d frames in %.2f seconds", len(frames), download_time)
            
            # Для каждого кадра вызываем detection и сохраняем результат
            total_detection_time = 0
            total_save_time = 0
            
            for idx, frame in enumerate(frames):
                frame_start = datetime.utcnow()
                logger.info("Processing frame %d/%d for scenario %s", idx + 1, len(frames), scenario_id)
                
                try:
                    # Detection
                    detection_start = datetime.utcnow()
                    logger.debug("Sending frame %d to detection service...", idx + 1)
                    detections = await self.detection_client.send_frame(frame, scenario_id)
                    detection_time = (datetime.utcnow() - detection_start).total_seconds()
                    total_detection_time += detection_time
                    logger.info("Frame %d detection completed in %.2f seconds, found %d objects", 
                              idx + 1, detection_time, len(detections.get('detections', [])))
                    
                    # Save detection results
                    save_start = datetime.utcnow()
                    logger.debug("Saving detection results for frame %d...", idx + 1)
                    await self.s3_client.save_detection_results(scenario_id, idx, detections)
                    save_time = (datetime.utcnow() - save_start).total_seconds()
                    total_save_time += save_time
                    logger.info("Frame %d detection results saved in %.2f seconds", idx + 1, save_time)
                    
                    # Отправляем heartbeat
                    await self.heartbeat.send_heartbeat({
                        "scenario_id": scenario_id,
                        "action": "processing",
                        "frame": idx + 1,
                        "total_frames": len(frames),
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    logger.debug("Heartbeat sent for frame %d", idx + 1)
                    
                    # Обновляем updated_at в БД
                    async for session in get_session():
                        await update_scenario_timestamp(session, scenario_id)
                    
                    frame_time = (datetime.utcnow() - frame_start).total_seconds()
                    logger.info("Frame %d total processing time: %.2f seconds", idx + 1, frame_time)
                    
                except Exception as e:
                    logger.error("Error processing frame %d for scenario %s: %s", idx + 1, scenario_id, e, exc_info=True)
                    # Continue with next frame instead of failing entire scenario
            
            # Финальный heartbeat (stop)
            await self.heartbeat.send_heartbeat({
                "scenario_id": scenario_id,
                "action": "stop",
                "frame": len(frames),
                "total_frames": len(frames),
                "timestamp": datetime.utcnow().isoformat()
            })
            
            total_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info("=== Scenario processing completed ===")
            logger.info("Scenario ID: %s", scenario_id)
            logger.info("Total frames processed: %d", len(frames))
            logger.info("Total processing time: %.2f seconds", total_time)
            logger.info("Average detection time per frame: %.2f seconds", total_detection_time / len(frames) if frames else 0)
            logger.info("Average save time per frame: %.2f seconds", total_save_time / len(frames) if frames else 0)
            logger.info("Processing completed at: %s", datetime.utcnow())
            
        except Exception as e:
            logger.error("Critical error processing scenario %s: %s", scenario_id, e, exc_info=True)
            # Send error heartbeat
            await self.heartbeat.send_heartbeat({
                "scenario_id": scenario_id,
                "action": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
        finally:
            async with self.lock:
                self.active_runners.pop(scenario_id, None)
            logger.info("Scenario %s removed from active runners", scenario_id)

    async def register_stop_event(self, scenario_id):
        logger.info("Registering stop event for scenario %s", scenario_id)
        await self.stop(scenario_id)

    async def stop(self, scenario_id):
        logger.info("Stopping scenario %s", scenario_id)
        
        # Отправляем heartbeat о остановке
        await self.heartbeat.send_heartbeat({
            "scenario_id": scenario_id,
            "action": "stop",
            "timestamp": datetime.utcnow().isoformat()
        })
        logger.info("Stop heartbeat sent for scenario %s", scenario_id)
        
        async with self.lock:
            task = self.active_runners.get(scenario_id)
            if task:
                task.cancel()
                logger.info("Scenario %s task cancelled", scenario_id)
                self.active_runners.pop(scenario_id, None)
            else:
                logger.warning("Scenario %s not found in active runners", scenario_id)

    async def process_stop_event(self):
        logger.info("Starting stop event processor...")
        while True:
            await asyncio.sleep(10)
            try:
                # Проверяем неактивные сценарии и останавливаем их
                async for session in get_session():
                    inactive = await get_inactive_scenarios(session)
                    if inactive:
                        logger.info("Found %d inactive scenarios", len(inactive))
                        for scenario in inactive:
                            logger.info("Stopping inactive scenario %s", scenario.id)
                            await self.stop(scenario.id)
            except Exception as e:
                logger.error("Error in stop event processor: %s", e, exc_info=True) 