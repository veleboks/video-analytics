import asyncio
import logging
import sys
from scenario import Runner
from config import load_config
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/runner.log')
    ]
)
logger = logging.getLogger('runner_main')

async def main():
    start_time = datetime.utcnow()
    logger.info("=== Runner Service Starting ===")
    logger.info("Start time: %s", start_time)
    
    try:
        logger.info("Loading configuration...")
        config = load_config()
        logger.info("Configuration loaded successfully")
        logger.debug("Configuration: %s", config)
        
        logger.info("Creating runner instance...")
        runner = await Runner.create(config)
        logger.info("Runner instance created successfully")
        
        logger.info("Starting runner services...")
        await asyncio.gather(
            runner.listen_and_run(),
            runner.process_stop_event()
        )
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error("Critical error in main runner: %s", e, exc_info=True)
        raise
    finally:
        end_time = datetime.utcnow()
        runtime = (end_time - start_time).total_seconds()
        logger.info("=== Runner Service Stopped ===")
        logger.info("End time: %s", end_time)
        logger.info("Total runtime: %.2f seconds", runtime)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Runner stopped by user")
    except Exception as e:
        logger.error("Fatal error in runner: %s", e, exc_info=True)
        sys.exit(1) 