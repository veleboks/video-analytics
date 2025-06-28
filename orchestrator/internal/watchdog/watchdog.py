import asyncio
import logging
from datetime import datetime, timedelta
from typing import List
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..database.db import get_session
from ..database.scenario import update_scenario_status
from ..database.outbox import add_to_outbox
from ..models.models import ScenarioStatus

logger = logging.getLogger('watchdog')

WATCH_INTERVAL = 30  # seconds


class Watchdog:
    def __init__(self):
        logger.info("Watchdog initialized")

    async def start(self):
        try:
            stuck_scenarios = await self.find_stuck_scenarios()
            
            if stuck_scenarios:
                logger.info("Found %d stuck scenarios", len(stuck_scenarios))
                
                for scenario in stuck_scenarios:
                    logger.info("Found stuck scenario %s, sending restart command", scenario['id'])
                    
                    try:
                        # Add restart command to outbox
                        await add_to_outbox(scenario['id'], 'start')
                        
                        # Update scenario status to INIT_STARTUP
                        async for session in get_session():
                            await update_scenario_status(session, scenario['id'], ScenarioStatus.INIT_STARTUP)
                        
                        logger.info("Successfully restarted scenario %s", scenario['id'])
                        
                    except Exception as e:
                        logger.error("Failed to restart scenario %s: %s", scenario['id'], e, exc_info=True)
            else:
                logger.debug("No stuck scenarios found")
                
        except Exception as e:
            logger.error("Error checking scenarios: %s", e, exc_info=True)

    async def find_stuck_scenarios(self) -> List[dict]:
        cutoff_time = datetime.utcnow() - timedelta(seconds=WATCH_INTERVAL)
        
        async for session in get_session():
            query = text("""
                SELECT s.id, s.status, s.video_source, s.created_at, s.updated_at
                FROM scenario s
                LEFT JOIN (
                    SELECT scenario_id, MAX(timestamp) as last_heartbeat
                    FROM heartbeat
                    GROUP BY scenario_id
                ) h ON s.id = h.scenario_id
                WHERE s.status = :status AND (h.last_heartbeat IS NULL OR h.last_heartbeat < :cutoff_time)
            """)
            
            result = await session.execute(query, {
                'status': ScenarioStatus.ACTIVE,
                'cutoff_time': cutoff_time
            })
            
            scenarios = []
            for row in result:
                scenarios.append({
                    'id': row[0],
                    'status': row[1],
                    'video_source': row[2],
                    'created_at': row[3],
                    'updated_at': row[4]
                })
            
            return scenarios


async def start_watchdog():
    watchdog = Watchdog()
    logger.info("Watchdog task started")
    
    while True:
        try:
            await watchdog.start()
            await asyncio.sleep(WATCH_INTERVAL)
        except Exception as e:
            logger.error("Error in watchdog task: %s", e, exc_info=True)
            await asyncio.sleep(WATCH_INTERVAL)


async def main():
    watchdog = Watchdog()
    await watchdog.start()


if __name__ == "__main__":
    asyncio.run(main()) 