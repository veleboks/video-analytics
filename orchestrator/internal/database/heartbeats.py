from sqlmodel import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from internal.models.models import Heartbeat, Scenario, ScenarioStatus
from datetime import datetime, timedelta
from typing import List

async def write_heartbeat(session: AsyncSession, heartbeat: Heartbeat) -> None:
    session.add(heartbeat)
    await session.commit()

async def find_stuck_scenarios(session: AsyncSession, interval_seconds: int) -> List[Scenario]:
    # Аналог LEFT JOIN + фильтрация по времени последнего heartbeat
    query = text('''
        SELECT s.id, s.status, s.video_source, s.created_at, s.updated_at
        FROM scenario s
        LEFT JOIN (
            SELECT scenario_id, MAX(timestamp) as last_heartbeat
            FROM heartbeat
            GROUP BY scenario_id
        ) h ON s.id = h.scenario_id
        WHERE s.status = :active_status AND (h.last_heartbeat IS NULL OR h.last_heartbeat < :threshold)
    ''')
    threshold = datetime.utcnow() - timedelta(seconds=interval_seconds)
    result = await session.execute(query, {"active_status": ScenarioStatus.ACTIVE, "threshold": threshold})
    rows = result.fetchall()
    return [Scenario(
        id=row[0], status=row[1], video_source=row[2], created_at=row[3], updated_at=row[4]
    ) for row in rows] 