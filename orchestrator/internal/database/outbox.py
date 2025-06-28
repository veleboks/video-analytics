from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select, text
from internal.models.models import OutboxMessage, Scenario
from typing import List
from datetime import datetime
import uuid

async def add_to_outbox(session: AsyncSession, scenario_id: str, action: str) -> None:
    msg = OutboxMessage(
        id=str(uuid.uuid4()),
        scenario_id=scenario_id,
        action=action,
        created_at=datetime.utcnow(),
        video_source=""  # будет заполнено позже, если нужно
    )
    session.add(msg)

async def get_pending_outbox_messages(session: AsyncSession, limit: int = 10) -> List[OutboxMessage]:
    query = text('''
        SELECT o.id, o.scenario_id, o.action, o.created_at, o.processed_at, s.video_source
        FROM outboxmessage o
        JOIN scenario s ON o.scenario_id = s.id
        WHERE o.processed_at IS NULL
        ORDER BY o.created_at
        LIMIT :limit
    ''')
    result = await session.execute(query, {"limit": limit})
    rows = result.fetchall()
    return [OutboxMessage(
        id=row[0], scenario_id=row[1], action=row[2], created_at=row[3], processed_at=row[4], video_source=row[5]
    ) for row in rows]

async def mark_outbox_message_processed_by_scenario_id(session: AsyncSession, scenario_id: str) -> bool:
    query = text('''
        UPDATE outboxmessage
        SET processed_at = :now
        WHERE id = (
            SELECT id FROM outboxmessage
            WHERE processed_at IS NULL AND scenario_id = :scenario_id
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id
    ''')
    now = datetime.utcnow()
    result = await session.execute(query, {"now": now, "scenario_id": scenario_id})
    return result.rowcount > 0

async def mark_outbox_message_as_processed(session: AsyncSession, id: str) -> None:
    query = text('UPDATE outboxmessage SET processed_at = :now WHERE id = :id')
    now = datetime.utcnow()
    await session.execute(query, {"now": now, "id": id})
    # Убираем session.commit() чтобы позволить использовать в большей транзакции 