from sqlmodel import SQLModel, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from models import Scenario
from datetime import datetime, timedelta

DATABASE_URL = "postgresql+asyncpg://vidanalytics:secret@postgres:5432/runner"

async_engine = create_async_engine(DATABASE_URL, echo=True, future=True)
AsyncSessionLocal = sessionmaker(
    bind=async_engine, class_=AsyncSession, expire_on_commit=False
)

async def init_db():
    async with async_engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

async def get_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session

async def create_scenario(session: AsyncSession, scenario: Scenario):
    now = datetime.utcnow()
    scenario.created_at = now
    scenario.updated_at = now
    session.add(scenario)

async def get_scenario_by_id(session: AsyncSession, scenario_id: str):
    result = await session.execute(select(Scenario).where(Scenario.id == scenario_id))
    return result.scalar_one_or_none()

async def update_scenario_status(session: AsyncSession, scenario_id: str, action: str):
    scenario = await get_scenario_by_id(session, scenario_id)
    if scenario:
        scenario.action = action
        scenario.updated_at = datetime.utcnow()

async def update_scenario_timestamp(session: AsyncSession, scenario_id: str):
    scenario = await get_scenario_by_id(session, scenario_id)
    if scenario:
        scenario.updated_at = datetime.utcnow()

async def get_inactive_scenarios(session: AsyncSession, threshold_seconds: int = 30):
    now = datetime.utcnow()
    threshold = now - timedelta(seconds=threshold_seconds)
    result = await session.execute(select(Scenario).where(Scenario.updated_at < threshold))
    return result.scalars().all() 