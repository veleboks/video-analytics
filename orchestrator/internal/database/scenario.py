from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from internal.models.models import Scenario, ScenarioStatus
from typing import Optional
from datetime import datetime
import logging

logger = logging.getLogger('scenario_db')

async def get_scenario_by_id(session: AsyncSession, scenario_id: str) -> Optional[Scenario]:
    logger.debug("Getting scenario by ID: %s", scenario_id)
    result = await session.execute(select(Scenario).where(Scenario.id == scenario_id))
    scenario = result.scalar_one_or_none()
    if scenario:
        logger.debug("Found scenario: %s with status: %s", scenario.id, scenario.status)
    else:
        logger.warning("Scenario not found: %s", scenario_id)
    return scenario

async def create_scenario(session: AsyncSession, scenario: Scenario) -> None:
    logger.info("Creating scenario: %s with status: %s", scenario.id, scenario.status)
    now = datetime.utcnow()
    scenario.created_at = now
    scenario.updated_at = now
    session.add(scenario)
    logger.info("Scenario created successfully: %s", scenario.id)

async def update_scenario_status(session: AsyncSession, scenario_id: str, status: str) -> None:
    logger.info("Updating scenario %s status to: %s", scenario_id, status)
    scenario = await get_scenario_by_id(session, scenario_id)
    if scenario:
        old_status = scenario.status
        scenario.status = status
        scenario.updated_at = datetime.utcnow()
        logger.info("Scenario %s status updated from %s to %s", scenario_id, old_status, status)
    else:
        logger.error("Failed to update scenario %s status - scenario not found", scenario_id) 