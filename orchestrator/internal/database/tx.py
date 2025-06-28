from sqlalchemy.ext.asyncio import AsyncSession
from contextlib import asynccontextmanager

@asynccontextmanager
async def in_tx(session: AsyncSession):
    async with session.begin():
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        else:
            await session.commit() 