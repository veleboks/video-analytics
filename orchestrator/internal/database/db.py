from sqlmodel import SQLModel, create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import asyncio

DATABASE_URL = "postgresql+asyncpg://vidanalytics:secret@postgres:5432/vidanalytics"

async_engine = create_async_engine(DATABASE_URL, echo=True, future=True)
AsyncSessionLocal = sessionmaker(
    bind=async_engine, class_=AsyncSession, expire_on_commit=False
)

async def init_db():
    async with async_engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

async def close_db():
    await async_engine.dispose()

async def get_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session 