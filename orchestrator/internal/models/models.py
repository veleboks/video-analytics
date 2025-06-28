from typing import Optional, Any
from datetime import datetime
from sqlmodel import SQLModel, Field
from sqlalchemy import Column, JSON

class ScenarioStatus:
    INIT_STARTUP = "init_startup"
    IN_STARTUP_PROCESSING = "in_startup_processing"
    ACTIVE = "active"
    INIT_SHUTDOWN = "init_shutdown"
    IN_SHUTDOWN_PROCESSING = "in_shutdown_processing"
    INACTIVE = "inactive"

class Scenario(SQLModel, table=True):
    id: str = Field(primary_key=True)
    status: str
    video_source: str
    created_at: datetime
    updated_at: datetime

class Prediction(SQLModel, table=True):
    id: str = Field(primary_key=True)
    scenario_id: str
    timestamp: datetime
    data: dict = Field(sa_column=Column(JSON))

class ScenarioCreate(SQLModel):
    video_source: str
    config: Optional[dict] = None

class StatusUpdate(SQLModel):
    status: str

class CommandAction:
    START = "start"
    STOP = "stop"

class OutboxMessage(SQLModel, table=True):
    id: str = Field(primary_key=True)
    scenario_id: str
    action: str
    created_at: datetime
    processed_at: Optional[datetime] = None
    video_source: str

class Heartbeat(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    scenario_id: str
    action: str
    frame: Optional[int] = None
    timestamp: datetime 