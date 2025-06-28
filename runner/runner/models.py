from sqlmodel import SQLModel, Field
from sqlalchemy import Column, JSON
from typing import Optional
from datetime import datetime

class Scenario(SQLModel, table=True):
    id: str = Field(primary_key=True)
    action: str
    video_source: str
    created_at: datetime
    updated_at: datetime

class Prediction(SQLModel, table=True):
    id: str = Field(primary_key=True)
    scenario_id: str
    timestamp: datetime
    data: dict = Field(sa_column=Column(JSON)) 