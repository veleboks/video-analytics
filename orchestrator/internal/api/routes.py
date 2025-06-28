from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, status, Query
from fastapi.responses import JSONResponse
from sqlmodel.ext.asyncio.session import AsyncSession
from internal.database.db import get_session
from internal.models.models import Scenario, ScenarioStatus, CommandAction, OutboxMessage, Prediction
from internal.database.scenario import get_scenario_by_id, create_scenario, update_scenario_status
from internal.database.outbox import add_to_outbox, mark_outbox_message_processed_by_scenario_id
from internal.database.tx import in_tx
from internal.s3.s3 import S3Client
from uuid import uuid4, UUID
from datetime import datetime
import shutil
import os
import tempfile
import subprocess
from typing import List
import asyncio
import logging

logger = logging.getLogger('orchestrator_api')

router = APIRouter()

s3 = S3Client(endpoint="minio:9000", access_key="minio-access-key", secret_key="minio-secret-key")
BUCKET = "frames"

async def extract_frames(video_path: str, frames_dir: str) -> List[str]:
    logger.info("Starting frame extraction - video: %s, frames_dir: %s", video_path, frames_dir)
    
    os.makedirs(frames_dir, exist_ok=True)
    frame_pattern = os.path.join(frames_dir, "frame_%04d.jpg")
    
    cmd = [
        "ffmpeg", "-i", video_path, "-vf", "fps=3", "-q:v", "2", frame_pattern
    ]
    
    logger.info("Running ffmpeg command: %s", " ".join(cmd))
    
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    
    if proc.returncode != 0:
        error_msg = f"ffmpeg failed with return code {proc.returncode}: {stderr.decode()}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    extracted_frames = sorted([os.path.join(frames_dir, f) for f in os.listdir(frames_dir) if f.endswith('.jpg')])
    logger.info("Frame extraction completed - extracted %d frames", len(extracted_frames))
    
    if extracted_frames:
        logger.debug("Extracted frames: %s", extracted_frames[:5])  # Показываем первые 5 кадров
    
    return extracted_frames

async def save_frames_to_s3(scenario_id: str, files: List[str]):
    logger.info("Starting S3 upload for scenario %s - %d files to upload", scenario_id, len(files))
    
    if not files:
        error_msg = "No frames extracted from video"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    uploaded_count = 0
    for idx, frame_path in enumerate(files):
        try:
            file_name = os.path.basename(frame_path)
            object_name = f"{scenario_id}/frames/{file_name}"
            
            logger.debug("Uploading frame %d/%d: %s -> %s", idx + 1, len(files), frame_path, object_name)
            
            await s3.upload_file_stream(BUCKET, object_name, frame_path)
            uploaded_count += 1
            
            logger.debug("Successfully uploaded frame %d/%d", idx + 1, len(files))
            
        except Exception as e:
            logger.error("Failed to upload frame %d/%d (%s): %s", idx + 1, len(files), frame_path, e)
            raise
    
    logger.info("S3 upload completed for scenario %s - %d/%d files uploaded successfully", scenario_id, uploaded_count, len(files))

@router.post("/scenarios")
async def create_scenario_endpoint(
    video: UploadFile = File(...),
    session: AsyncSession = Depends(get_session)
):
    start_time = datetime.utcnow()
    logger.info("=== Starting scenario creation ===")
    logger.info("Received video upload - filename: %s, content_type: %s, size: %d bytes", 
               video.filename, video.content_type, video.size if hasattr(video, 'size') else 'unknown')
    
    if video.content_type not in ["video/mp4", "video/mpeg"]:
        logger.error("Invalid video format: %s", video.content_type)
        raise HTTPException(status_code=400, detail="Invalid video format")
    
    scenario_id = str(uuid4())
    logger.info("Generated scenario ID: %s", scenario_id)
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info("Created temporary directory: %s", temp_dir)
            
            # Сохраняем видео во временный файл
            video_path = os.path.join(temp_dir, f"{scenario_id}.mp4")
            logger.info("Saving video to temporary file: %s", video_path)
            
            with open(video_path, "wb") as f:
                shutil.copyfileobj(video.file, f)
            
            video_size = os.path.getsize(video_path)
            logger.info("Video saved successfully - size: %d bytes", video_size)
            
            # Извлекаем кадры
            frames_dir = os.path.join(temp_dir, f"frames_{scenario_id}")
            logger.info("Extracting frames to directory: %s", frames_dir)
            
            frames = await extract_frames(video_path, frames_dir)
            logger.info("Frame extraction completed - %d frames extracted", len(frames))
            
            # Загружаем кадры в S3
            logger.info("Starting S3 upload for scenario %s", scenario_id)
            await save_frames_to_s3(scenario_id, frames)
            logger.info("S3 upload completed for scenario %s", scenario_id)
        
        # Создаем сценарий в базе данных
        now = datetime.utcnow()
        scenario = Scenario(
            id=scenario_id,
            status=ScenarioStatus.INIT_STARTUP,
            video_source=scenario_id,
            created_at=now,
            updated_at=now
        )
        
        logger.info("Creating scenario in database - ID: %s, status: %s, video_source: %s", 
                   scenario_id, scenario.status, scenario.video_source)
        
        async with in_tx(session):
            await create_scenario(session, scenario)
            logger.info("Scenario created in database successfully")
            
            await add_to_outbox(session, scenario.id, CommandAction.START)
            logger.info("Added START command to outbox for scenario %s", scenario.id)
        
        total_time = (datetime.utcnow() - start_time).total_seconds()
        logger.info("=== Scenario creation completed ===")
        logger.info("Scenario ID: %s", scenario_id)
        logger.info("Total processing time: %.2f seconds", total_time)
        logger.info("Frames extracted: %d", len(frames))
        
        return {"id": scenario_id, "status": ScenarioStatus.INIT_STARTUP}
        
    except Exception as e:
        logger.error("Error creating scenario %s: %s", scenario_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create scenario: {str(e)}")

@router.post("/scenarios/{scenario_id}/status")
async def update_scenario_status_endpoint(
    scenario_id: str,
    action: str,
    session: AsyncSession = Depends(get_session)
):
    scenario = await get_scenario_by_id(session, scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    current_status = scenario.status
    new_status = None
    
    try:
        if action == CommandAction.START:
            if current_status in [ScenarioStatus.INIT_STARTUP, ScenarioStatus.IN_STARTUP_PROCESSING, ScenarioStatus.ACTIVE]:
                raise HTTPException(status_code=400, detail=f"Invalid transaction from status {current_status}")
            
            # Выполняем операции без дополнительной транзакции
            await mark_outbox_message_processed_by_scenario_id(session, scenario_id)
            await update_scenario_status(session, scenario_id, ScenarioStatus.INIT_STARTUP)
            await add_to_outbox(session, scenario_id, CommandAction.START)
            await session.commit()
            new_status = ScenarioStatus.INIT_STARTUP
            
        elif action == CommandAction.STOP:
            if current_status in [ScenarioStatus.INIT_SHUTDOWN, ScenarioStatus.IN_SHUTDOWN_PROCESSING, ScenarioStatus.INACTIVE]:
                raise HTTPException(status_code=400, detail=f"Invalid transaction from status {current_status}")
            
            # Выполняем операции без дополнительной транзакции
            await mark_outbox_message_processed_by_scenario_id(session, scenario_id)
            await update_scenario_status(session, scenario_id, ScenarioStatus.INIT_SHUTDOWN)
            await add_to_outbox(session, scenario_id, CommandAction.STOP)
            await session.commit()
            new_status = ScenarioStatus.INIT_SHUTDOWN
            
        else:
            raise HTTPException(status_code=400, detail="action parameter is required (start/stop)")
            
        return {"id": scenario_id, "status": new_status}
        
    except Exception as e:
        await session.rollback()
        logger.error("Error updating scenario status: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update scenario status: {str(e)}")

@router.get("/scenarios/{scenario_id}/status")
async def get_scenario_status_endpoint(
    scenario_id: str,
    session: AsyncSession = Depends(get_session)
):
    scenario = await get_scenario_by_id(session, scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return scenario

@router.get("/scenarios/{scenario_id}/predictions")
async def get_predictions_endpoint(
    scenario_id: str,
    session: AsyncSession = Depends(get_session)
):
    # Проверка существования сценария
    scenario = await get_scenario_by_id(session, scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    # Получение предсказаний
    from sqlmodel import select
    result = await session.execute(
        select(Prediction).where(Prediction.scenario_id == scenario_id).order_by(Prediction.timestamp.desc()).limit(100)
    )
    predictions = result.scalars().all()
    return predictions

@router.post("/scenario")
async def create_scenario_gateway(
    video: UploadFile = File(...),
    session: AsyncSession = Depends(get_session)
):
    start_time = datetime.utcnow()
    logger.info("=== Starting scenario creation (gateway endpoint) ===")
    logger.info("Received video upload via gateway - filename: %s, content_type: %s, size: %d bytes", 
               video.filename, video.content_type, video.size if hasattr(video, 'size') else 'unknown')
    
    # Тело совпадает с create_scenario_endpoint
    if video.content_type not in ["video/mp4", "video/mpeg"]:
        logger.error("Invalid video format via gateway: %s", video.content_type)
        raise HTTPException(status_code=400, detail="Invalid video format")
    
    scenario_id = str(uuid4())
    logger.info("Generated scenario ID via gateway: %s", scenario_id)
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info("Created temporary directory for gateway: %s", temp_dir)
            
            video_path = os.path.join(temp_dir, f"{scenario_id}.mp4")
            logger.info("Saving video to temporary file via gateway: %s", video_path)
            
            with open(video_path, "wb") as f:
                shutil.copyfileobj(video.file, f)
            
            video_size = os.path.getsize(video_path)
            logger.info("Video saved successfully via gateway - size: %d bytes", video_size)
            
            frames_dir = os.path.join(temp_dir, f"frames_{scenario_id}")
            logger.info("Extracting frames via gateway to directory: %s", frames_dir)
            
            frames = await extract_frames(video_path, frames_dir)
            logger.info("Frame extraction completed via gateway - %d frames extracted", len(frames))
            
            logger.info("Starting S3 upload via gateway for scenario %s", scenario_id)
            await save_frames_to_s3(scenario_id, frames)
            logger.info("S3 upload completed via gateway for scenario %s", scenario_id)
        
        now = datetime.utcnow()
        scenario = Scenario(
            id=scenario_id,
            status=ScenarioStatus.INIT_STARTUP,
            video_source=scenario_id,
            created_at=now,
            updated_at=now
        )
        
        logger.info("Creating scenario in database via gateway - ID: %s, status: %s, video_source: %s", 
                   scenario_id, scenario.status, scenario.video_source)
        
        async with in_tx(session):
            await create_scenario(session, scenario)
            logger.info("Scenario created in database successfully via gateway")
            
            await add_to_outbox(session, scenario.id, CommandAction.START)
            logger.info("Added START command to outbox via gateway for scenario %s", scenario.id)
        
        total_time = (datetime.utcnow() - start_time).total_seconds()
        logger.info("=== Scenario creation completed via gateway ===")
        logger.info("Scenario ID: %s", scenario_id)
        logger.info("Total processing time: %.2f seconds", total_time)
        logger.info("Frames extracted: %d", len(frames))
        
        return {"id": scenario_id, "status": ScenarioStatus.INIT_STARTUP}
        
    except Exception as e:
        logger.error("Error creating scenario via gateway %s: %s", scenario_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create scenario: {str(e)}")

@router.get("/scenario/{scenario_id}")
async def get_scenario_status_gateway(
    scenario_id: UUID,
    session: AsyncSession = Depends(get_session)
):
    scenario = await get_scenario_by_id(session, str(scenario_id))
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return scenario

@router.post("/scenario/{scenario_id}")
async def change_scenario_status_gateway(
    scenario_id: UUID,
    action: str = Query(...),
    session: AsyncSession = Depends(get_session)
):
    scenario = await get_scenario_by_id(session, str(scenario_id))
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    current_status = scenario.status
    new_status = None
    
    try:
        if action == CommandAction.START:
            if current_status in [ScenarioStatus.INIT_STARTUP, ScenarioStatus.IN_STARTUP_PROCESSING, ScenarioStatus.ACTIVE]:
                raise HTTPException(status_code=400, detail=f"Invalid transaction from status {current_status}")
            
            # Выполняем операции без дополнительной транзакции
            await mark_outbox_message_processed_by_scenario_id(session, str(scenario_id))
            await update_scenario_status(session, str(scenario_id), ScenarioStatus.INIT_STARTUP)
            await add_to_outbox(session, str(scenario_id), CommandAction.START)
            await session.commit()
            new_status = ScenarioStatus.INIT_STARTUP
            
        elif action == CommandAction.STOP:
            if current_status in [ScenarioStatus.INIT_SHUTDOWN, ScenarioStatus.IN_SHUTDOWN_PROCESSING, ScenarioStatus.INACTIVE]:
                raise HTTPException(status_code=400, detail=f"Invalid transaction from status {current_status}")
            
            # Выполняем операции без дополнительной транзакции
            await mark_outbox_message_processed_by_scenario_id(session, str(scenario_id))
            await update_scenario_status(session, str(scenario_id), ScenarioStatus.INIT_SHUTDOWN)
            await add_to_outbox(session, str(scenario_id), CommandAction.STOP)
            await session.commit()
            new_status = ScenarioStatus.INIT_SHUTDOWN
            
        else:
            raise HTTPException(status_code=400, detail="action parameter is required (start/stop)")
            
        return {"id": str(scenario_id), "status": new_status}
        
    except Exception as e:
        await session.rollback()
        logger.error("Error changing scenario status: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to change scenario status: {str(e)}")

@router.get("/prediction/{scenario_id}")
async def get_predictions_gateway(
    scenario_id: UUID,
    session: AsyncSession = Depends(get_session)
):
    scenario = await get_scenario_by_id(session, str(scenario_id))
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    from sqlmodel import select
    result = await session.execute(
        select(Prediction).where(Prediction.scenario_id == str(scenario_id)).order_by(Prediction.timestamp.desc()).limit(100)
    )
    predictions = result.scalars().all()
    return predictions 