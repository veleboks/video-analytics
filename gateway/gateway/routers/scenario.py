import json
import logging

from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, UploadFile, File
from uuid import UUID
import httpx

from gateway.clients.s3 import s3Client
from gateway.config import ORCHESTRATOR_URL, S3_BUCKET
from gateway.schemas.scenario import ScenarioAction

router = APIRouter()
client = httpx.AsyncClient(timeout=30.0)
logger = logging.getLogger("gateway.prediction")


def parse_httpx_error(e: httpx.HTTPStatusError) -> HTTPException:
    try:
        detail = e.response.json()
    except Exception:
        detail = e.response.text
    return HTTPException(
        status_code=e.response.status_code, detail=f"Orchestrator error: {detail}"
    )


@router.post("/scenario/")
async def initialize_scenario(video: UploadFile = File(...)):
    try:
        file_bytes = await video.read()
        files = {"video": (video.filename, file_bytes, video.content_type)}

        resp = await client.post(f"{ORCHESTRATOR_URL}/scenario", files=files)
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise parse_httpx_error(e)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=f"Orchestrator HTTPError: {e}")
    return resp.json()


@router.get("/scenario/{scenario_id}/")
async def get_scenario_status(scenario_id: UUID):
    try:
        resp = await client.get(f"{ORCHESTRATOR_URL}/scenario/{scenario_id}")
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise parse_httpx_error(e)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=f"Orchestrator HTTPError: {e}")
    return resp.json()


@router.post("/scenario/{scenario_id}/")
async def change_scenario_status(scenario_id: UUID, action: ScenarioAction):
    try:
        resp = await client.post(
            f"{ORCHESTRATOR_URL}/scenario/{scenario_id}",
            params={"action": action.value},
        )
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise parse_httpx_error(e)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=f"Orchestrator HTTPError: {e}")
    return resp.json()


@router.get("/prediction/{scenario_id}/")
async def get_predictions(scenario_id: UUID):
    print(f"PRINT DEBUG: Starting prediction request for scenario: {scenario_id}")
    logger.info(f"=== Starting prediction request for scenario: {scenario_id} ===")
    try:
        # Runner сохраняет в {scenario_id}/predictions/prediction_{idx}.json
        folder_prefix = f"{str(scenario_id)}/predictions/"
        logger.info(f"Looking for predictions with prefix: {folder_prefix}")
        logger.info(f"Using bucket: {S3_BUCKET}")
        
        results = []

        # Получаем список объектов в папке
        objects = s3Client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=folder_prefix
        )
        
        logger.info(f"S3 response: {objects}")
        logger.info(f"Contents key exists: {'Contents' in objects}")
        if 'Contents' in objects:
            logger.info(f"Found {len(objects['Contents'])} objects")

        if "Contents" not in objects:
            raise HTTPException(
                status_code=404, detail="No predictions found for this scenario"
            )

        # Обрабатываем каждый файл
        for obj in objects["Contents"]:
            if obj["Key"].endswith("/"):
                continue

            file_key = obj["Key"]
            logger.info(f"Processing file: {file_key}")
            try:
                response = s3Client.get_object(Bucket=S3_BUCKET, Key=file_key)
                file_content = response["Body"].read().decode("utf-8")
                
                # Извлекаем номер кадра из имени файла prediction_{idx}.json
                frame_number = file_key.split("/")[-1].replace("prediction_", "").replace(".json", "")
                
                results.append(
                    {
                        "frame": frame_number,
                        "predictions": json.loads(file_content),
                    }
                )
                logger.info(f"Added result for frame {frame_number}")
            except ClientError as e:
                logger.info(f"ClientError for {file_key}: {e}")
                continue
            except json.JSONDecodeError as e:
                logger.info(f"JSONDecodeError for {file_key}: {e}")
                continue

        if not results:
            raise HTTPException(
                status_code=404, detail="No valid prediction files found"
            )

        # Сортируем по номеру кадра
        results.sort(key=lambda x: int(x["frame"]))
        logger.info(f"Returning {len(results)} results")

        return {"scenario_id": str(scenario_id), "results": results}
    except HTTPException:
        raise
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            raise HTTPException(status_code=404, detail="Predictions bucket not found")
        raise HTTPException(status_code=500, detail=f"S3 error: {str(e)}")
    except Exception as e:
        logger.info(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
