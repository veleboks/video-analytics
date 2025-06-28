import boto3
import asyncio
import tempfile
import logging
from datetime import datetime
from typing import List, Dict, Any
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

logger = logging.getLogger('s3_client')

class S3Client:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str, max_retries: int = 3):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.max_retries = max_retries
        
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto3.session.Config(
                retries=dict(
                    max_attempts=max_retries
                )
            )
        )
        logger.info("S3 client initialized with endpoint: %s, bucket: %s", endpoint, bucket)

    async def _retry_operation(self, operation, *args, **kwargs):
        """Retry wrapper для S3 операций"""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return await asyncio.get_event_loop().run_in_executor(
                    None, operation, *args, **kwargs
                )
            except (ClientError, NoCredentialsError, EndpointConnectionError) as e:
                last_exception = e
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning("S3 operation failed (attempt %d/%d), retrying in %d seconds: %s", 
                                 attempt + 1, self.max_retries + 1, wait_time, str(e))
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("S3 operation failed after %d attempts: %s", self.max_retries + 1, str(e))
                    raise last_exception

    async def download_files_from_url(self, prefix: str) -> List[str]:
        """Скачивает файлы из S3 по префиксу с retry logic"""
        start_time = datetime.utcnow()
        logger.info("Downloading files from S3 - prefix: %s", prefix)
        
        try:
            # Получаем список объектов
            list_operation = lambda: self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            objects_response = await self._retry_operation(list_operation)
            
            if 'Contents' not in objects_response:
                logger.warning("No objects found with prefix: %s", prefix)
                return []
            
            files = []
            for obj in objects_response['Contents']:
                if obj['Key'].endswith('/'):
                    continue
                
                # Скачиваем файл
                download_operation = lambda: self.s3_client.get_object(Bucket=self.bucket, Key=obj['Key'])
                response = await self._retry_operation(download_operation)
                
                # Сохраняем во временный файл
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.jpg')
                temp_file.write(response['Body'].read())
                temp_file.close()
                files.append(temp_file.name)
                
                logger.debug("Downloaded file: %s -> %s", obj['Key'], temp_file.name)
            
            download_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info("Downloaded %d files in %.2f seconds", len(files), download_time)
            return files
            
        except Exception as e:
            logger.error("Error downloading files from S3 - prefix: %s, error: %s", prefix, str(e), exc_info=True)
            raise

    async def save_detection_results(self, scenario_id: str, frame_index: int, detection_data: Dict[str, Any]) -> str:
        """Сохраняет результаты detection в S3 с retry logic"""
        start_time = datetime.utcnow()
        object_key = f"{scenario_id}/predictions/prediction_{frame_index}.json"
        
        logger.info("Saving detection results - scenario: %s, frame: %d, object: %s", 
                   scenario_id, frame_index, object_key)
        
        try:
            import json
            json_data = json.dumps(detection_data, indent=2)
            
            upload_operation = lambda: self.s3_client.put_object(
                Bucket=self.bucket,
                Key=object_key,
                Body=json_data,
                ContentType='application/json'
            )
            
            await self._retry_operation(upload_operation)
            
            save_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info("Detection results saved successfully in %.2f seconds - %s (%d bytes)", 
                       save_time, object_key, len(json_data))
            
            return object_key
            
        except Exception as e:
            logger.error("Error saving detection results - scenario: %s, frame: %d, error: %s", 
                        scenario_id, frame_index, str(e), exc_info=True)
            raise
