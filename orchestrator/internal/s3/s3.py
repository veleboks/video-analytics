from minio import Minio
from minio.error import S3Error
import asyncio
import logging
import os

logger = logging.getLogger('orchestrator_s3')

class S3Client:
    def __init__(self, endpoint: str, access_key: str, secret_key: str):
        logger.info("Initializing S3 client - endpoint: %s", endpoint)
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        logger.info("S3 client initialized successfully")

    async def ensure_bucket_exists(self, bucket_name: str):
        logger.debug("Checking if bucket exists: %s", bucket_name)
        found = await asyncio.to_thread(self.client.bucket_exists, bucket_name)
        if not found:
            logger.info("Creating bucket: %s", bucket_name)
            await asyncio.to_thread(self.client.make_bucket, bucket_name)
            logger.info("Bucket created successfully: %s", bucket_name)
        else:
            logger.debug("Bucket already exists: %s", bucket_name)

    async def upload_file_stream(self, bucket_name: str, object_name: str, file_path: str, content_type: str = "image/jpeg") -> str:
        start_time = asyncio.get_event_loop().time()
        
        logger.debug("Starting file upload - bucket: %s, object: %s, file: %s", bucket_name, object_name, file_path)
        
        # Проверяем существование файла
        if not os.path.exists(file_path):
            error_msg = f"File not found: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        file_size = os.path.getsize(file_path)
        logger.debug("File size: %d bytes", file_size)
        
        try:
            await self.ensure_bucket_exists(bucket_name)
            
            logger.debug("Uploading file to S3...")
            await asyncio.to_thread(
                self.client.fput_object,
                bucket_name,
                object_name,
                file_path,
                content_type
            )
            
            upload_time = asyncio.get_event_loop().time() - start_time
            logger.info("File uploaded successfully - bucket: %s, object: %s, size: %d bytes, time: %.2f seconds", 
                       bucket_name, object_name, file_size, upload_time)
            
            return object_name
            
        except S3Error as e:
            error_msg = f"S3 error uploading {file_path} to {bucket_name}/{object_name}: {e}"
            logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = f"Unexpected error uploading {file_path} to {bucket_name}/{object_name}: {e}"
            logger.error(error_msg)
            raise 