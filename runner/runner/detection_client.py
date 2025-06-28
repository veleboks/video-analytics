import aiohttp
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger('detection_client')

class DetectionClient:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        logger.info("Detection client initialized with endpoint: %s", endpoint)

    async def send_frame(self, frame_path, scenario_id):
        start_time = datetime.utcnow()
        
        # frame_path should be a string path to the file
        if not isinstance(frame_path, str):
            raise ValueError(f"Expected string path, got {type(frame_path)}")
        
        logger.info("Sending frame file to detection service - scenario: %s, file: %s", scenario_id, frame_path)
        
        try:
            async with aiohttp.ClientSession() as session:
                logger.debug("Making POST request to %s/predict", self.endpoint)
                data = aiohttp.FormData()
                
                # Open and read the file
                with open(frame_path, 'rb') as f:
                    data.add_field('file', f, filename='frame.jpg', content_type='image/jpeg')
                    
                    async with session.post(f"{self.endpoint}/predict", data=data) as resp:
                        if resp.status != 200:
                            error_msg = f"Detection failed with status {resp.status}"
                            logger.error("%s for scenario %s", error_msg, scenario_id)
                            raise Exception(error_msg)
                        
                        result = await resp.json()
                        detection_time = (datetime.utcnow() - start_time).total_seconds()
                        
                        # Log detection results
                        detections_count = len(result.get('detections', []))
                        logger.info("Detection completed for scenario %s in %.2f seconds, found %d objects", 
                                  scenario_id, detection_time, detections_count)
                        
                        if detections_count > 0:
                            logger.debug("Detection details for scenario %s: %s", scenario_id, result)
                        
                        return result
                        
        except aiohttp.ClientError as e:
            logger.error("Network error sending frame to detection service for scenario %s: %s", scenario_id, e)
            raise
        except Exception as e:
            logger.error("Unexpected error in detection for scenario %s: %s", scenario_id, e, exc_info=True)
            raise 