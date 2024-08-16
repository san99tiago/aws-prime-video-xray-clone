# Built-in imports
import os

# Own imports
from common.logger import custom_logger
from state_machine.base_step_function import BaseStepFunction
from state_machine.processing.video_cutter_s3 import VideoCutterS3
from common.helpers.s3_helper import S3Helper

# Setup logger
logger = custom_logger()

# Initialize S3 Helper
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
s3_helper = S3Helper(S3_BUCKET_NAME)


class ConvertVideoToImages(BaseStepFunction):
    """
    This class contains methods that serve as the "convert video to images" for the State Machine.
    """

    def __init__(self, event):
        super().__init__(event, logger=logger)

        # Define class variables for the paths and keys
        self.LOCAL_VIDEO_PATH = "/tmp/video.mp4"
        self.LOCAL_SCREENSHOT_PATH = "/tmp/screenshot.jpg"
        self.DISTRIBUTED_MAP_KEY = "maps/00_distributed_map.json"

        # TODO Add correlation IDs and extra keys to the logger

    def convert_video_to_images(self):
        """
        Method to convert the input video into images and save them to S3 accordingly.
        """

        self.logger.info("Starting convert_video_to_images process...")

        # TODO: Add actual video processing and cutting logic here!!!
        self.total_images = (
            10  # TODO: Update from 10 to an actual number of images from video
        )
        # TODO: Add saving logic to specific

        # TODO: Enhance validations
        s3_bucket_name = self.event.get("detail", {}).get("bucket", {}).get("name")
        if not s3_bucket_name:
            self.logger.error("No S3 bucket name found for the input video!")
            raise ValueError("No S3 bucket name found for the input video!")

        logger.info(f"Bucket name: {s3_bucket_name}")

        s3_key_input_video = self.event.get("detail", {}).get("object", {}).get("key")
        if not s3_key_input_video:
            self.logger.error("No S3 key found for the input video!")
            raise ValueError("No S3 key found for the input video!")
        logger.info(f"S3 Key: {s3_bucket_name}")

        # TODO: Enhance with better error handling and logging...
        self.logger.info("Starting video cutting process...")
        input_video_name = s3_key_input_video.split("/")[-1]
        s3_folder_output = f"results/{input_video_name.split('.mp4')[0]}"

        # TODO: Uncomment after the distributed map tests are done (to avoid re-processing while WIP)
        video_cutter = VideoCutterS3(
            s3_bucket_name, s3_key_input_video, s3_folder_output
        )
        video_cutter.download_video_from_s3(self.LOCAL_VIDEO_PATH)
        video_cutter.initialize_video_capture(self.LOCAL_VIDEO_PATH)
        video_cutter.extract_frames_and_upload_to_s3(self.LOCAL_SCREENSHOT_PATH)
        video_cutter.upload_distributed_map_to_s3(s3_key=self.DISTRIBUTED_MAP_KEY)

        # Really extensive log (only debugging)
        self.logger.info("Convert video to images finished successfully")
        self.logger.debug(video_cutter.screenshots, message_details="Screenshots")

        self.event.update(
            {
                "s3_bucket_name": s3_bucket_name,
                "s3_folder_output": s3_folder_output,
                "total_images": len(video_cutter.screenshots),
                "s3_distributed_map_json": self.DISTRIBUTED_MAP_KEY,  # TODO: Make dynamically
            }
        )

        return self.event
