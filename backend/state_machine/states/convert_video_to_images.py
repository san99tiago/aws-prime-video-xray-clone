# Built-in imports
import os

# Own imports
from common.logger import custom_logger
from state_machine.base_step_function import BaseStepFunction
from state_machine.processing.video_cutter_s3 import VideoCutterS3

# Setup logger
logger = custom_logger()

# Initialize S3 Helper
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]


class ConvertVideoToImages(BaseStepFunction):
    """
    This class contains methods that serve as the "convert video to images" for the State Machine.
    """

    def __init__(self, event):
        super().__init__(event, logger=logger)

    def convert_video_to_images(self):
        """
        Method to convert the input video into images and save them to S3 accordingly.
        """

        self.logger.info("Starting convert_video_to_images process...")

        # TODO: Get S3 key for the input video
        # TODO: Add actual video processing and cutting logic here!!!
        self.total_images = (
            10  # TODO: Update from 10 to an actual number of images from video
        )

        self.logger.info("Validation finished successfully")

        self.event["total_images"] = self.total_images

        return self.event
