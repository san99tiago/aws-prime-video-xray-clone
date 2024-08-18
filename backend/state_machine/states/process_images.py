# Built-in imports
import os

# Own imports
from common.logger import custom_logger
from common.helpers.s3_helper import S3Helper
from common.helpers.dynamodb_helper import DynamoDBHelper
from state_machine.base_step_function import BaseStepFunction
from state_machine.processing.image_detection import recognize_celebrities
from state_machine.processing.image_drawing import ImageDrawing

logger = custom_logger()

# Initialize AWS helpers (S3 and DynamoDB) from env vars
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
s3_helper = S3Helper(S3_BUCKET_NAME)
dynamodb_helper = DynamoDBHelper(DYNAMODB_TABLE_NAME)


class ProcessImages(BaseStepFunction):
    """
    This class contains methods that will "process the images" for the State Machine.
    """

    def __init__(self, event):
        super().__init__(event, logger=logger)

        self.LOCAL_SCREENSHOT_PATH = "/tmp/screenshot.jpg"

        # TODO Add correlation IDs and extra keys to the logger

    def process_images(self):
        """
        Method to process the images to the user.
        """
        self.logger.info("Starting process_images for the given screenshots...")

        # TODO: Add possibility to process multiple images (batches)
        # Get Image details from child workflow execution input (distributed map)
        self.s3_bucket_name = self.event.get("s3_bucket_name")
        self.s3_key = self.event.get("s3_key")
        self.input_video_name = self.event.get("input_video_name")
        self.frame_time = self.event.get("frame_time")
        self.s3_processed_image_key = self._generate_s3_processed_image_key()

        # Step by step image processing pipeline...
        self.download_image()
        result = self.run_face_recognition_process()
        self.total_celebrities = self.draw_faces(result)
        self.upload_image_to_s3()
        self.save_results_in_dynamodb(result)

        self.event.update(
            {
                "total_celebrities": self.total_celebrities,
                "rekognition_detect_face_response": result,
                "s3_processed_image_key": self.s3_processed_image_key,
            }
        )
        return self.event

    def download_image(self):
        """
        Internal method to download the image from S3.
        """
        logger.info(
            f"Downloading image from s3_bucket_name: {self.s3_bucket_name} on s3_key: {self.s3_key}..."
        )

        # Download the image from S3 to the local system
        s3_helper.download_object(self.s3_key, self.LOCAL_SCREENSHOT_PATH)

    def run_face_recognition_process(self):
        """
        Internal method to run image processing.
        """
        logger.info(
            f"Processing image located in s3_bucket_name: {self.s3_bucket_name}"
            f" on key: {self.s3_key}"
        )
        # Run image processing with Rekognition helpers
        recognize_celebrities_response = recognize_celebrities(
            s3_bucket_name=self.s3_bucket_name,
            image_key=self.s3_key,
        )
        logger.debug(
            recognize_celebrities_response,
            message_details="recognize_celebrities_response",
        )
        logger.info("Famous people detection finished!")
        return recognize_celebrities_response

    def draw_faces(self, rekognition_detect_face_response: dict):
        """
        Internal method to draw faces on the image.
        :param rekognition_detect_face_response: The response from the Rekognition service
            "recognize_celebrities" API call.
        """
        logger.info("Drawing faces on the image...")
        # Draw faces on the image
        image_drawing = ImageDrawing(
            image_path=self.LOCAL_SCREENSHOT_PATH,
            rekognition_detect_face_response=rekognition_detect_face_response,
            result_demo_output_path=self.LOCAL_SCREENSHOT_PATH,  # Overwrite the image (for now)
        )
        image_drawing.draw_faces()
        image_drawing.save_image()

    def upload_image_to_s3(self):
        """
        Internal method to upload the image to S3.
        """
        logger.info(
            f"Uploading image to s3_bucket_name: {self.s3_bucket_name}"
            f" on s3_key: {self.s3_processed_image_key}..."
        )

        # Upload the modified image to S3
        s3_helper.upload_object_from_file(
            s3_key=self.s3_processed_image_key,
            local_upload_path=self.LOCAL_SCREENSHOT_PATH,
        )

    def save_results_in_dynamodb(self, rekognition_detect_face_response: dict):
        """
        Internal method to save the results in DynamoDB.
        :param rekognition_detect_face_response: The response from the Rekognition service
            "recognize_celebrities" API call.
        """
        logger.info("Saving results in DynamoDB...")

        # Save the results in DynamoDB
        # TODO: Add more robust model definition for the DynamoDB items
        dynamodb_helper.put_item(
            {
                "PK": self.input_video_name,
                "SK": f"{self.frame_time:05}",  # Pad with zeros up to 5 digits,
                "celebrities": self.total_celebrities,
                "rekognition_detect_face_response": rekognition_detect_face_response,
                "s3_key_raw_image": self.s3_key,
                "s3_key_processed_image": self.s3_processed_image_key,
                "s3_bucket_name": self.s3_bucket_name,
            }
        )

    def _generate_s3_processed_image_key(self):
        """
        Internal method to generate the S3 key for the processed image.
        """
        return self.s3_key.replace("raw", "processed")
