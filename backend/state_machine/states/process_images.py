# Own imports
from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger
from state_machine.processing.image_detection import recognize_celebrities


logger = custom_logger()


class ProcessImages(BaseStepFunction):
    """
    This class contains methods that will "process the images" for the State Machine.
    """

    def __init__(self, event):
        super().__init__(event, logger=logger)

        # TODO Add correlation IDs and extra keys to the logger

    def process_images(self):
        """
        Method to process the images to the user.
        """

        self.logger.info("Starting process_images for the chatbot")

        # Obtain Image details from child workflow execution input
        s3_bucket_name = self.event.get("s3_bucket_name")
        s3_key = self.event.get("s3_key")

        logger.info(
            f"Processing image in s3_bucket_name: {s3_bucket_name}" f" on key: {s3_key}"
        )
        result = recognize_celebrities(s3_bucket_name=s3_bucket_name, image_key=s3_key)

        logger.debug(result, message_details="Result")
        logger.info("Famous people detection finished!")

        # TODO: Add "drawing" faces here!!!
        self.logger.info("Drawing faces...")
        # TODO: Save the image with the faces drawn in S3!!!
        self.logger.info("Saving new screenshots in S3...")
        # TODO: add save results in DynamoDB!!!
        self.logger.info("Saving results in DynamoDB...")

        self.event.update(
            {
                "results": result,
            }
        )

        return self.event
