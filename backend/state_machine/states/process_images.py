# Built-in imports
from datetime import datetime

# Own imports
from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger


logger = custom_logger()


class ProcessImages(BaseStepFunction):
    """
    This class contains methods that will "process the images" for the State Machine.
    """

    def __init__(self, event):
        super().__init__(event, logger=logger)

    def process_images(self):
        """
        Method to process the images to the user.
        """

        self.logger.info("Starting process_images for the chatbot")

        # TODO: add real logic to process image and save results in DynamoDB!!!
        self.logger.info("Processing images dummy for now...")

        return self.event
