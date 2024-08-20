# Built-in imports
import os
import json

# Own imports
from common.logger import custom_logger
from common.helpers.s3_helper import S3Helper
from common.helpers.dynamodb_helper import DynamoDBHelper
from state_machine.base_step_function import BaseStepFunction

# Setup logger
logger = custom_logger()

# Initialize AWS helpers (S3 and DynamoDB) from env vars
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
s3_helper = S3Helper(S3_BUCKET_NAME)
dynamodb_helper = DynamoDBHelper(DYNAMODB_TABLE_NAME)


class ArrangeFinalResults(BaseStepFunction):
    """
    This class contains methods that serve as the "arrange final results" for the State Machine.
    """

    def __init__(self, event):
        super().__init__(event, logger=logger)

        # Define class variables for the paths and keys
        self.SK_INITIAL_PREFIX = "RESULTS#"

        # TODO Add correlation IDs and extra keys to the logger

    def arrange_final_results(self):
        """
        Method to obtain final results from the DynamoDB and S3 and arrange them accordingly.
        """
        self.logger.info("Starting arrange_final_results process...")

        # Load the PK/SK for the DynamoDB table from the event
        pk = self.event.get("input_video_name")
        sanitized_video_name = pk.split(".mp4")[0]

        # Define class variables for the paths and keys
        self.ARRANGED_RESULTS_S3_KEY = (
            f"results/{sanitized_video_name}/arranged_results.json"
        )

        # Load the results from the DynamoDB table
        dynamodb_results = self.load_results_from_dynamodb(pk)
        self.upload_results_to_s3(dynamodb_results)

        self.logger.info(
            f"Final results arranged and uploaded to S3 key: {self.ARRANGED_RESULTS_S3_KEY}"
        )

        self.event.update(
            {
                "arranged_results_s3_key": self.ARRANGED_RESULTS_S3_KEY,
            }
        )

        return self.event

    def load_results_from_dynamodb(self, pk) -> dict:
        """
        Method to load the results from the DynamoDB table.
        """
        self.logger.info("Loading results from the DynamoDB table...")

        # Load the results from the DynamoDB table
        results = dynamodb_helper.query_by_pk_and_sk_begins_with(
            pk,
            self.SK_INITIAL_PREFIX,
        )
        self.logger.debug(
            str(results),
            message_details=f"Results DynamoDB query with pk={pk}",
        )

        return results

    def upload_results_to_s3(self, dynamodb_results) -> None:
        """
        Method to upload the results to S3.
        """
        self.logger.info(
            f"Uploading results to s3_bucket = {S3_BUCKET_NAME} and s3_key = {self.ARRANGED_RESULTS_S3_KEY}..."
        )

        # Upload the results to S3
        s3_helper.upload_object_from_memory(
            s3_key=self.ARRANGED_RESULTS_S3_KEY,
            data=dynamodb_results,
        )
