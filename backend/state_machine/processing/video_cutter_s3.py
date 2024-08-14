# Built-in imports
from typing import Optional, List

# External imports
import cv2  # Note: for Lambda Functions, I leveraged "opencv-python-headless"

# Own imports
from common.logger import custom_logger
from common.helpers.s3_helper import S3Helper

logger = custom_logger()


class VideoCutterS3:
    """
    Class to interact with videos stored in an S3 bucket and apply operations to them.
    Disclaimer: This class is not meant to be used in production. Experimental usage only.
    Available methods:
    - download_video_from_s3: Download a video from an S3 bucket.
    - initialize_video_capture: Initialize the video capture object.
    - extract_frames_and_upload_to_s3: Extract frames from a video and upload them to an S3 bucket.
    """

    def __init__(
        self,
        s3_bucket_name: str,
        s3_key_input_video: str,
        s3_folder_output: str,
    ):
        """
        Constructor method to initialize the VideoCutterS3 object.
        :param s3_bucket_name: The name of the S3 bucket.
        :param s3_key_input_video: The path to the input video file.
        :pram s3_folder_output: The path to the output folder in the S3 bucket.
        """
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_input_video = s3_key_input_video
        self.s3_folder_output = s3_folder_output

        # Initialize the S3 Helper
        self.s3_helper = S3Helper(s3_bucket_name)

    def download_video_from_s3(self, download_path: Optional[str] = "/tmp/video.mp4"):
        """
        Method to download a video from an S3 bucket.
        :param download_path: The path where the video will be saved.
            Important: For Lambda Functions, use the /tmp directory to avoid permission issues.
        """
        logger.info(f"Starting download of video from S3: {self.s3_key_input_video}")
        response = self.s3_helper.download_object(
            self.s3_key_input_video, download_path
        )
        logger.info(f"Response Details: {response}")
        logger.info(f"Video downloaded to {download_path}")

    def initialize_video_capture(self, download_path: str):
        """
        Method to initialize the video capture object from the downloaded video.
        :param download_path: The path where the video is saved.
        """
        self.video_capture = cv2.VideoCapture(download_path)
        self.frame_count = 0
        self.fps = self.video_capture.get(cv2.CAP_PROP_FPS)

    def extract_frames_and_upload_to_s3(
        self,
        temp_screenshot_path: Optional[str] = "/tmp/screenshot.jpg",
        frame_rate: Optional[int] = 1,
    ) -> List[str]:
        """
        Method to extract frames from a video and save them to an S3 bucket in a given folder.
        The frames will be saved as JPG images with the format: screenshot_{frame_time}.jpg
        :param temp_screenshot_path: The path where the screenshot will be saved temporarily.
            Important: For Lambda Functions, use the /tmp directory to avoid permission issues.
        :param frame_rate: The rate at which the frames will be extracted (e.g. every 1 second).
        """
        # Get the frames per second (fps) of the video
        frame_interval = self.fps * frame_rate

        # TODO: Add more robust error handling...
        if not self.video_capture.isOpened():
            logger.error("Video capture object is not initialized")
            logger.error("Make sure to call the initialize_video_capture method first")
            raise Exception("Video capture object is not initialized")

        self.screenshots = []
        while True:
            # Set the current position of the video file in milliseconds
            self.video_capture.set(cv2.CAP_PROP_POS_FRAMES, self.frame_count)
            success, frame = self.video_capture.read()

            if not success:
                break  # TODO: Add more robust error management

            # Save the frame locally as a screenshot (same filename to reduce overall space usage)
            frame_time = int(self.frame_count / self.fps)

            # Zero-pad the frame_time to ensure filenames are in the correct order
            frame_time_str = f"{frame_time:05}"  # Pad with zeros up to 5 digits

            cv2.imwrite(temp_screenshot_path, frame)
            logger.debug(f"Saved {temp_screenshot_path}")

            # Upload the screenshot to S3 with the correct filename
            s3_key_upload = f"{self.s3_folder_output}/screenshot_{frame_time_str}.jpg"
            self.s3_helper.upload_object(
                s3_key=s3_key_upload,
                local_upload_path=temp_screenshot_path,
            )
            logger.debug(f"Uploaded screenshot to S3: {s3_key_upload}")

            # Add the screenshot to the list of screenshots to be returned
            self.screenshots.append(s3_key_upload)

            # Skip to the next frame based on frame_interval
            self.frame_count += frame_interval

        logger.debug("Finished extracting frames from video")

        # Release the video capture object
        self.video_capture.release()

        return self.screenshots
