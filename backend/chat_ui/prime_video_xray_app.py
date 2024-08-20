# -------------------------------------------------------------------------------
# TODO: Enhance the UI with better OOP, encapsulation, and error handling
# Disclaimer: This code is for educational purposes only, not for production use!
# -------------------------------------------------------------------------------

# Built-in imports
import os
import json

# External imports
import streamlit as st
from aws_lambda_powertools import Logger

# Add the path to the sys.path
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# Own imports
from common.helpers.s3_helper import S3Helper
from common.helpers.dynamodb_helper import DynamoDBHelper

# Logger
logger = Logger(
    service="rekognition-demo",
    log_uncaught_exceptions=True,
    owner="santi-tests",
)

# Global path configurations
PATH_TO_LOCAL_IMAGES = os.path.join(
    os.path.dirname(__file__),
    "local",
)
TEMP_RAW_PATH = os.path.join(PATH_TO_LOCAL_IMAGES, "raw.png")
TEMP_PROCESSED_PATH = os.path.join(PATH_TO_LOCAL_IMAGES, "processed.png")

# Initialize the S3 and DynamoDB helpers from env vars
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
s3_helper = S3Helper(S3_BUCKET_NAME)
dynamodb_helper = DynamoDBHelper(DYNAMODB_TABLE_NAME)

# General Streamlit configurations
st.set_page_config(layout="wide", page_title="Prime Video X-Ray App")
st.title("Prime Video X-Ray App")
col1, col2, col3 = st.columns([2, 2, 2])

with col2:
    st.subheader("Raw Image")


with col3:
    st.subheader("Processed Image")


with col1:
    # Obtain video from user's input
    uploaded_file = st.file_uploader("Upload an video", type=["mp4"])

    st.subheader("Process Video")
    st.write("Click the button below to Process Video for Amazon Prime Video X-Ray")
    generate_button = st.button("Process", type="primary")

    if generate_button:
        logger.info("Starting the generation process...")
        if uploaded_file:
            video_bytes = uploaded_file.read()

            # Upload the image to S3
            s3_helper.upload_binary_object(
                s3_key=f"videos/{uploaded_file.name}",
                data=video_bytes,
            )
            logger.info(f"Video uploaded to S3: {uploaded_file.name}")
        st.success("Video uploaded successfully!", icon=":material/thumb_up:")

    # Add time to process the video
    frame_time = int(
        st.text_input("Frame time to obtain results (in seconds)", value="39")
    )

    # Add a button to obtain the results
    get_images_button = st.button("Get Images", type="primary")

    if get_images_button:
        try:
            logger.info("Starting the process to obtain images...")
            # Download the image from DynamoDB
            frame_time_str = f"RESULTS#{frame_time:05}"  # Pad with zeros up to 5 digits

            item = dynamodb_helper.get_item_by_pk_and_sk(
                partition_key=uploaded_file.name,
                sort_key=frame_time_str,
            )
            logger.info(
                f"Image downloaded from DynamoDB pk={uploaded_file.name} sk={frame_time_str}"
            )
            logger.info(f"Item: {item}")

            # Download the images from S3
            s3_key_raw_image = item.get("s3_key_raw_image", {}).get("S")
            s3_key_processed_image = item.get("s3_key_processed_image", {}).get("S")
            raw_image = s3_helper.download_object(s3_key_raw_image, TEMP_RAW_PATH)
            processed_image = s3_helper.download_object(
                s3_key_processed_image, TEMP_PROCESSED_PATH
            )

            logger.info(
                f"Images downloaded from S3: {s3_key_raw_image} and {s3_key_processed_image}"
            )

            # Display the images
            with col2:
                # Display the raw image
                st.image(
                    TEMP_RAW_PATH,
                    caption="Raw Image",
                    use_column_width=True,
                )

            with col3:
                # Display the processed image
                st.image(
                    TEMP_PROCESSED_PATH,
                    caption="Processed Image",
                    use_column_width=True,
                )

                # Display the famous people recognized
                json_str_response = item.get(
                    "rekognition_detect_face_response", {}
                ).get("S")
                json_response = json.loads(json_str_response)
                names = [
                    celeb.get("Name", "NOT DETECTED")
                    for celeb in json_response["CelebrityFaces"]
                ]
                links = [
                    celeb.get("Urls", ["NOT DETECTED"])[0]
                    for celeb in json_response["CelebrityFaces"]
                ]
                st.write(
                    f"Names: {', '.join(names)}",
                )
                st.write(
                    f"Links: {', '.join(links)}",
                )

        except Exception as e:
            logger.exception("Error obtaining the image from DynamoDB", exc_info=e)
            st.error("Make sure to upload the video first!")
