# External imports
from PIL import Image, ImageDraw, ImageFont

# Own imports
from common.logger import custom_logger

logger = custom_logger()


class ImageDrawing:
    """
    Class to draw faces on an image that interacts with local system.
    Note: This class is not meant to be used in production. Experimental usage only.
        - TODO Enhancement: Add parallel processing for multiple faces drawing.
        - TODO Enhancement: Add drawing configurations (colors, texts, sizes, etc).
    """

    def __init__(
        self,
        image_path: str,
        rekognition_detect_face_response: dict,
        result_demo_output_path: str,
    ):
        """
        :param image_path: Path to the image file in the local system.
        :param rekognition_detect_face_response: Response from AWS Rekognition DetectFaces API.
            Must contain the CelebrityFaces key.
        :param result_demo_output_path: Path to save the modified image.
        """
        self.image_path = image_path
        self.image = Image.open(image_path)
        self.draw = ImageDraw.Draw(self.image)
        self.font = ImageFont.load_default(size=24)
        self.rekognition_detect_face_response = rekognition_detect_face_response
        self.result_demo_output_path = result_demo_output_path

    def draw_faces(self) -> int:
        """
        Draw faces on the image and save it back to the same file.
        :return: Number of faces drawn on the image (total celebrities).
        """
        total_celebrities = 0
        if "CelebrityFaces" not in self.rekognition_detect_face_response:
            logger.info(f"No CelebrityFaces found in the image {self.image_path}")
            return total_celebrities

        for match in self.rekognition_detect_face_response["CelebrityFaces"]:
            total_celebrities += 1
            # Debug prints
            logger.debug("Bounding Box:", match["Face"]["BoundingBox"])
            logger.debug("Image Size:", self.image.size)
            logger.debug("The face identified is:", match["Name"])

            # Draw green square around recognized face
            box = match["Face"]["BoundingBox"]
            img_width, img_height = self.image.size
            left = img_width * box["Left"]
            top = img_height * box["Top"]
            width = img_width * box["Width"]
            height = img_height * box["Height"]
            logger.debug("Drawing Rectangle:", left, top, left + width, top + height)
            self.draw.rectangle(
                [left, top, left + width, top + height], outline="red", width=5
            )

            # Display name at the bottom
            text = match["Name"]
            f = self.draw.textlength(text, self.font)
            logger.debug("Text Position:", left, top + height)
            self.draw.text((left, top + height), text, fill="red", font=self.font)
        return total_celebrities

    def save_image(self) -> None:
        """
        Save the modified image to the local path.
        """
        # Save the modified image locally
        logger.info(
            f"Saving the modified image locally to {self.result_demo_output_path}..."
        )
        self.image.save(self.result_demo_output_path)
