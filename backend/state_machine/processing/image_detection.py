# External imports
import boto3

rekognition_client = boto3.client("rekognition")


def recognize_celebrities(s3_bucket_name: str, image_key: str):
    """
    Recognize celebrities in an image.
    """
    result = rekognition_client.recognize_celebrities(
        Image={
            "S3Object": {
                "Bucket": s3_bucket_name,
                "Name": image_key,
            },
        },
    )
    return result
