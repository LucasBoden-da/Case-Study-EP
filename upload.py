from dotenv import load_dotenv
import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load database and AWS credentials from environment variables
load_dotenv()

def upload_to_s3(file_path, bucket_name, s3_key):
    """
    Uploads a local file to an S3 bucket.
        
    Parameters:
    - file_path (str): Path to the local file.
    - bucket_name (str): Name of the target S3 bucket.
    - s3_key (str): Destination path and filename in the S3 bucket.

    Returns:
    - str: S3 URL of the uploaded file if successful.
    - None: If an error occurs.
    """

    if not os.path.exists(file_path):
        logging.error(f"File {file_path} not found.")
        return None
    
    try:
        s3_client = boto3.client('s3')
        s3_client.upload_file(file_path, bucket_name, s3_key)
        s3_url = f"s3://{bucket_name}/{s3_key}"
        logging.info(f"File uploaded: {s3_url}")
        return s3_url
    except NoCredentialsError:
        logging.error("AWS credentials not found.")
    except ClientError as e:
        logging.error(f"Error uploading to S3: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    return None
