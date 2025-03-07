from extract import extract_burritos_tacos_count
from upload import upload_to_s3
import os

S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

if __name__ == "__main__":
    file_path = extract_burritos_tacos_count()
    if file_path:
        s3_key = os.path.basename(file_path)
        s3_url = upload_to_s3(file_path, S3_BUCKET_NAME, s3_key)
