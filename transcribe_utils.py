import requests
import sqlite3
import os
import logging
from airflow.models import Variable
import boto3
from botocore import UNSIGNED
from botocore.client import Config

WHISPER_API_URL = Variable.get("WHISPER_API_URL")
BUCKET_NAME = Variable.get("BUCKET_NAME")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = Variable.get("AWS_REGION")

# Initialize S3 resource
s3 = boto3.resource('s3', 
                    AWS_ACCESS_KEY_ID,
                    AWS_SECRET_ACCESS_KEY,
                    region_name=AWS_REGION,
                    verify=False)

def download_from_s3(s3_url):
    """Download a file from S3 to a temporary location"""
    if not s3_url.startswith('s3://'):
        return s3_url  # Return as is if not an S3 URL
    

    parts = s3_url.replace('s3://', '').split('/', 1)
    bucket = parts[0]
    key = parts[1]
    
    os.makedirs('/tmp', exist_ok=True)
    
    # Generate a temporary file path
    temp_file = f"/tmp/{os.path.basename(key)}"
    
    try:
        logging.info(f"Downloading {s3_url} to {temp_file}")
        s3.Bucket(bucket).download_file(key, temp_file)
        return temp_file
    except Exception as e:
        logging.error(f"Failed to download from S3: {e}")
        raise

def run_transcription(job_id, db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
    conn.close()

    if not job:
        raise Exception(f"Job {job_id} not found in DB")

    job_dict = dict(job)
    file_path = job_dict.get("file_path")
    
    # Handle S3 URLs
    if file_path and file_path.startswith('s3://'):
        file_path = download_from_s3(file_path)

    if not file_path or not os.path.exists(file_path):
        raise Exception(f"Audio file not found at path: {file_path}")

    with open(file_path, "rb") as f:
        try:
            response = requests.post(
                WHISPER_API_URL,
                files={"file": f},               
                data={"job_id": job_id},          
                timeout=600
            )
        except Exception as e:
            raise Exception(f"Failed to connect to Whisper service: {e}")

    if response.status_code != 200:
        raise Exception(f"Transcription failed: {response.status_code} - {response.text}")

    transcription = response.json().get("transcription")
    if not transcription:
        raise Exception("No transcription returned")

    # Update the job with transcription
    conn = sqlite3.connect(db_path)
    conn.execute(
        'UPDATE jobs SET transcription = ?, status = ? WHERE id = ?',
        (transcription, 'transcribed', job_id)
    )
    conn.commit()
    conn.close()
    
    # Clean up temporary file if it was downloaded from S3
    if job_dict.get("file_path", "").startswith('s3://') and os.path.exists(file_path):
        try:
            os.remove(file_path)
        except Exception as e:
            logging.warning(f"Failed to remove temporary file {file_path}: {e}")
