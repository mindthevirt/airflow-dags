import boto3
import os
import sqlite3
from botocore import UNSIGNED
from botocore.client import Config
from pathlib import Path
from airflow.models import Variable


S3_ENDPOINT = Variable.get("S3_ENDPOINT")
BUCKET_NAME = Variable.get("BUCKET_NAME")
DB_KEY = Variable.get("DB_KEY")
LOCAL_DB_PATH = "/tmp/database.db"

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url=S3_ENDPOINT,
    verify=False, config=Config(signature_version=UNSIGNED)
)

def ensure_db_exists():
    local = Path(LOCAL_DB_PATH)

    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=DB_KEY)
    except s3.exceptions.ClientError:
        # File not found in S3, create and upload
        create_local_db_schema(local)
        upload_db_to_s3()

def create_local_db_schema(path: Path):
    conn = sqlite3.connect(path)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            status TEXT,
            transcription TEXT,
            summary TEXT
        )
    ''')
    conn.commit()
    conn.close()

def download_db_from_s3():
    s3.download_file(BUCKET_NAME, DB_KEY, LOCAL_DB_PATH)
    return LOCAL_DB_PATH

def upload_db_to_s3():
    s3.upload_file(LOCAL_DB_PATH, BUCKET_NAME, DB_KEY)
