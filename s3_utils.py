import boto3
import os
import sqlite3
from botocore import UNSIGNED
from botocore.client import Config
from pathlib import Path
from airflow.models import Variable


def get_token():
    TOKEN_PATH = '/etc/secrets/ezua/.auth_token'
    with open(TOKEN_PATH, 'r') as f:
        auth_token = f.read()

    os.environ['AUTH_TOKEN'] = auth_token
    print(os.environ['AUTH_TOKEN'])
    return auth_token


token = get_token()

S3_ENDPOINT = Variable.get("S3_ENDPOINT")
BUCKET_NAME = Variable.get("BUCKET_NAME")
DB_KEY = Variable.get("DB_KEY")
LOCAL_DB_PATH = "/tmp/database.db"
AUTH_TOKEN = token
AWS_ACCESS_KEY_ID = token

# Use boto3 resource instead of client
s3 = boto3.resource('s3', 
                    endpoint_url=S3_ENDPOINT, 
                    verify=False)

def ensure_db_exists():
    local = Path(LOCAL_DB_PATH)

    try: 
        # Check if the file exists in S3
        s3.Object(BUCKET_NAME, DB_KEY).load()
    except s3.meta.client.exceptions.ClientError:
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
    try:
        s3.Bucket(BUCKET_NAME).download_file(DB_KEY, LOCAL_DB_PATH)
    except Exception as e:
        print(f"Error downloading database: {e}")
        # Create a new database if download fails
        create_local_db_schema(Path(LOCAL_DB_PATH))
    return LOCAL_DB_PATH

def upload_db_to_s3():
    try:
        s3.Bucket(BUCKET_NAME).upload_file(LOCAL_DB_PATH, DB_KEY)
    except Exception as e:
        print(f"Error uploading database: {e}")
