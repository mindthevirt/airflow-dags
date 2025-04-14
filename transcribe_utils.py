import requests
import sqlite3
import os
from airflow.models import Variable

WHISPER_API_URL = Variable.get("WHISPER_API_URL")


def run_transcription(job_id, db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
    conn.close()

    if not job:
        raise Exception(f"Job {job_id} not found in DB")

    job_dict = dict(job)
    file_path = job_dict.get("file_path")

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
