import sys
import os
sys.path.append(os.path.dirname(__file__))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.models import Variable
import requests
import sqlite3
import logging

from validate_file import validate_file
from summarize_text import summarize_text
from s3_utils import download_db_from_s3, upload_db_to_s3, ensure_db_exists

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='process_uploaded_file',
    description='Transcribe and summarize uploaded file',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['audio', 'processing'],
    params={
        "job_id": Param("", type="string", description="Job ID from the database"),
        "s3_url": Param("s3://mlflow.iaie02/recording.mp4", type="string", description="S3 URL to the audio file")
    }
) as dag:

    def validate_op(**kwargs):
        job_id = kwargs["params"]["job_id"]
        logging.info(f"Validating job: {job_id}")

        ensure_db_exists()
        db_path = download_db_from_s3()
        result = validate_file(job_id, db_path)
        upload_db_to_s3()
        return result

    def fetch_transcription_op(**kwargs):
        job_id = kwargs["params"]["job_id"]
        logging.info(f"Fetching transcription for job: {job_id}")

        whisper_result_url = Variable.get("WHISPER_RESULT_URL")
        url = f"{whisper_result_url}/{job_id}"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
        except Exception as e:
            raise Exception(f"Failed to fetch transcription for job {job_id}: {e}")

        transcription = response.json().get("transcription")
        if not transcription:
            raise Exception("No transcription found in response")

        ensure_db_exists()
        db_path = download_db_from_s3()
        conn = sqlite3.connect(db_path)
        conn.execute(
            'UPDATE jobs SET transcription = ?, status = ? WHERE id = ?',
            (transcription, 'transcribed', job_id)
        )
        conn.commit()
        conn.close()
        upload_db_to_s3()
        logging.info(f"Transcription saved for job {job_id}")

    def summarize_op(**kwargs):
        job_id = kwargs["params"]["job_id"]
        logging.info(f"Summarizing job: {job_id}")

        ensure_db_exists()
        db_path = download_db_from_s3()
        result = summarize_text(job_id, db_path)
        upload_db_to_s3()
        return result

    t_validate = PythonOperator(
        task_id='validate_file',
        python_callable=validate_op
    )

    t_transcribe = KubernetesPodOperator(
        task_id="transcribe_file",
        name="whisper-transcribe",
        namespace="default",
        image="ghcr.io/mindthevirt/whisper-service:latest",
        cmds=["curl"],
        arguments=[
            "-X", "POST",
            "-F", "job_id={{ params.job_id }}",
            "-F", "s3_url={{ params.s3_url }}",
            "http://localhost:6000/transcribe"
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        env_vars={
            "PYTHONUNBUFFERED": "1",
        }
    )

    t_fetch_transcription = PythonOperator(
        task_id='fetch_transcription',
        python_callable=fetch_transcription_op
    )

    t_summarize = PythonOperator(
        task_id='summarize_text',
        python_callable=summarize_op
    )

    t_validate >> t_transcribe >> t_fetch_transcription >> t_summarize
