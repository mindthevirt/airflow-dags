import os
import sqlite3
import logging

def validate_file(job_id, db_path):
    logging.info(f"Validating job: {job_id}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
    conn.close()

    if not job:
        raise Exception(f"Job {job_id} not found")

    job_dict = dict(job)
    file_path = job_dict.get("file_path")

    if not file_path:
        raise Exception(f"No file path found for job {job_id}")

    if not os.path.exists(file_path):
        raise Exception(f"File does not exist: {file_path}")

    if not file_path.lower().endswith(".wav"):
        raise Exception(f"Unsupported file format: {file_path}. Only .wav files are supported.")

    logging.info(f"Validation passed for file: {file_path}")
