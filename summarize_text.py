import os
import requests
import sqlite3
import logging
from openai import OpenAI
from airflow.models import Variable


CUSTOM_OPENAI_API_KEY = Variable.get("CUSTOM_OPENAI_API_KEY")
CUSTOM_OPENAI_API_BASE = Variable.get("CUSTOM_OPENAI_API_BASE")
CUSTOM_OPENAI_MODEL = Variable.get("CUSTOM_OPENAI_MODEL")


client = OpenAI(
    base_url=CUSTOM_OPENAI_API_BASE,
    api_key=CUSTOM_OPENAI_API_KEY,
)

def summarize_text(job_id, db_path):
    logging.info(f"Summarizing job: {job_id}")
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
    conn.close()

    transcription = dict(job).get("transcription")

    if not transcription:
        raise Exception(f"No transcription found for job {job_id}")

    prompt = f"""You are a professional summarization agent. Please summarize the following transcription into clear, concise bullet points suitable for someone who wants to quickly understand what was said.

Transcription:
{transcription}"""

    try:
        response = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}],
                },
            ],
            model=CUSTOM_OPENAI_MODEL,
            max_tokens=1024,
        )
        summary = response.choices[0].message.content.strip()
    except Exception as e:
        raise Exception(f"Summarization failed: {e}")

    conn = sqlite3.connect(db_path)
    conn.execute(
        'UPDATE jobs SET summary = ?, status = ?, completed_at = CURRENT_TIMESTAMP WHERE id = ?',
        (summary, 'completed', job_id)
    )
    conn.commit()
    conn.close()

    logging.info(f"Summary completed for job {job_id}")