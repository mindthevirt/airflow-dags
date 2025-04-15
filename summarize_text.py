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
    logging.info(f"Opening DB at {db_path}")

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            
            if not job:
                raise Exception(f"Job {job_id} not found in database")

            transcription = dict(job).get("transcription")
            if not transcription:
                raise Exception(f"No transcription found for job {job_id}")

            prompt = f"""You are a professional summarization agent. Please summarize the following transcription into clear, concise bullet points suitable for someone who wants to quickly understand what was said.\n\nTranscription:\n{transcription}"""

            try:
                logging.info(f"Calling OpenAI API for job {job_id}")
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
                logging.info(f"Successfully generated summary for job {job_id}")
            except Exception as e:
                logging.error(f"OpenAI API call failed for job {job_id}: {e}")
                raise Exception(f"Summarization failed: {e}")

            conn.execute(
                'UPDATE jobs SET summary = ?, status = ?, completed_at = CURRENT_TIMESTAMP WHERE id = ?',
                (summary, 'completed', job_id)
            )
            conn.commit()

            logging.info(f"Summary completed for job {job_id}")
            logging.debug(f"Summary content: {summary[:200]}...")

    except sqlite3.Error as e:
        logging.error(f"Database error for job {job_id}: {e}")
        raise Exception(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Failed to summarize job {job_id}: {e}")
        raise

    return summary
