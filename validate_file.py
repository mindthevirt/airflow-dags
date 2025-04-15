import sqlite3
import logging


def validate_file(job_id, db_path):
    logging.info(f"Opening DB at {db_path}")
    inserted = False

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT id FROM jobs WHERE id = ?", (job_id,))
            result = cursor.fetchone()

            if result is None:
                logging.info(f"Job {job_id} not found. Creating new entry.")
                cursor.execute("INSERT INTO jobs (id, status) VALUES (?, ?)", (job_id, 'created'))
                conn.commit()
                inserted = True
            else:
                logging.info(f"Job {job_id} already exists.")

    except Exception as e:
        logging.error(f"Failed to validate job: {e}")
        raise

    return {"job_id": job_id, "inserted": inserted}
