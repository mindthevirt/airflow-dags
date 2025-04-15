import sqlite3
import logging


def validate_file(job_id, db_path):
    logging.info(f"Opening DB at {db_path}")
    inserted = False

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Check if the job exists
            cursor.execute("SELECT id, status FROM jobs WHERE id = ?", (job_id,))
            result = cursor.fetchone()

            if result is None:
                logging.info(f"Job {job_id} not found. Creating new entry.")
                cursor.execute("INSERT INTO jobs (id, status) VALUES (?, ?)", (job_id, 'created'))
                conn.commit()
                inserted = True
                logging.info(f"Successfully created new job entry for {job_id}")
            else:
                job_id, status = result
                logging.info(f"Job {job_id} already exists with status: {status}")

    except sqlite3.Error as e:
        logging.error(f"Database error while validating job {job_id}: {e}")
        raise Exception(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Failed to validate job {job_id}: {e}")
        raise

    return {"job_id": job_id, "inserted": inserted}
