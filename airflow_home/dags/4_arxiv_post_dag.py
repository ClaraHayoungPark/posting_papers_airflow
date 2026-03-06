from datetime import datetime
import logging
import os
import sqlite3
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from requests_oauthlib import OAuth1

logger = logging.getLogger(__name__)
DEFAULT_DB_PATH = str(Path(__file__).resolve().parents[2] / "arxiv_pipeline.db")
DB_PATH = None

POST_SCORE = 4
X_POST_URL = "https://api.twitter.com/2/tweets"


def get_db_path() -> str:
    return os.getenv("ARXIV_DB_PATH", DEFAULT_DB_PATH)


def ensure_pipeline_support_tables() -> None:
    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS papers (
                arxiv_id   TEXT PRIMARY KEY,
                title      TEXT,
                authors    TEXT,
                url        TEXT,
                abstract   TEXT,
                fetched_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS summaries (
                arxiv_id    TEXT PRIMARY KEY,
                summary_500 TEXT NOT NULL,
                model       TEXT,
                created_at  TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rankings (
                arxiv_id  TEXT PRIMARY KEY,
                score     INTEGER NOT NULL CHECK(score BETWEEN 1 AND 5),
                ranked_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS posted (
                arxiv_id  TEXT PRIMARY KEY,
                posted_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id)
            )
            """
        )


DB_PATH = get_db_path()


def post_to_x():
    ensure_pipeline_support_tables()

    ck = os.getenv("CONSUMER_KEY")
    cs = os.getenv("CONSUMER_SECRET")
    at = os.getenv("ACCESS_TOKEN")
    ats = os.getenv("ACCESS_TOKEN_SECRET")
    if not all([ck, cs, at, ats]):
        raise RuntimeError("Missing X credentials (CONSUMER_KEY/SECRET, ACCESS_TOKEN/SECRET).")

    auth = OAuth1(ck, cs, at, ats)

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT p.arxiv_id, s.summary_500
            FROM rankings r
            JOIN summaries s ON s.arxiv_id = r.arxiv_id
            JOIN papers p ON p.arxiv_id = r.arxiv_id
            LEFT JOIN posted t ON t.arxiv_id = r.arxiv_id
            WHERE r.score = ?
              AND t.arxiv_id IS NULL
            ORDER BY r.ranked_at DESC
            """,
            (POST_SCORE,),
        )
        rows = cur.fetchall()

        posted_count = 0
        for arxiv_id, summary in rows:
            text = f"{summary}\nhttps://arxiv.org/abs/{arxiv_id}"
            resp = requests.post(
                X_POST_URL,
                auth=auth,
                json={"text": text},
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if resp.status_code == 201:
                cur.execute("INSERT OR IGNORE INTO posted (arxiv_id) VALUES (?)", (arxiv_id,))
                posted_count += 1
            else:
                logger.warning("[post] fail arxiv_id=%s status=%s body=%s", arxiv_id, resp.status_code, resp.text)

        conn.commit()
        logger.info("[post] done posted=%d/%d (score=%d)", posted_count, len(rows), POST_SCORE)


with DAG(
    dag_id="arxiv_post_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="post_to_x", python_callable=post_to_x)
