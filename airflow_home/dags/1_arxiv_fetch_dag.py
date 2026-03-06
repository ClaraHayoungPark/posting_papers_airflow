from datetime import datetime
import logging
import os
import sqlite3
from pathlib import Path

import requests
import feedparser
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

logger = logging.getLogger(__name__)
DEFAULT_DB_PATH = str(Path(__file__).resolve().parents[2] / "arxiv_pipeline.db")
DB_PATH = None
ARXIV_URL = "https://export.arxiv.org/api/query"

EXCLUDED_KEYWORDS = ["survey", "benchmark", "dataset"]
MIN_ABSTRACT_LENGTH = 400


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
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_papers_fetched_at ON papers(fetched_at DESC)"
        )


DB_PATH = get_db_path()


def fetch_arxiv_papers(max_papers=100):
    r = requests.get(
        ARXIV_URL,
        params={
            "search_query": "cat:cs.AI OR cat:cs.CL OR cat:cs.LG",
            "start": 0,
            "max_results": max_papers,
            "sortBy": "lastUpdatedDate",
            "sortOrder": "descending",
        },
        timeout=60,
    )
    r.raise_for_status()

    rows = []
    for e in feedparser.parse(r.text).entries:
        title = e.title.strip()
        abstract = e.summary.replace("\n", " ").strip().lower()

        if len(abstract) < MIN_ABSTRACT_LENGTH:
            continue
        if any(k in (title.lower() + " " + abstract) for k in EXCLUDED_KEYWORDS):
            continue

        rows.append(
            (
                e.id.split("/")[-1],                 # arxiv_id
                title,                               # title
                ", ".join(a.name for a in e.authors),# authors
                e.link,                              # url
                e.summary.replace("\n", " ").strip() # abstract
            )
        )

    logger.info("Fetched=%d, kept=%d", len(feedparser.parse(r.text).entries), len(rows))
    return rows


def store_papers_to_sqlite():
    ensure_pipeline_support_tables()

    ti = get_current_context()["ti"]
    rows = ti.xcom_pull(task_ids="fetch_arxiv") or []
    if not rows:
        logger.info("No rows to store.")
        return

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO papers VALUES (?, ?, ?, ?, ?, datetime('now','localtime'))",
            rows,
        )

    logger.info("Stored rows=%d (duplicates ignored)", len(rows))


with DAG(
    dag_id="arxiv_fetch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    fetch = PythonOperator(
        task_id="fetch_arxiv",
        python_callable=fetch_arxiv_papers,
        op_kwargs={"max_papers": 100},
    )

    store = PythonOperator(
        task_id="store_to_sqlite",
        python_callable=store_papers_to_sqlite,
    )

    fetch >> store
