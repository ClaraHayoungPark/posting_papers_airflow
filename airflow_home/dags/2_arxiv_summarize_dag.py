from datetime import datetime
import os
import sqlite3
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_DB_PATH = str(Path(__file__).resolve().parents[2] / "arxiv_pipeline.db")
DB_PATH = None


def get_db_path() -> str:
    return os.getenv("ARXIV_DB_PATH", DEFAULT_DB_PATH)


def get_env_int(name: str, default: int, minimum: int | None = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, value) if minimum is not None else value


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


DB_PATH = get_db_path()
BATCH_SIZE = get_env_int("ARXIV_SUMMARY_BATCH_SIZE", 5, minimum=1)
SUMMARY_MAX_CHARS = 240

SELECT_BATCH = """
SELECT p.arxiv_id, p.title, p.abstract
FROM papers p
LEFT JOIN summaries s ON s.arxiv_id = p.arxiv_id
WHERE s.arxiv_id IS NULL
ORDER BY p.fetched_at DESC
LIMIT ?
"""

INSERT_SUMMARY = """
INSERT OR IGNORE INTO summaries (arxiv_id, summary_500, model)
VALUES (?, ?, ?)
"""


def get_openai_client():
    from openai import OpenAI
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing.")
    return OpenAI(api_key=api_key)


def summarize_240(client, title: str, abstract: str):
    prompt = f"""
Summarize the following abstract in English.
Hard limit: 240 characters maximum (including spaces and punctuation).
One paragraph. Include problem, method, and key evidence.
Do not add new information. Output only the summary.

Title: {title}
Abstract: {abstract}
""".strip()

    resp = client.responses.create(model="gpt-5-mini", input=prompt)
    summary = " ".join(resp.output_text.strip().split())

    if len(summary) > SUMMARY_MAX_CHARS:
        summary = summary[: SUMMARY_MAX_CHARS - 3].rstrip() + "..."

    return summary, getattr(resp, "model", "unknown")


def run_batch():
    ensure_pipeline_support_tables()
    client = get_openai_client()

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(SELECT_BATCH, (BATCH_SIZE,))
        rows = cur.fetchall()

        for arxiv_id, title, abstract in rows:
            summary, model = summarize_240(client, title, abstract)
            cur.execute(INSERT_SUMMARY, (arxiv_id, summary, model))


with DAG(
    dag_id="arxiv_summarize_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,     
    catchup=False,
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="summarize_batch",
        python_callable=run_batch,
    )
