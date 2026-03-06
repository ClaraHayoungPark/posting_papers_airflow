import os
import re
import sqlite3
from datetime import datetime
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


DB_PATH = get_db_path()
BATCH_SIZE = get_env_int("ARXIV_RANKING_BATCH_SIZE", 5, minimum=1)

SELECT_BATCH = """
SELECT p.arxiv_id, p.title, s.summary_500
FROM papers p
JOIN summaries s ON s.arxiv_id = p.arxiv_id
LEFT JOIN rankings r ON r.arxiv_id = p.arxiv_id
WHERE r.arxiv_id IS NULL
ORDER BY p.fetched_at DESC
LIMIT ?
"""

UPSERT_RANKING = """
INSERT OR REPLACE INTO rankings (arxiv_id, score, ranked_at)
VALUES (?, ?, datetime('now'))
"""


def get_openai_client():
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing.")
    return OpenAI(api_key=api_key)


RUBRIC_PROMPT = """
You are scoring an AI research paper for long-term impact.

Score from 1 to 5:

1 = minor / narrow / incremental
2 = domain-specific improvement
3 = solid but limited scope
4 = strong contribution with broad relevance
5 = potentially foundational or major advance

Consider overall:
- Importance of the problem
- Novelty of the method
- Theoretical depth
- General applicability
- Strength of evidence

Think through these factors internally.
Return ONLY one integer (1, 2, 3, 4, or 5).
No explanation.
""".strip()


def score_paper(client, title: str, summary: str) -> int:
    prompt = f"""{RUBRIC_PROMPT}

Title: {title}
Summary: {summary}
""".strip()

    raw = client.responses.create(model="gpt-5-mini", input=prompt).output_text.strip()

    # 모델이 혹시라도 다른 텍스트를 섞으면 첫 1~5만 복구
    m = re.search(r"[1-5]", raw)
    if not m:
        raise ValueError(f"invalid score output: {raw!r}")
    return int(m.group(0))


def run_batch():
    ensure_pipeline_support_tables()
    client = get_openai_client()

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(SELECT_BATCH, (BATCH_SIZE,))
        rows = cur.fetchall()

        for arxiv_id, title, summary in rows:
            score = score_paper(client, title, summary)
            cur.execute(UPSERT_RANKING, (arxiv_id, score))


with DAG(
    dag_id="arxiv_ranking_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,  
    catchup=False,
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="rank_batch",
        python_callable=run_batch,
    )
