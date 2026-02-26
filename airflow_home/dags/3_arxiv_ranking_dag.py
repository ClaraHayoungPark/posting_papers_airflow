import logging
import os
import re
import sqlite3
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

DB_PATH = "/Users/hayoung/airflow-local/arxiv_pipeline.db"
RANKING_BATCH_SIZE = max(1, int(os.getenv("ARXIV_RANKING_BATCH_SIZE", 5)))

SELECT_UNRANKED_BATCH = """
SELECT p.arxiv_id, p.title, s.summary_500
FROM papers p
JOIN summaries s ON s.arxiv_id = p.arxiv_id
LEFT JOIN rankings r ON r.arxiv_id = p.arxiv_id
WHERE r.arxiv_id IS NULL
ORDER BY p.fetched_at DESC
LIMIT ?
"""

COUNT_UNRANKED = """
SELECT COUNT(*)
FROM papers p
JOIN summaries s ON s.arxiv_id = p.arxiv_id
LEFT JOIN rankings r ON r.arxiv_id = p.arxiv_id
WHERE r.arxiv_id IS NULL
"""

UPSERT_RANKING = """
INSERT OR REPLACE INTO rankings (arxiv_id, score, ranked_at)
VALUES (?, ?, datetime('now'))
"""


def score_paper_openai(title: str, summary: str) -> int:
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing.")

    client = OpenAI(api_key=api_key)
    prompt = f"""
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

Title: {title}
Summary: {summary}
""".strip()
    raw = client.responses.create(model="gpt-5-mini", input=prompt).output_text.strip()
    match = re.search(r"[1-5]", raw)
    if not match:
        raise ValueError(f"invalid score output: {raw!r}")
    return max(1, min(5, int(match.group(0))))


def rank_candidates():
    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(SELECT_UNRANKED_BATCH, (RANKING_BATCH_SIZE,))
        rows = cur.fetchall()
        if not rows:
            return 0

        success_count = 0
        for arxiv_id, title, summary in rows:
            try:
                score = score_paper_openai(title, summary)
            except Exception as exc:
                logger.warning("[ranking] skip %s: %s", arxiv_id, exc)
                continue
            cur.execute(UPSERT_RANKING, (arxiv_id, score))
            success_count += 1
        return success_count


def should_trigger_next_batch(ti):
    processed = ti.xcom_pull(task_ids="rank_candidates") or 0
    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(COUNT_UNRANKED)
        remaining = cur.fetchone()[0]
    return processed > 0 and remaining > 0


with DAG(
    dag_id="arxiv_ranking_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    rank = PythonOperator(task_id="rank_candidates", python_callable=rank_candidates)
    check_more = ShortCircuitOperator(
        task_id="check_more_candidates",
        python_callable=should_trigger_next_batch,
    )
    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_batch",
        trigger_dag_id="arxiv_ranking_pipeline",
        wait_for_completion=False,
    )

    rank >> check_more >> trigger_next
