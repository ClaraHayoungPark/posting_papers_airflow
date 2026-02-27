import logging
import os
import re
import sqlite3
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from arxiv_common import (
    ensure_pipeline_support_tables,
    get_db_path,
    get_env_float,
    get_env_int,
)

logger = logging.getLogger(__name__)

DB_PATH = get_db_path()
RANKING_BATCH_SIZE = get_env_int("ARXIV_RANKING_BATCH_SIZE", 5, minimum=1)
RANKING_OPENAI_TIMEOUT = get_env_float(
    "ARXIV_RANKING_OPENAI_TIMEOUT", 120.0, minimum=1.0
)
RANKING_OPENAI_RETRIES = get_env_int("ARXIV_RANKING_OPENAI_RETRIES", 2, minimum=0)
RANKING_MAX_FAILURES = get_env_int("ARXIV_RANKING_MAX_FAILURES", 3, minimum=1)
RANKING_STAGE = "ranking"

SELECT_UNRANKED_BATCH = """
SELECT p.arxiv_id, p.title, s.summary_500
FROM papers p
JOIN summaries s ON s.arxiv_id = p.arxiv_id
LEFT JOIN rankings r ON r.arxiv_id = p.arxiv_id
LEFT JOIN processing_failures f
  ON f.arxiv_id = p.arxiv_id
 AND f.stage = ?
WHERE r.arxiv_id IS NULL
  AND COALESCE(f.failure_count, 0) < ?
ORDER BY COALESCE(f.failure_count, 0) ASC, p.fetched_at DESC
LIMIT ?
"""

COUNT_UNRANKED = """
SELECT COUNT(*)
FROM papers p
JOIN summaries s ON s.arxiv_id = p.arxiv_id
LEFT JOIN rankings r ON r.arxiv_id = p.arxiv_id
LEFT JOIN processing_failures f
  ON f.arxiv_id = p.arxiv_id
 AND f.stage = ?
WHERE r.arxiv_id IS NULL
  AND COALESCE(f.failure_count, 0) < ?
"""

UPSERT_RANKING = """
INSERT OR REPLACE INTO rankings (arxiv_id, score, ranked_at)
VALUES (?, ?, datetime('now'))
"""

UPSERT_FAILURE = """
INSERT INTO processing_failures (
    stage,
    arxiv_id,
    failure_count,
    last_error,
    last_failed_at
)
VALUES (?, ?, 1, ?, datetime('now'))
ON CONFLICT(stage, arxiv_id) DO UPDATE SET
    failure_count = processing_failures.failure_count + 1,
    last_error = excluded.last_error,
    last_failed_at = excluded.last_failed_at
"""

DELETE_FAILURE = """
DELETE FROM processing_failures
WHERE stage = ? AND arxiv_id = ?
"""


def get_openai_client():
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing.")

    return OpenAI(
        api_key=api_key,
        timeout=RANKING_OPENAI_TIMEOUT,
        max_retries=RANKING_OPENAI_RETRIES,
    )


def score_paper_openai(client, title: str, summary: str) -> int:
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
    ensure_pipeline_support_tables()
    client = get_openai_client()

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(
            SELECT_UNRANKED_BATCH,
            (RANKING_STAGE, RANKING_MAX_FAILURES, RANKING_BATCH_SIZE),
        )
        rows = cur.fetchall()
    if not rows:
        return {"success": 0, "failed": 0, "attempted": 0}

    success_rows = []
    failed_rows = []
    for arxiv_id, title, summary in rows:
        try:
            score = score_paper_openai(client, title, summary)
        except Exception as exc:
            logger.warning("[ranking] skip %s: %s", arxiv_id, exc)
            failed_rows.append((RANKING_STAGE, arxiv_id, f"{type(exc).__name__}: {exc}"[:500]))
            continue
        success_rows.append((arxiv_id, score))

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        if success_rows:
            cur.executemany(UPSERT_RANKING, success_rows)
            cur.executemany(
                DELETE_FAILURE,
                [(RANKING_STAGE, arxiv_id) for arxiv_id, _ in success_rows],
            )
        if failed_rows:
            cur.executemany(UPSERT_FAILURE, failed_rows)

    logger.info(
        "[ranking] done success=%d failed=%d batch=%d",
        len(success_rows),
        len(failed_rows),
        len(rows),
    )
    return {
        "success": len(success_rows),
        "failed": len(failed_rows),
        "attempted": len(rows),
    }


def should_trigger_next_batch(ti):
    ensure_pipeline_support_tables()
    result = ti.xcom_pull(task_ids="rank_candidates") or {}
    processed = int(result.get("attempted", 0) or 0)
    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(COUNT_UNRANKED, (RANKING_STAGE, RANKING_MAX_FAILURES))
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
