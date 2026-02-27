from datetime import datetime
import logging
import os
import sqlite3
import time

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
BATCH_SIZE = get_env_int("ARXIV_SUMMARY_BATCH_SIZE", 5, minimum=1)
OPENAI_TIMEOUT = get_env_float("ARXIV_SUMMARY_OPENAI_TIMEOUT", 120.0, minimum=1.0)
OPENAI_MAX_RETRIES = get_env_int("ARXIV_SUMMARY_OPENAI_RETRIES", 2, minimum=0)
PER_PAPER_RETRY = get_env_int("ARXIV_SUMMARY_PER_PAPER_RETRY", 2, minimum=1)
MAX_FAILURES = get_env_int("ARXIV_SUMMARY_MAX_FAILURES", 3, minimum=1)
SUMMARY_STAGE = "summarize"
SUMMARY_MAX_CHARS = 240

SELECT_UNSUMMARIZED_BATCH = """
    SELECT p.arxiv_id, p.title, p.abstract
    FROM papers p
    LEFT JOIN summaries s ON s.arxiv_id = p.arxiv_id
    LEFT JOIN processing_failures f
      ON f.arxiv_id = p.arxiv_id
     AND f.stage = ?
    WHERE s.arxiv_id IS NULL
      AND COALESCE(f.failure_count, 0) < ?
    ORDER BY COALESCE(f.failure_count, 0) ASC, p.fetched_at DESC
    LIMIT ?
"""

COUNT_UNSUMMARIZED = """
    SELECT COUNT(*)
    FROM papers p
    LEFT JOIN summaries s ON s.arxiv_id = p.arxiv_id
    LEFT JOIN processing_failures f
      ON f.arxiv_id = p.arxiv_id
     AND f.stage = ?
    WHERE s.arxiv_id IS NULL
      AND COALESCE(f.failure_count, 0) < ?
"""

INSERT_SUMMARY = """
    INSERT OR IGNORE INTO summaries (arxiv_id, summary_500, model)
    VALUES (?, ?, ?)
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
        timeout=OPENAI_TIMEOUT,
        max_retries=OPENAI_MAX_RETRIES,
    )


def summarize_text_openai(client, title: str, abstract: str):
    prompt = f"""
Summarize the following abstract in English.
Hard limit: 240 characters maximum (including spaces and punctuation).
If your draft is longer than 240 characters, rewrite it until it is 240 or fewer.
One paragraph. Include problem, method, and key findings/evidence.
Do not add new information. Output only the summary.

Title: {title}
Abstract: {abstract}
""".strip()

    resp = client.responses.create(model="gpt-5-mini", input=prompt)
    summary = " ".join(resp.output_text.strip().split())
    if len(summary) > SUMMARY_MAX_CHARS:
        summary = summary[: SUMMARY_MAX_CHARS - 3].rstrip() + "..."
    return summary, getattr(resp, "model", "unknown")


def summarize_candidates():
    ensure_pipeline_support_tables()
    client = get_openai_client()

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(SELECT_UNSUMMARIZED_BATCH, (SUMMARY_STAGE, MAX_FAILURES, BATCH_SIZE))
        rows = cur.fetchall()
    logger.info(
        "[summarize] candidates=%d (batch_size=%d, max_failures=%d)",
        len(rows),
        BATCH_SIZE,
        MAX_FAILURES,
    )

    success_rows = []
    failed_rows = []

    for idx, (arxiv_id, title, abstract) in enumerate(rows, start=1):
        logger.info("[summarize] %d/%d arxiv_id=%s", idx, len(rows), arxiv_id)

        summary = model = None
        last_err = None
        for attempt in range(1, PER_PAPER_RETRY + 1):
            try:
                summary, model = summarize_text_openai(client, title, abstract)
                break
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "[summarize] retry=%d/%d arxiv_id=%s error=%s: %s",
                    attempt,
                    PER_PAPER_RETRY,
                    arxiv_id,
                    type(exc).__name__,
                    exc,
                )
                if attempt < PER_PAPER_RETRY:
                    time.sleep(attempt)

        if summary is None or model is None:
            failed_rows.append(
                (
                    SUMMARY_STAGE,
                    arxiv_id,
                    f"{type(last_err).__name__}: {last_err}"[:500],
                )
            )
            logger.warning(
                "[summarize] skip arxiv_id=%s after retries error=%s: %s",
                arxiv_id,
                type(last_err).__name__,
                last_err,
            )
            continue

        success_rows.append((arxiv_id, summary, model))

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        if success_rows:
            cur.executemany(INSERT_SUMMARY, success_rows)
            cur.executemany(
                DELETE_FAILURE,
                [(SUMMARY_STAGE, arxiv_id) for arxiv_id, _, _ in success_rows],
            )
        if failed_rows:
            cur.executemany(UPSERT_FAILURE, failed_rows)

    logger.info(
        "[summarize] done success=%d failed=%d batch=%d",
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
    result = ti.xcom_pull(task_ids="summarize_candidates") or {}
    success = int(result.get("success", 0) or 0)
    failed = int(result.get("failed", 0) or 0)
    attempted = int(result.get("attempted", 0) or 0)

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(COUNT_UNSUMMARIZED, (SUMMARY_STAGE, MAX_FAILURES))
        remaining = cur.fetchone()[0]

    logger.info(
        "[summarize] attempted=%d success=%d failed=%d remaining=%d",
        attempted,
        success,
        failed,
        remaining,
    )
    # 이번 배치에서 1건이라도 시도했다면 다음 배치로 계속 진행
    return remaining > 0 and attempted > 0


with DAG(
    dag_id="arxiv_summarize_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    summarize = PythonOperator(
        task_id="summarize_candidates",
        python_callable=summarize_candidates,
    )

    check_more = ShortCircuitOperator(
        task_id="check_more_candidates",
        python_callable=should_trigger_next_batch,
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next_batch",
        trigger_dag_id="arxiv_summarize_pipeline",
        wait_for_completion=False,
    )

    summarize >> check_more >> trigger_next
