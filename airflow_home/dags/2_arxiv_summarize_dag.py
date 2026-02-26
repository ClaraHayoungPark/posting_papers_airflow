from datetime import datetime
import logging
import os
import sqlite3
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

DB_PATH = "/Users/hayoung/airflow-local/arxiv_pipeline.db"
BATCH_SIZE = max(1, int(os.getenv("ARXIV_SUMMARY_BATCH_SIZE", "5")))
OPENAI_TIMEOUT = float(os.getenv("ARXIV_SUMMARY_OPENAI_TIMEOUT", "120"))
OPENAI_MAX_RETRIES = max(0, int(os.getenv("ARXIV_SUMMARY_OPENAI_RETRIES", "2")))
PER_PAPER_RETRY = max(1, int(os.getenv("ARXIV_SUMMARY_PER_PAPER_RETRY", "2")))
SUMMARY_MAX_CHARS = 240

SELECT_UNSUMMARIZED_BATCH = """
    SELECT p.arxiv_id, p.title, p.abstract
    FROM papers p
    LEFT JOIN summaries s ON s.arxiv_id = p.arxiv_id
    WHERE s.arxiv_id IS NULL
    ORDER BY p.fetched_at DESC
    LIMIT ?
"""

COUNT_UNSUMMARIZED = """
    SELECT COUNT(*)
    FROM papers p
    LEFT JOIN summaries s ON s.arxiv_id = p.arxiv_id
    WHERE s.arxiv_id IS NULL
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
    client = get_openai_client()

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(SELECT_UNSUMMARIZED_BATCH, (BATCH_SIZE,))
        rows = cur.fetchall()
        logger.info("[summarize] candidates=%d (batch_size=%d)", len(rows), BATCH_SIZE)

        success_count = 0
        fail_count = 0

        for idx, (arxiv_id, title, abstract) in enumerate(rows, start=1):
            logger.info("[summarize] %d/%d arxiv_id=%s", idx, len(rows), arxiv_id)

            # 논문별 transient 오류를 흡수하기 위한 짧은 재시도 루프
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
                fail_count += 1
                logger.warning(
                    "[summarize] skip arxiv_id=%s after retries error=%s: %s",
                    arxiv_id,
                    type(last_err).__name__,
                    last_err,
                )
                continue

            cur.execute(INSERT_SUMMARY, (arxiv_id, summary, model))
            success_count += 1

        logger.info(
            "[summarize] done success=%d failed=%d batch=%d",
            success_count,
            fail_count,
            len(rows),
        )
        return {
            "success": success_count,
            "failed": fail_count,
            "attempted": len(rows),
        }


def should_trigger_next_batch(ti):
    result = ti.xcom_pull(task_ids="summarize_candidates") or {}
    success = int(result.get("success", 0) or 0)
    failed = int(result.get("failed", 0) or 0)
    attempted = int(result.get("attempted", 0) or 0)

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(COUNT_UNSUMMARIZED)
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
