from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

TRIGGER_KWARGS = {
    "wait_for_completion": True,
    "poke_interval": 30,
    "reset_dag_run": True,
    "allowed_states": ["success"],
    "failed_states": ["failed"],
}


with DAG(
    dag_id="arxiv_monday_orchestrator",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * 1",  # Every Monday 06:00 (local scheduler timezone)
    catchup=False,
    max_active_runs=1,
) as dag:
    fetch = TriggerDagRunOperator(
        task_id="trigger_fetch",
        trigger_dag_id="arxiv_fetch_pipeline",
        **TRIGGER_KWARGS,
    )

    summarize = TriggerDagRunOperator(
        task_id="trigger_summarize",
        trigger_dag_id="arxiv_summarize_pipeline",
        **TRIGGER_KWARGS,
    )

    rank = TriggerDagRunOperator(
        task_id="trigger_rank",
        trigger_dag_id="arxiv_ranking_pipeline",
        **TRIGGER_KWARGS,
    )

    post = TriggerDagRunOperator(
        task_id="trigger_post",
        trigger_dag_id="arxiv_post_pipeline",
        **TRIGGER_KWARGS,
    )

    fetch >> summarize >> rank >> post
