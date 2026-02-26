from datetime import datetime
import logging
import re
import sqlite3
import requests
import feedparser
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

# SQLite DB 파일 경로
DB_PATH = "/Users/hayoung/airflow-local/arxiv_pipeline.db"

# arXiv Atom API 엔드포인트
ARXIV_URL = "http://export.arxiv.org/api/query"
logger = logging.getLogger(__name__)

EXCLUDED_KEYWORDS = [
    "survey",
    "benchmark",
    "leaderboard",
    "dataset",
    "shared task",
    "competition",
]

EXCLUDED_DOMAIN_KEYWORDS = [
    "clinical",
    "patient",
    "hospital",
    "radiology",
    "ehr",
    "genomics",
    "molecule",
    "drug discovery",
]

EXCLUDED_PATTERNS = [
    re.compile(r"\b(only|purely|solely)\s+(benchmark|evaluation)\b", re.IGNORECASE),
    re.compile(r"\bbenchmark\s+(study|analysis)\b", re.IGNORECASE),
]

MIN_ABSTRACT_LENGTH = 400


def should_include_entry(title: str, abstract: str):
    """고정 규칙으로 논문 포함 여부를 판단한다."""
    normalized_abstract = " ".join(abstract.replace("\n", " ").split())
    searchable = f"{title} {normalized_abstract}".lower()

    if len(normalized_abstract) < MIN_ABSTRACT_LENGTH:
        return False, "short_abstract"

    if any(keyword in searchable for keyword in EXCLUDED_KEYWORDS):
        return False, "excluded_keyword"

    if any(keyword in searchable for keyword in EXCLUDED_DOMAIN_KEYWORDS):
        return False, "excluded_domain"

    if any(pattern.search(searchable) for pattern in EXCLUDED_PATTERNS):
        return False, "excluded_pattern"

    return True, None


def fetch_arxiv_papers(max_papers=100):
    """cs.AI / cs.CL / cs.LG 최신 논문 최대 N개를 조회한다."""

    # XCom -> DB INSERT에 전달할 결과 버퍼
    rows = []

    # 최신 업데이트 기준으로 max_papers개만 단일 조회
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

    entries = feedparser.parse(r.text).entries
    filtered_out = {
        "short_abstract": 0,
        "excluded_keyword": 0,
        "excluded_domain": 0,
        "excluded_pattern": 0,
    }

    for e in entries:
        title = e.title.strip()
        abstract = e.summary.replace("\n", " ").strip()
        include, reason = should_include_entry(title, abstract)
        if not include:
            filtered_out[reason] += 1
            continue

        rows.append(
            (
                e.id.split("/")[-1],
                title,
                ", ".join(a.name for a in e.authors),
                e.link,
                abstract,
            )
        )

    logger.info(
        (
            "Fetched %d papers from arXiv (max_papers=%d). "
            "kept=%d filtered_short=%d filtered_keyword=%d "
            "filtered_domain=%d filtered_pattern=%d filtered_total=%d"
        ),
        len(entries),
        max_papers,
        len(rows),
        filtered_out["short_abstract"],
        filtered_out["excluded_keyword"],
        filtered_out["excluded_domain"],
        filtered_out["excluded_pattern"],
        sum(filtered_out.values()),
    )
    return {
        "rows": rows,
        "stats": {
            "fetched_from_api": len(entries),
            "filtered_out": filtered_out,
            "filtered_total": sum(filtered_out.values()),
            "kept_after_filter": len(rows),
        },
    }


def store_papers_to_sqlite():
    """fetch 결과를 XCom에서 받아 papers 테이블에 저장한다."""

    ti = get_current_context()["ti"]
    fetch_result = ti.xcom_pull(task_ids="fetch_arxiv") or {}
    if isinstance(fetch_result, dict) and "rows" in fetch_result:
        rows = fetch_result.get("rows") or []
        fetch_stats = fetch_result.get("stats") or {}
    else:
        rows = fetch_result if isinstance(fetch_result, list) else []
        fetch_stats = {}

    fetched_count = len(rows)
    fetched_from_api = int(fetch_stats.get("fetched_from_api", fetched_count))
    filtered_total = int(fetch_stats.get("filtered_total", 0))

    logger.info(
        (
            "Fetch/Filter summary: fetched_from_api=%d "
            "filtered_out_by_rules=%d kept_after_filter=%d"
        ),
        fetched_from_api,
        filtered_total,
        fetched_count,
    )
    if not rows:
        logger.info("No papers to store.")
        return {
            "fetched_from_api": fetched_from_api,
            "filtered_out_by_rules": filtered_total,
            "kept_after_filter": 0,
            "inserted": 0,
            "ignored_duplicates": 0,
        }

    conn = sqlite3.connect(DB_PATH)
    before_changes = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO papers VALUES (?, ?, ?, ?, ?, datetime('now','localtime'))",
        rows,
    )
    inserted_count = conn.total_changes - before_changes
    ignored_count = fetched_count - inserted_count
    conn.commit()
    conn.close()

    logger.info(
        (
            "SQLite store result: fetched_from_api=%d filtered_out_by_rules=%d "
            "kept_after_filter=%d inserted=%d ignored_duplicates=%d"
        ),
        fetched_from_api,
        filtered_total,
        fetched_count,
        inserted_count,
        ignored_count,
    )
    return {
        "fetched_from_api": fetched_from_api,
        "filtered_out_by_rules": filtered_total,
        "kept_after_filter": fetched_count,
        "inserted": inserted_count,
        "ignored_duplicates": ignored_count,
    }


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
