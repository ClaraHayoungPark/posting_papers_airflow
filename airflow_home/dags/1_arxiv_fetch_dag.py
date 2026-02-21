from datetime import datetime
import sqlite3
import requests
import feedparser
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

# SQLite DB 경로
DB_PATH = "/Users/hayoung/airflow-local/arxiv_pipeline.db"

# arXiv API 엔드포인트
ARXIV_URL = "http://export.arxiv.org/api/query"


def fetch_arxiv_papers(max_results=5):
    """
    arXiv에서 최신 논문 메타데이터를 가져와
    (id, title, authors, url, abstract) 형태로 반환
    """

    # arXiv API 호출
    r = requests.get(
        ARXIV_URL,
        params={
            "search_query": "cat:cs.AI OR cat:cs.CL OR cat:cs.LG",
            "start": 0,
            "max_results": max_results,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        },
    )

    # XML 응답을 파싱해서 논문 리스트 생성
    entries = feedparser.parse(r.text).entries

    # DB에 넣기 좋은 튜플 형태로 변환
    return [
        (
            e.id.split("/")[-1],                  
            e.title.strip(),                      
            ", ".join(a.name for a in e.authors), 
            e.link,                               
            e.summary.replace("\n", " ").strip(), 
        )
        for e in entries
    ]


def store_papers_to_sqlite():
    """
    fetch_arxiv 태스크 결과를 XCom에서 가져와 SQLite에 저장
    """

    # 현재 태스크 인스턴스
    ti = get_current_context()["ti"]

    # 이전 태스크 결과 가져오기
    rows = ti.xcom_pull(task_ids="fetch_arxiv") or []

    # SQLite 저장
    conn = sqlite3.connect(DB_PATH)
    conn.executemany(
        "INSERT OR IGNORE INTO papers VALUES (?, ?, ?, ?, ?, datetime('now','localtime'))",
        rows,
    )
    conn.commit()
    conn.close()


with DAG(
    dag_id="arxiv_fetch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # arXiv에서 논문 수집
    fetch = PythonOperator(
        task_id="fetch_arxiv",
        python_callable=fetch_arxiv_papers,
        op_kwargs={"max_results": 5},
    )

    # SQLite에 저장
    store = PythonOperator(
        task_id="store_to_sqlite",
        python_callable=store_papers_to_sqlite,
    )

    # 실행 순서
    fetch >> store