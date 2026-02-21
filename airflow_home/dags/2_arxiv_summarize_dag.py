from datetime import datetime          # DAG 시작 날짜 설정용
import sqlite3                         # SQLite DB 접근
import os                              # 환경변수 읽기

from airflow import DAG                # Airflow DAG 정의
from airflow.operators.python import PythonOperator  # Python 함수 실행용 Operator

from dotenv import load_dotenv         # .env 파일 로드

load_dotenv()                          # .env의 환경변수를 현재 환경에 적용

DB_PATH = "/Users/hayoung/airflow-local/arxiv_pipeline.db"   # SQLite DB 경로


def summarize_text_openai(title: str, abstract: str):
    # DAG 파싱 시 import 에러를 피하기 위해 실행 시점에만 import
    from openai import OpenAI

    # OpenAI API 키 확인
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing.")

    # OpenAI 클라이언트 생성
    client = OpenAI(api_key=api_key)

    # 요약 요청 프롬프트
    prompt = f"""
Summarize the following abstract in English in about 500 characters (±50).
One paragraph. Include problem, method, and results.
Do not add new information. Output only the summary.

Title: {title}
Abstract: {abstract}
""".strip()

    # OpenAI API 호출
    resp = client.responses.create(
        model="gpt-5-mini",
        input=prompt,
    )

    # 모델이 생성한 텍스트 요약 추출
    summary = resp.output_text.strip()

    # 사용된 모델명 기록(없으면 unknown)
    model = getattr(resp, "model", "unknown")

    return summary, model


def summarize_latest_5():
    # SQLite DB 연결
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 아직 요약되지 않은 최신 논문 5개 조회
    cur.execute("""
        SELECT p.arxiv_id, p.title, p.abstract
        FROM papers p
        LEFT JOIN summaries s ON s.arxiv_id = p.arxiv_id
        WHERE s.arxiv_id IS NULL
        ORDER BY p.fetched_at DESC
        LIMIT 5
    """)
    rows = cur.fetchall()

    # 각 논문에 대해 요약 생성 후 저장
    for arxiv_id, title, abstract in rows:
        summary, model = summarize_text_openai(title, abstract)

        cur.execute("""
            INSERT OR IGNORE INTO summaries (arxiv_id, summary_500, model)
            VALUES (?, ?, ?)
        """, (arxiv_id, summary, model))

    # DB 반영 및 연결 종료
    conn.commit()
    conn.close()


# Airflow DAG 정의
with DAG(
    dag_id="arxiv_summarize_pipeline",   
    start_date=datetime(2026, 1, 1),    
    schedule="@daily",                   
    catchup=False,                       # 과거 실행분 자동 실행 안 함
) as dag:

    summarize = PythonOperator(
        task_id="summarize_latest_5",
        python_callable=summarize_latest_5,
    )