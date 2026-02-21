# arxiv_post_dag.py

from datetime import datetime
import sqlite3
import os
import tweepy
from airflow import DAG
from airflow.operators.python import PythonOperator

DB_PATH = "/Users/hayoung/airflow-local/papers.db"


def post_to_x():
    # X 인증
    client = tweepy.Client(
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_SECRET"],
    )

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 아직 포스팅 안 된 최신 5개
    cur.execute("""
        SELECT p.arxiv_id, p.title, s.summary_500, p.url
        FROM summaries s
        JOIN papers p ON p.arxiv_id = s.arxiv_id
        LEFT JOIN posted t ON t.arxiv_id = p.arxiv_id
        WHERE t.arxiv_id IS NULL
        ORDER BY s.created_at DESC
        LIMIT 5
    """)
    rows = cur.fetchall()

    for arxiv_id, title, summary, url in rows:
        tweet = f"{title}\n\n{summary}\n\n{url}"
        tweet = tweet[:280]  # X 글자 제한 안전장치

        client.create_tweet(text=tweet)

        # 포스팅 완료 기록
        cur.execute("""
            INSERT OR IGNORE INTO posted (arxiv_id)
            VALUES (?)
        """, (arxiv_id,))

    conn.commit()
    conn.close()


with DAG(
    dag_id="arxiv_post_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    post = PythonOperator(
        task_id="post_to_x",
        python_callable=post_to_x,
    )