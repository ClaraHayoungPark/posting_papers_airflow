from datetime import datetime
import logging
import os
import sqlite3

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from requests_oauthlib import OAuth1

load_dotenv()
logger = logging.getLogger(__name__)

DB_PATH = "/Users/hayoung/airflow-local/arxiv_pipeline.db"
POST_SCORE = 5
X_POST_URL = "https://api.twitter.com/2/tweets"

CONSUMER_KEY = os.environ.get("CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")


def build_tweet_text(summary: str, arxiv_id: str) -> str:
    return f"{summary}\nhttps://arxiv.org/abs/{arxiv_id}"


def post_tweet(text: str):
    return requests.post(
        url=X_POST_URL,
        auth=OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET),
        json={"text": text},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )


def post_to_x():
    # X 인증키 누락 시 즉시 실패
    missing = [
        name
        for name, value in {
            "CONSUMER_KEY": CONSUMER_KEY,
            "CONSUMER_SECRET": CONSUMER_SECRET,
            "ACCESS_TOKEN": ACCESS_TOKEN,
            "ACCESS_TOKEN_SECRET": ACCESS_TOKEN_SECRET,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing X credentials: {', '.join(missing)}")

    with sqlite3.connect(DB_PATH, timeout=60) as conn:
        cur = conn.cursor()

        # score=5 이고 아직 미포스팅인 항목만 조회
        cur.execute(
            """
            SELECT p.arxiv_id, s.summary_500
            FROM rankings r
            JOIN summaries s ON s.arxiv_id = r.arxiv_id
            JOIN papers p ON p.arxiv_id = r.arxiv_id
            LEFT JOIN posted t ON t.arxiv_id = r.arxiv_id
            WHERE r.score = ?
              AND t.arxiv_id IS NULL
            ORDER BY r.ranked_at DESC
            """,
            (POST_SCORE,),
        )
        rows = cur.fetchall()
        logger.info("[post] candidates(score=%d)=%d", POST_SCORE, len(rows))

        posted_count = 0
        for idx, (arxiv_id, summary) in enumerate(rows, start=1):
            tweet_text = build_tweet_text(summary, arxiv_id)
            response = post_tweet(tweet_text)

            # 성공한 건만 posted 테이블에 기록
            if response.status_code == 201:
                cur.execute(
                    "INSERT OR IGNORE INTO posted (arxiv_id) VALUES (?)", (arxiv_id,)
                )
                posted_count += 1
                logger.info("[post] %d/%d success arxiv_id=%s", idx, len(rows), arxiv_id)
            else:
                logger.warning(
                    "[post] %d/%d fail arxiv_id=%s status=%s body=%s",
                    idx,
                    len(rows),
                    arxiv_id,
                    response.status_code,
                    response.text,
                )

        conn.commit()
        logger.info("[post] done posted=%d/%d", posted_count, len(rows))


with DAG(
    dag_id="arxiv_post_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    post = PythonOperator(task_id="post_to_x", python_callable=post_to_x)
