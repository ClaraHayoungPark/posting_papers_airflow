# arxiv_ranking_dag.py
# 목적:
#   - papers 테이블에 저장된 최신 논문들을 가져와서
#   - 간단한 규칙 기반 점수(score)를 계산한 뒤
#   - ranked_papers 테이블에 (arxiv_id, score) 형태로 저장하는 DAG
#
# 이 DAG를 분리해두면 좋은 점:
#   - fetch(수집) / summarize(요약) / post(배포)와 "선별 로직"이 분리되어 유지보수 쉬움
#   - 나중에 점수 로직을 바꾸거나 ML 모델로 바꿔도 ranking DAG만 수정하면 됨
#   - summarizer는 ranked_papers에서 상위 N개만 가져오면 되므로 비용/처리량 통제가 쉬움

from datetime import datetime
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator

# SQLite DB 경로 (fetch DAG와 동일한 DB를 사용해야 함)
DB_PATH = "/Users/hayoung/airflow-local/papers.db"


def score_paper(title: str, abstract: str) -> int:
    """
    논문 1편에 대한 점수를 계산하는 함수(규칙 기반 스코어링).

    입력:
      - title: 논문 제목
      - abstract: 논문 초록

    출력:
      - int 점수

    점수 설계 의도(아주 단순한 MVP):
      - 특정 "핫 키워드"가 있으면 가산점 (LLM/Transformer/Diffusion/RAG 등)
      - benchmark/dataset/evaluation 같은 단어가 있으면 실험/검증 성격이라 가산점
      - abstract가 충분히 길면 정보량이 많다고 가정하여 가산점
      - abstract가 너무 짧으면 저정보/초안일 가능성이 있어 감점

    주의:
      - 이 점수는 절대적인 품질을 보장하지 않음
      - 하지만 "상위 N개를 뽑는" 용도로는 꽤 유용한 휴리스틱임
    """

    # 제목+초록을 합쳐 lower()로 소문자화해서 키워드 포함 여부를 단순 문자열 검색
    text = (title + " " + abstract).lower()
    score = 0

    # 1) 핵심 키워드 가산점:
    #    - LLM/transformer/diffusion/RAG 등은 최근 AI 핵심 트렌드라 "관심도"를 높게 가정
    if any(k in text for k in ["llm", "transformer", "diffusion", "rag"]):
        score += 3

    # 2) 실험/재현성/벤치마크 관련 키워드:
    #    - benchmark/dataset/evaluation이 있으면 실증적 내용일 가능성이 높아 가산점
    if any(k in text for k in ["benchmark", "dataset", "evaluation"]):
        score += 2

    # 3) 초록 길이 기반 가산점:
    #    - 초록이 길면 문제/방법/결과가 상대적으로 더 많이 담겨 있을 가능성이 높음
    #    - 숫자 기준은 프로젝트에 맞춰 조정 가능
    if len(abstract) > 1200:
        score += 2

    # 4) 초록이 너무 짧으면 감점:
    #    - 정보량 부족 또는 연구가 덜 정리된 경우가 있어 감점
    if len(abstract) < 400:
        score -= 2

    return score


def rank_latest_50():
    """
    papers 테이블에서 최신 논문 50개를 가져와 점수를 계산하고,
    ranked_papers 테이블에 결과를 저장하는 배치 함수.

    저장 방식:
      - INSERT OR REPLACE 사용
        -> 같은 arxiv_id에 대해 이미 점수가 있으면 덮어씀(점수 로직이 바뀌거나 재실행해도 최신값 유지)

    전제:
      - ranked_papers 테이블이 미리 생성되어 있어야 함

        예)
        CREATE TABLE IF NOT EXISTS ranked_papers (
          arxiv_id TEXT PRIMARY KEY,
          score INTEGER,
          ranked_at TEXT DEFAULT (datetime('now')),
          FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id)
        );
    """

    # 1) DB 연결
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 2) papers에서 "최신 50개" 가져오기
    #    - fetched_at은 papers 테이블 스키마에서 "수집 시각" 컬럼
    #    - 최신순 정렬 후 LIMIT 50
    #
    #    여기서 50은 운영/비용/속도 고려한 숫자이며, 필요하면 늘려도 됨
    cur.execute("""
        SELECT arxiv_id, title, abstract
        FROM papers
        ORDER BY fetched_at DESC
        LIMIT 50
    """)
    rows = cur.fetchall()

    # rows 형태 예시:
    # [
    #   ('2501.12345', 'Title A', 'Abstract A...'),
    #   ('2501.67890', 'Title B', 'Abstract B...'),
    #   ...
    # ]

    # 3) 각 논문에 점수 계산 후 ranked_papers에 저장
    for arxiv_id, title, abstract in rows:
        score = score_paper(title, abstract)

        # INSERT OR REPLACE:
        #   - arxiv_id가 PRIMARY KEY라서
        #   - 이미 존재하면 해당 행을 교체(업데이트)함
        #
        # ranked_at은 테이블 DEFAULT(datetime('now'))로 자동 채워짐
        cur.execute("""
            INSERT OR REPLACE INTO ranked_papers (arxiv_id, score)
            VALUES (?, ?)
        """, (arxiv_id, score))

    # 4) 커밋 및 종료
    conn.commit()
    conn.close()


# DAG 정의
# - 이 DAG는 하루 1번 실행되도록 @daily로 설정
# - catchup=False로 과거 날짜 소급 실행 방지
with DAG(
    dag_id="arxiv_ranking_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # rank_latest_50() 파이썬 함수를 Airflow task로 실행
    rank = PythonOperator(
        task_id="rank_latest_50",
        python_callable=rank_latest_50,
    )