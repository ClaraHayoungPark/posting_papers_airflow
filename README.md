# AI Paper Signal  
**선별형 arXiv 논문 큐레이션 자동화 서비스**

Service: https://x.com/PaperAi27274  
Period: February 2026 ~


## Overview
arXiv에 게시되는 AI 관련 논문을 자동으로 수집하고,  
규칙 기반 필터링과 LLM 기반 요약·평가를 통해 선별된 논문만 X(Twitter)에 게시하는 자동화 프로젝트입니다.


## Goal
사람의 수동 검토 없이도 일정한 품질 기준을 유지하며  
읽을 가치가 높은 논문만 선별할 수 있는 **End-to-End 자동화 파이프라인**을 구축하는 것을 목표로 합니다.


## Architecture
Apache Airflow Orchestrator 기반으로 4개의 DAG를 순차적으로 실행합니다.

```
arxiv_orchestrator
    ├── 1. arxiv_fetch_pipeline      # arXiv 논문 수집 및 필터링
    ├── 2. arxiv_summarize_pipeline  # LLM 기반 요약 생성
    ├── 3. arxiv_ranking_pipeline    # LLM 기반 논문 점수 평가
    └── 4. arxiv_post_pipeline       # X(Twitter) 포스팅
```


## Pipeline Details

### 1. Fetch (`1_arxiv_fetch_dag.py`)
- arXiv API에서 `cs.AI`, `cs.CL`, `cs.LG` 카테고리 논문 수집
- 필터링 기준:
  - 초록 길이 400자 미만 제외
  - `survey`, `benchmark`, `dataset` 키워드 포함 논문 제외
- 수집 결과를 SQLite DB(`arxiv_pipeline.db`)의 `papers` 테이블에 저장

### 2. Summarize (`2_arxiv_summarize_dag.py`)
- `papers` 테이블에서 미요약 논문을 배치(기본 5건)로 가져옴
- GPT 모델(`gpt-5-mini`)로 논문 제목·초록 기반 요약 생성
- 결과를 `summaries` 테이블에 저장

### 3. Rank (`3_arxiv_ranking_dag.py`)
- `summaries` 테이블에서 미평가 논문을 배치(기본 5건)로 가져옴
- GPT 모델(`gpt-5-mini`)로 논문 품질 점수(1~5) 평가
- 결과를 `rankings` 테이블에 저장

### 4. Post (`4_arxiv_post_dag.py`)
- 점수 **4점 이상** 논문을 X(Twitter) API로 게시
- OAuth1 인증 방식 사용


## Project Structure
```
airflow-local/
├── airflow_home/
│   ├── dags/
│   │   ├── 0_orchestrator_dag.py
│   │   ├── 1_arxiv_fetch_dag.py
│   │   ├── 2_arxiv_summarize_dag.py
│   │   ├── 3_arxiv_ranking_dag.py
│   │   └── 4_arxiv_post_dag.py
│   ├── airflow.cfg
│   └── airflow.db
├── arxiv_pipeline.db
└── requirements.txt
```


## Tech Stack
- **Orchestration**: Apache Airflow 2.10.5
- **Database**: SQLite
- **LLM**: OpenAI API (gpt-5-mini)
- **Posting**: X(Twitter) API v2 + OAuth1
- **Data Source**: arXiv API (Atom feed)


## Environment Variables
| 변수명 | 설명 | 기본값 |
|--------|------|--------|
| `ARXIV_DB_PATH` | SQLite DB 경로 | 프로젝트 루트 `arxiv_pipeline.db` |
| `ARXIV_SUMMARY_BATCH_SIZE` | 요약 배치 크기 | 5 |
| `ARXIV_RANKING_BATCH_SIZE` | 평가 배치 크기 | 5 |
| `OPENAI_API_KEY` | OpenAI API 키 | - |
| X API 인증 키 4종 | Twitter OAuth1 인증 | - |
