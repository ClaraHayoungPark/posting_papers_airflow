# AI Paper Signal  
**선별형 arXiv 논문 큐레이션 자동화 서비스**

Service: https://x.com/PaperAi27274  
Period: February 2026  


## Overview
arXiv에 게시되는 AI 관련 논문을 자동으로 수집하고,  
규칙 기반 필터링과 LLM 기반 요약·평가를 통해 선별된 논문만 X(Twitter)에 게시하는 자동화 프로젝트입니다.


## Goal
사람의 수동 검토 없이도 일정한 품질 기준을 유지하며  
읽을 가치가 높은 논문만 선별할 수 있는 **End-to-End 자동화 파이프라인**을 구축하는 것을 목표로 합니다.


## Architecture
Airflow Orchestrator 기반으로 다음 단계의 DAG를 순차적으로 실행하도록 설계했습니다.
