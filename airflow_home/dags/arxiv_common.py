import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = str(Path(__file__).resolve().parents[2] / "arxiv_pipeline.db")


def get_db_path() -> str:
    return os.getenv("ARXIV_DB_PATH", DEFAULT_DB_PATH)


def get_env_int(name: str, default: int, minimum: int | None = None) -> int:
    value = int(os.getenv(name, str(default)))
    if minimum is not None:
        value = max(minimum, value)
    return value


def get_env_float(name: str, default: float, minimum: float | None = None) -> float:
    value = float(os.getenv(name, str(default)))
    if minimum is not None:
        value = max(minimum, value)
    return value


def ensure_pipeline_support_tables() -> None:
    db_path = get_db_path()
    with sqlite3.connect(db_path, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS papers (
                arxiv_id   TEXT PRIMARY KEY,
                title      TEXT,
                authors    TEXT,
                url        TEXT,
                abstract   TEXT,
                fetched_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS summaries (
                arxiv_id    TEXT PRIMARY KEY,
                summary_500 TEXT NOT NULL,
                model       TEXT,
                created_at  TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rankings (
                arxiv_id  TEXT PRIMARY KEY,
                score     INTEGER NOT NULL CHECK(score BETWEEN 1 AND 5),
                ranked_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS posted (
                arxiv_id  TEXT PRIMARY KEY,
                posted_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (arxiv_id) REFERENCES papers(arxiv_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS processing_failures (
                stage          TEXT NOT NULL,
                arxiv_id       TEXT NOT NULL,
                failure_count  INTEGER NOT NULL DEFAULT 0,
                last_error     TEXT,
                last_failed_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (stage, arxiv_id)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_papers_fetched_at ON papers(fetched_at DESC)"
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_processing_failures_stage_count
            ON processing_failures(stage, failure_count)
            """
        )

        legacy_ranking_exists = cur.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = 'paper_rankings'
            """
        ).fetchone()
        if legacy_ranking_exists:
            cur.execute(
                """
                INSERT OR IGNORE INTO rankings (arxiv_id, score, ranked_at)
                SELECT arxiv_id, score, ranked_at
                FROM paper_rankings
                """
            )

        cur.execute("DROP VIEW IF EXISTS v_pipeline_status")
        cur.execute(
            """
            CREATE VIEW v_pipeline_status AS
            SELECT
              p.arxiv_id,
              p.title,
              p.fetched_at,
              CASE WHEN s.arxiv_id IS NULL THEN 0 ELSE 1 END AS summarized,
              s.created_at AS summarized_at,
              CASE WHEN r.arxiv_id IS NULL THEN 0 ELSE 1 END AS ranked,
              r.score,
              r.ranked_at
            FROM papers p
            LEFT JOIN summaries s ON s.arxiv_id = p.arxiv_id
            LEFT JOIN rankings r ON r.arxiv_id = p.arxiv_id
            """
        )
