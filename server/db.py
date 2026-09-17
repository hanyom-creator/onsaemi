"""SQLite 접속 헬퍼. schema.sql 로 만든 DB를 사용한다."""
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone, timedelta
from typing import Optional

from .config import DB_PATH

KST = timezone(timedelta(hours=9))


def now() -> str:
    """ISO 8601 (KST) 문자열."""
    return datetime.now(KST).isoformat(timespec="seconds")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ------------------------------------------------------------
# session
# ------------------------------------------------------------
def ensure_session(session_id: str) -> bool:
    """세션이 없으면 만든다. 새로 만들었으면 True."""
    with closing(get_conn()) as conn, conn:
        try:
            conn.execute(
                "INSERT INTO session (session_id, started_at) VALUES (?, ?)",
                (session_id, now()),
            )
            return True
        except sqlite3.IntegrityError:
            # 동시 요청으로 이미 만들어진 세션 — 정상 상황
            return False


def is_session_open(session_id: str) -> bool:
    with closing(get_conn()) as conn, conn:
        row = conn.execute(
            "SELECT ended_at FROM session WHERE session_id = ?", (session_id,)
        ).fetchone()
        return row is not None and row["ended_at"] is None


def close_session(session_id: str, reason: str) -> dict:
    """세션을 닫고 집계값을 채운다."""
    with closing(get_conn()) as conn, conn:
        agg = conn.execute(
            """SELECT COUNT(*) AS cnt, COALESCE(MAX(score_total), 0) AS max_score
               FROM utterance WHERE session_id = ?""",
            (session_id,),
        ).fetchone()
        max_level = conn.execute(
            "SELECT COALESCE(MAX(level), 0) AS lv FROM script WHERE session_id = ?",
            (session_id,),
        ).fetchone()["lv"]

        conn.execute(
            """UPDATE session
               SET ended_at = ?, end_reason = ?, total_chunks = ?,
                   max_risk_score = ?, max_level = ?
               WHERE session_id = ? AND ended_at IS NULL""",
            (now(), reason, agg["cnt"], agg["max_score"], max_level, session_id),
        )
        return {
            "session_id": session_id,
            "total_chunks": agg["cnt"],
            "max_risk_score": agg["max_score"],
            "max_level": max_level,
        }


# ------------------------------------------------------------
# utterance
# ------------------------------------------------------------
def save_utterance(
    session_id: str,
    seq: int,
    recorded_at: str,
    transcript: Optional[str] = None,
    signals: Optional[list] = None,
    score_delta: int = 0,
    score_total: Optional[int] = None,
    latency_ms: Optional[int] = None,
    audio_path: Optional[str] = None,
) -> bool:
    """청크 1건 저장. 이미 같은 seq 가 있으면 무시하고 False."""
    with closing(get_conn()) as conn, conn:
        try:
            conn.execute(
                """INSERT INTO utterance
                   (session_id, seq, recorded_at, received_at, transcript,
                    signals, score_delta, score_total, latency_ms, audio_path)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id, seq, recorded_at, now(), transcript,
                    json.dumps(signals, ensure_ascii=False) if signals else None,
                    score_delta, score_total, latency_ms, audio_path,
                ),
            )
            return True
        except sqlite3.IntegrityError:
            # UNIQUE(session_id, seq) — 재전송된 중복 청크
            return False


def last_score_total(session_id: str) -> int:
    with closing(get_conn()) as conn, conn:
        row = conn.execute(
            """SELECT COALESCE(MAX(score_total), 0) AS s
               FROM utterance WHERE session_id = ?""",
            (session_id,),
        ).fetchone()
        return row["s"]


def get_context(session_id: str) -> list[str]:
    """seq 오름차순 발화 목록. 도착 순서가 아니라 seq 기준으로 정렬한다."""
    with closing(get_conn()) as conn, conn:
        rows = conn.execute(
            """SELECT transcript FROM utterance
               WHERE session_id = ? AND transcript IS NOT NULL
               ORDER BY seq ASC""",
            (session_id,),
        ).fetchall()
        return [r["transcript"] for r in rows]


def last_activity_time(session_id: str) -> Optional[str]:
    """마지막 청크 도착 시각. 청크가 하나도 없으면 세션 시작 시각으로 대체한다."""
    with closing(get_conn()) as conn, conn:
        row = conn.execute(
            """SELECT COALESCE(
                   (SELECT MAX(received_at) FROM utterance WHERE session_id = ?),
                   (SELECT started_at FROM session WHERE session_id = ?)
               ) AS t""",
            (session_id, session_id),
        ).fetchone()
        return row["t"]


# ------------------------------------------------------------
# script
# ------------------------------------------------------------
def save_script(
    session_id: str, seq: int, level: int, level_reason: str, content: str
) -> int:
    with closing(get_conn()) as conn, conn:
        cur = conn.execute(
            """INSERT INTO script
               (session_id, seq, level, level_reason, content, sent_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (session_id, seq, level, level_reason, content, now()),
        )
        return cur.lastrowid
