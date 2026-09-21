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
    stt_confidence: Optional[float] = None,
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
                    signals, stt_confidence, score_delta, score_total, latency_ms, audio_path)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id, seq, recorded_at, now(), transcript,
                    json.dumps(signals, ensure_ascii=False) if signals else None,
                    stt_confidence, score_delta, score_total, latency_ms, audio_path,
                ),
            )
            return True
        except sqlite3.IntegrityError:
            # UNIQUE(session_id, seq) — 재전송된 중복 청크
            return False


def get_events_since(session_id: str, since_seq: Optional[int]) -> list[dict]:
    """분석이 끝난 청크 결과를 seq 오름차순으로 재구성한다 (SSE 재연결 재전송용).

    실시간 publish 페이로드와 같은 형식을 유지한다. level/level_reason/script/
    script_followed 는 3.2.1(위험도·스크립트) 구현 전까지는 실시간 쪽도 항상
    스텁 값이라, script 테이블에 매칭되는 행이 없으면 그 스텁 값을 그대로 채운다.
    """
    floor_seq = since_seq if since_seq is not None else -1
    with closing(get_conn()) as conn, conn:
        rows = conn.execute(
            """SELECT u.seq, u.transcript, u.signals, u.score_total, u.latency_ms,
                      s.level, s.level_reason, s.content AS script, s.script_followed
               FROM utterance u
               LEFT JOIN script s ON s.session_id = u.session_id AND s.seq = u.seq
               WHERE u.session_id = ? AND u.seq > ?
               ORDER BY u.seq ASC""",
            (session_id, floor_seq),
        ).fetchall()

    events = []
    for r in rows:
        events.append({
            "seq": r["seq"],
            "transcript": r["transcript"],
            "risk_score": r["score_total"],
            "level": r["level"] if r["level"] is not None else 0,
            "level_reason": r["level_reason"] or "score",
            "signals": json.loads(r["signals"]) if r["signals"] else [],
            "script": r["script"],
            "script_followed": r["script_followed"],
            "latency_ms": r["latency_ms"],
        })
    return events


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
