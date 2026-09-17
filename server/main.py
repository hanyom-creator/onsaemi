"""온새미 서버 — WBS 3.1.1 골격.

엔드포인트 3개만 세운 상태. STT(3.1.2) · 마스킹(3.1.3) · LLM(3.2.1) 은 TODO.
"""
import asyncio
import time
from contextlib import closing
from datetime import datetime

from fastapi import FastAPI, Form, File, UploadFile, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse

from . import db, sse
from .config import API_KEY, AUDIO_DIR, KEEP_AUDIO, CHUNK_TIMEOUT_SEC

app = FastAPI(title="Onsaemi API", version="0.1")


# ------------------------------------------------------------
# 공통
# ------------------------------------------------------------
def check_api_key(x_api_key: str | None) -> None:
    """API_KEY 환경변수가 비어 있으면 검증을 건너뛴다 (개발 구간)."""
    if not API_KEY:
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid_api_key")


@app.get("/health")
async def health():
    return {"status": "ok", "time": db.now()}


# ------------------------------------------------------------
# 1. 청크 업로드
# ------------------------------------------------------------
@app.post("/sessions/{session_id}/chunks", status_code=202)
async def upload_chunk(
    session_id: str,
    seq: int = Form(...),
    recorded_at: str = Form(...),
    audio: UploadFile = File(...),
    x_api_key: str | None = Header(default=None),
):
    check_api_key(x_api_key)
    t0 = time.perf_counter()

    # 처음 보는 session_id 면 세션 자동 생성
    await run_in_threadpool(db.ensure_session, session_id)

    if not await run_in_threadpool(db.is_session_open, session_id):
        raise HTTPException(status_code=404, detail="session_closed")

    data = await audio.read()

    # 원본 보관 — 실험용 빌드에서만
    audio_path = None
    if KEEP_AUDIO:
        sess_dir = AUDIO_DIR / session_id
        audio_path = str(sess_dir / f"{seq:05d}.wav")

        def _write_audio() -> None:
            sess_dir.mkdir(parents=True, exist_ok=True)
            with open(audio_path, "wb") as f:
                f.write(data)

        await run_in_threadpool(_write_audio)

    # TODO 3.1.2  Google STT 연동 — 여기서 transcript 생성
    transcript = None
    # TODO 3.1.3  민감정보 탐지 → 신호 기록 → 치환
    signals: list[str] = []
    # TODO 3.2.1  위험점수 산출 + 스크립트 생성
    score_delta = 0
    score_total = await run_in_threadpool(db.last_score_total, session_id) + score_delta

    latency_ms = int((time.perf_counter() - t0) * 1000)

    saved = await run_in_threadpool(
        db.save_utterance,
        session_id=session_id,
        seq=seq,
        recorded_at=recorded_at,
        transcript=transcript,
        signals=signals or None,
        score_delta=score_delta,
        score_total=score_total,
        latency_ms=latency_ms,
        audio_path=audio_path,
    )

    # 중복(재전송) 청크는 분석 결과를 다시 내보내지 않는다
    if saved:
        await sse.publish(session_id, {
            "seq": seq,
            "transcript": transcript,
            "risk_score": score_total,
            "level": 0,
            "level_reason": "score",
            "signals": signals,
            "script": None,
            "script_followed": None,
            "latency_ms": latency_ms,
        })

    return {"seq": seq, "accepted": True, "bytes": len(data)}


# ------------------------------------------------------------
# 2. SSE 스트림
# ------------------------------------------------------------
@app.get("/sessions/{session_id}/stream")
async def stream(
    session_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None),
):
    check_api_key(x_api_key)
    queue = sse.subscribe(session_id)

    async def event_gen():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15)
                    yield sse.format_event(payload)
                except asyncio.TimeoutError:
                    # 연결 유지용 주석 프레임
                    yield ": keep-alive\n\n"
        finally:
            sse.unsubscribe(session_id, queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ------------------------------------------------------------
# 3. 세션 종료
# ------------------------------------------------------------
@app.post("/sessions/{session_id}/end")
async def end_session(
    session_id: str,
    x_api_key: str | None = Header(default=None),
):
    check_api_key(x_api_key)
    if not await run_in_threadpool(db.is_session_open, session_id):
        raise HTTPException(status_code=404, detail="session_not_found_or_closed")
    result = await run_in_threadpool(db.close_session, session_id, "app_notified")
    await sse.publish(session_id, {"event": "session_ended", **result})
    return result


# ------------------------------------------------------------
# 백그라운드 — 무청크 타임아웃 정리
# ------------------------------------------------------------
async def timeout_watcher():
    """마지막 청크 후 CHUNK_TIMEOUT_SEC 경과한 세션을 닫는다."""
    while True:
        await asyncio.sleep(10)
        try:
            with closing(db.get_conn()) as conn, conn:
                rows = conn.execute(
                    "SELECT session_id FROM session WHERE ended_at IS NULL"
                ).fetchall()
            for r in rows:
                sid = r["session_id"]
                last = db.last_activity_time(sid)
                if not last:
                    continue
                gap = (datetime.now(db.KST) - datetime.fromisoformat(last)).total_seconds()
                if gap > CHUNK_TIMEOUT_SEC:
                    db.close_session(sid, "chunk_timeout")
        except Exception as e:  # noqa: BLE001
            print(f"[timeout_watcher] {e}")


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(timeout_watcher())
