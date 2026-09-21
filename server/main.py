"""온새미 서버.

STT(3.1.2) 연동 완료. 마스킹(3.1.3) · 위험도·스크립트(LLM, 3.2.1) 는 아직 TODO.
"""
import asyncio
import time
import uuid
from contextlib import asynccontextmanager, closing
from datetime import datetime

from fastapi import BackgroundTasks, FastAPI, Form, File, UploadFile, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse

from . import db, sse, stt
from .config import API_KEY, AUDIO_DIR, KEEP_AUDIO, CHUNK_TIMEOUT_SEC, SESSION_LIMIT_SEC


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(timeout_watcher())
    yield
    task.cancel()


app = FastAPI(title="Onsaemi API", version="0.1", lifespan=lifespan)


# ------------------------------------------------------------
# 공통
# ------------------------------------------------------------
def check_api_key(x_api_key: str | None) -> None:
    """API_KEY 환경변수가 비어 있으면 검증을 건너뛴다 (개발 구간)."""
    if not API_KEY:
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid_api_key")


def validate_session_id(session_id: str) -> None:
    """UUID 형식만 허용한다. session_id를 검증 없이 폴더명(AUDIO_DIR/session_id)으로
    쓰면 "../../"가 섞인 값으로 임의 경로에 파일을 쓸 수 있다 (SPEC.md 10번 [1])."""
    try:
        uuid.UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid_session_id")


@app.get("/health")
async def health():
    return {"status": "ok", "time": db.now()}


# ------------------------------------------------------------
# 1. 청크 업로드
# ------------------------------------------------------------
@app.post("/sessions/{session_id}/chunks", status_code=202)
async def upload_chunk(
    session_id: str,
    background_tasks: BackgroundTasks,
    seq: int = Form(...),
    recorded_at: str = Form(...),
    audio: UploadFile = File(...),
    x_api_key: str | None = Header(default=None),
):
    check_api_key(x_api_key)
    validate_session_id(session_id)
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

    # STT · 분석은 응답 뒤로 미룬다 ([5] — 안 그러면 202가 STT 끝날 때까지 지연되고
    # 그 지연이 3초 청크 주기를 무너뜨려서 SSE를 쓰는 이유 자체가 무효화된다).
    background_tasks.add_task(
        analyze_and_publish, session_id, seq, recorded_at, data, audio_path, t0
    )

    return {"seq": seq, "accepted": True, "bytes": len(data)}


async def analyze_and_publish(
    session_id: str,
    seq: int,
    recorded_at: str,
    data: bytes,
    audio_path: str | None,
    t0: float,
) -> None:
    """업로드 응답 뒤에 비동기로 실행 — STT → (TODO) 마스킹/위험도 → 저장 → SSE.

    202 를 이미 보낸 뒤라 여기서 예외가 나도 앱에는 실패가 전달되지 않는다.
    최소한 로그는 남겨서 조용히 청크가 사라지는 일이 없게 한다.
    """
    try:
        transcript, stt_confidence = await run_in_threadpool(stt.transcribe_wav, data)

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
            stt_confidence=stt_confidence,
            score_delta=score_delta,
            score_total=score_total,
            latency_ms=latency_ms,
            audio_path=audio_path,
        )
    except Exception as e:  # noqa: BLE001
        print(f"[analyze_and_publish] session={session_id} seq={seq} 실패: {e}")
        return

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


# ------------------------------------------------------------
# 2. SSE 스트림
# ------------------------------------------------------------
@app.get("/sessions/{session_id}/stream")
async def stream(
    session_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None),
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
):
    check_api_key(x_api_key)
    validate_session_id(session_id)

    # 먼저 구독해야 재전송 조회 중에 들어오는 실시간 이벤트를 놓치지 않는다 ([4]).
    queue = sse.subscribe(session_id)

    try:
        since_seq = int(last_event_id) if last_event_id is not None else None
    except ValueError:
        since_seq = None  # 형식이 이상하면 처음부터 재전송

    # Last-Event-ID 가 있으면 그 이후, 없으면 세션 전체를 DB에서 재구성해 먼저 보낸다.
    replay = await run_in_threadpool(db.get_events_since, session_id, since_seq)
    # "마지막 seq 이하는 건너뛴다" 는 임계값 방식은 쓰지 않는다. 분석 완료 순서가
    # seq 순서와 다를 수 있어서(늦게 시작한 청크가 먼저 끝날 수 있다), 재전송
    # 시점에 아직 안 끝난 낮은 seq 가 나중에 실시간으로 들어오면 유실된다.
    # 그래서 재전송에서 실제로 보낸 seq 집합만 걸러낸다.
    replayed_seqs = {payload["seq"] for payload in replay}

    async def event_gen():
        try:
            for payload in replay:
                yield sse.format_event(payload)

            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15)
                    seq = payload.get("seq")
                    # 재전송에서 이미 보낸 seq가 실시간으로 다시 오면 건너뛴다.
                    if seq is not None and seq in replayed_seqs:
                        continue
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
    validate_session_id(session_id)
    if not await run_in_threadpool(db.is_session_open, session_id):
        raise HTTPException(status_code=404, detail="session_not_found_or_closed")
    result = await run_in_threadpool(db.close_session, session_id, "app_notified")
    await sse.publish(session_id, {"event": "session_ended", **result})
    return result


# ------------------------------------------------------------
# 백그라운드 — 무청크 타임아웃 정리
# ------------------------------------------------------------
async def timeout_watcher():
    """마지막 청크 후 CHUNK_TIMEOUT_SEC 경과, 또는 세션 시작 후 SESSION_LIMIT_SEC
    경과한 세션을 닫는다 (후자는 API 비용 방어 — SPEC.md 5.1.3, 10번 [3])."""
    while True:
        await asyncio.sleep(10)
        try:
            with closing(db.get_conn()) as conn, conn:
                rows = conn.execute(
                    "SELECT session_id, started_at FROM session WHERE ended_at IS NULL"
                ).fetchall()
            now = datetime.now(db.KST)
            for r in rows:
                sid = r["session_id"]

                started = datetime.fromisoformat(r["started_at"])
                if (now - started).total_seconds() > SESSION_LIMIT_SEC:
                    result = db.close_session(sid, "session_limit")
                    await sse.publish(sid, {"event": "session_ended", **result})
                    continue

                last = db.last_activity_time(sid)
                if not last:
                    continue
                gap = (now - datetime.fromisoformat(last)).total_seconds()
                if gap > CHUNK_TIMEOUT_SEC:
                    result = db.close_session(sid, "chunk_timeout")
                    await sse.publish(sid, {"event": "session_ended", **result})
        except Exception as e:  # noqa: BLE001
            print(f"[timeout_watcher] {e}")
