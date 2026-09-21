"""S1: STT 결과 반환(종료 판정) 확인용 테스트 도구 (WBS 3.1.2).

WAV 파일을 16kHz mono 16bit 로 맞춘 뒤 2초 청크로 잘라 서버에 업로드하고,
SSE 로 돌아오는 분석 결과를 실시간으로 출력한다. 청크 형식과 SSE 이벤트
형식은 SPEC.md 6.1·6.2 를 따른다.

사용:
    python tools/send_chunks.py samples/test_ko.wav
    python tools/send_chunks.py samples/test_ko.wav --url https://<ngrok>.ngrok-free.dev --realtime
"""
import argparse
import audioop
import io
import json
import sqlite3
import sys
import threading
import time
import uuid
import wave
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

KST = timezone(timedelta(hours=9))
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "server" / "onsaemi.db"
CHUNK_SECONDS = 2
SAMPLE_RATE = 16000
SAMPLE_WIDTH = 2  # 16bit
CHUNK_BYTES = SAMPLE_RATE * SAMPLE_WIDTH * CHUNK_SECONDS
HEADERS = {"ngrok-skip-browser-warning": "true"}


def load_pcm_16k_mono_16bit(path: Path) -> bytes:
    """입력 WAV를 16kHz mono 16bit PCM으로 맞춘다."""
    with wave.open(str(path), "rb") as w:
        channels = w.getnchannels()
        sampwidth = w.getsampwidth()
        framerate = w.getframerate()
        frames = w.readframes(w.getnframes())

    if sampwidth != SAMPLE_WIDTH:
        frames = audioop.lin2lin(frames, sampwidth, SAMPLE_WIDTH)
        sampwidth = SAMPLE_WIDTH

    if channels != 1:
        frames = audioop.tomono(frames, sampwidth, 0.5, 0.5)
        channels = 1

    if framerate != SAMPLE_RATE:
        frames, _ = audioop.ratecv(frames, sampwidth, channels, framerate, SAMPLE_RATE, None)

    return frames


def split_chunks(pcm: bytes) -> list[bytes]:
    """2초 단위로 자른다. 마지막 조각은 무음(0바이트)으로 채워 2초를 맞춘다."""
    chunks = []
    for i in range(0, len(pcm), CHUNK_BYTES):
        chunk = pcm[i : i + CHUNK_BYTES]
        if len(chunk) < CHUNK_BYTES:
            chunk = chunk + b"\x00" * (CHUNK_BYTES - len(chunk))
        chunks.append(chunk)
    return chunks or [b"\x00" * CHUNK_BYTES]


def wrap_wav(pcm_chunk: bytes) -> bytes:
    """PCM 조각에 표준 WAV 헤더를 씌운다 (stt.py 의 44바이트 헤더 전제와 맞춤)."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(SAMPLE_WIDTH)
        w.setframerate(SAMPLE_RATE)
        w.writeframes(pcm_chunk)
    return buf.getvalue()


class ReaderState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.received: dict[int, str | None] = {}
        self.stop = False


def fetch_confidences(db_path: Path, session_id: str) -> dict[int, float | None]:
    """stt_confidence 는 SSE payload 에 없어(분석용, 앱 화면에 불필요) 테스트가
    끝난 뒤 로컬 DB에서 한 번에 조회한다. DB 파일에 접근할 수 없으면(원격 서버
    등) 조용히 빈 dict를 돌려준다."""
    try:
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT seq, stt_confidence FROM utterance WHERE session_id = ? ORDER BY seq",
            (session_id,),
        ).fetchall()
        conn.close()
        return {seq: confidence for seq, confidence in rows}
    except Exception:  # noqa: BLE001
        return {}


def sse_reader(
    url: str,
    session_id: str,
    state: ReaderState,
    connected: threading.Event,
    ended: threading.Event,
) -> None:
    """SSE 스트림을 읽어 결과를 즉시 출력하고 state.received 에 기록한다."""
    try:
        resp = requests.get(
            f"{url}/sessions/{session_id}/stream",
            headers=HEADERS,
            stream=True,
            timeout=(5, None),
        )
        resp.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"[sse] 연결 실패: {e}", file=sys.stderr)
        connected.set()
        return

    connected.set()
    try:
        for raw_line in resp.iter_lines(decode_unicode=True):
            if state.stop:
                break
            if not raw_line or raw_line.startswith(":") or raw_line.startswith("id:"):
                continue
            if not raw_line.startswith("data:"):
                continue

            try:
                payload = json.loads(raw_line[len("data:") :].strip())
            except json.JSONDecodeError:
                continue

            if payload.get("event") == "session_ended":
                ended.set()
                break

            seq = payload.get("seq")
            latency_ms = payload.get("latency_ms")
            transcript = payload.get("transcript")
            print(f"seq={seq} latency_ms={latency_ms} transcript={transcript!r}")
            with state.lock:
                state.received[seq] = transcript
    finally:
        resp.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="S1: STT 종료 판정 확인용 청크 전송 도구")
    parser.add_argument("wav", type=Path, help="입력 WAV 파일 경로")
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="서버 base URL")
    parser.add_argument(
        "--realtime", action="store_true", help="청크 사이 2초 대기 (실시간 전송 흉내)"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="stt_confidence 조회용 로컬 DB 경로 (원격 서버면 조회 실패 시 조용히 생략)",
    )
    args = parser.parse_args()

    if not args.wav.exists():
        print(f"파일이 없다: {args.wav}", file=sys.stderr)
        sys.exit(1)

    pcm = load_pcm_16k_mono_16bit(args.wav)
    chunks = split_chunks(pcm)
    total = len(chunks)
    print(f"입력: {args.wav} -> {total}개 청크 (2초 단위, 16kHz mono 16bit)")

    url = args.url.rstrip("/")
    session_id = str(uuid.uuid4())
    print(f"세션: {session_id}")

    state = ReaderState()
    connected = threading.Event()
    ended = threading.Event()
    reader = threading.Thread(
        target=sse_reader,
        args=(url, session_id, state, connected, ended),
        daemon=True,
    )
    reader.start()

    if not connected.wait(timeout=5):
        print("[sse] 연결 대기 시간 초과", file=sys.stderr)

    start = datetime.now(KST)
    for seq, chunk in enumerate(chunks):
        recorded_at = (start + timedelta(seconds=CHUNK_SECONDS * seq)).isoformat(
            timespec="seconds"
        )
        files = {"audio": (f"{seq:05d}.wav", wrap_wav(chunk), "audio/wav")}
        data = {"seq": str(seq), "recorded_at": recorded_at}
        try:
            resp = requests.post(
                f"{url}/sessions/{session_id}/chunks",
                headers=HEADERS,
                data=data,
                files=files,
                timeout=10,
            )
            resp.raise_for_status()
        except Exception as e:  # noqa: BLE001
            print(f"[upload] seq={seq} 실패: {e}", file=sys.stderr)

        if args.realtime and seq < total - 1:
            time.sleep(CHUNK_SECONDS)

    print("모든 청크 업로드 완료. 결과 대기 중...")

    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        with state.lock:
            received_count = len(state.received)
        if received_count >= total:
            break
        time.sleep(0.2)

    ended_at = datetime.now(KST).isoformat(timespec="seconds")
    try:
        resp = requests.post(
            f"{url}/sessions/{session_id}/end",
            headers=HEADERS,
            json={"ended_at": ended_at, "last_seq": total - 1},
            timeout=10,
        )
        resp.raise_for_status()
        print(f"세션 종료: {resp.json()}")
    except Exception as e:  # noqa: BLE001
        print(f"[end] 실패: {e}", file=sys.stderr)

    ended.wait(timeout=5)
    state.stop = True
    reader.join(timeout=2)

    confidences = fetch_confidences(args.db, session_id)
    if confidences:
        print("--- STT 신뢰도 (DB 조회) ---")
        for seq in sorted(confidences):
            print(f"seq={seq} confidence={confidences[seq]!r}")
    else:
        print(f"[confidence] DB 조회 실패 또는 값 없음: {args.db}")

    with state.lock:
        received_count = len(state.received)
        success_count = sum(1 for t in state.received.values() if t)

    print(f"결과 {received_count}/{total}개 수신, 전사 성공 {success_count}개")


if __name__ == "__main__":
    main()
