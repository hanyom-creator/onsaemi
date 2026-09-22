"""
온새미 녹취 파일 주입 하네스 (WBS 4.1.1)

녹음 WAV 한 개를 앱과 같은 방식으로 잘라서 서버에 보낸다.
  WAV → 16kHz mono 16bit 변환 → 2초 창을 (2초 − 중첩) 간격으로 밀며 청크 생성
  → POST /sessions/{id}/chunks → (선택) SSE 수신 → POST /sessions/{id}/end

중첩 방식 B: 청크 길이는 항상 2초, 이동 간격이 2초 − 중첩.
  중첩 0.5초 → 0~2, 1.5~3.5, 3~5, ... 1.5초마다 전송
  녹음 끝에 2초가 안 되게 남은 조각은 짧은 채로 보낸다.

실시간 설계
  · 각 청크는 창이 끝나는 시각(녹음 시작 기준 절대 시각)에 보낸다 → 누적 밀림 없음
  · 전송은 별도 스레드에서 처리 → 느린 응답·재시도가 다음 청크를 막지 않음
  · 스레드마다 연결을 유지(keep-alive) → 청크마다 TCP·TLS 연결을 새로 맺지 않음
  · 모든 청크는 전송 전에 미리 WAV로 만들어 둠 → 전송 시점 변환 지연 없음
  · send_log.csv 의 send_delay_ms 로 예정 시각 대비 실제 전송 지연을 확인

외부 패키지 없이 Python 3.12 표준 라이브러리만 쓴다.

사용 예 (D:\\onsaemi 에서):
  python tools\\inject_harness.py --make-tone test_tone.wav
  python tools\\inject_harness.py test_tone.wav --dry-run --overlap 0.5
  python tools\\inject_harness.py call.wav --server http://127.0.0.1:8000 --listen --overlap 0.5

산출물: runs\\<시각>_ov<중첩>\\
  chunks\\chunk_0000.wav ...  서버로 보낸 청크 원본
  manifest.csv               청크별 원본 구간(초) — 경계 단어 소실 분석용
  send_log.csv               예정 시각, 전송 지연, 응답 코드, 전송 소요 시간
  events.jsonl               SSE 수신 이벤트 + 클라이언트 측 지연
  run.json                   실행 조건
"""

import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

import argparse
import audioop  # Python 3.12까지 표준 포함 (3.13에서 제거됨)
import csv
import datetime as dt
import http.client
import io
import json
import math
import os
import struct
import sys
import threading
import time
import urllib.request
import uuid
import wave
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

TARGET_RATE = 16000
TARGET_WIDTH = 2  # 16bit
KST = dt.timezone(dt.timedelta(hours=9))


# ------------------------------------------------------------
# 오디오
# ------------------------------------------------------------
def load_wav(path: Path) -> tuple[bytes, dict]:
    """WAV를 읽어 16kHz mono 16bit PCM으로 변환한다."""
    try:
        with wave.open(str(path), "rb") as w:
            ch, width, rate, n = w.getnchannels(), w.getsampwidth(), w.getframerate(), w.getnframes()
            data = w.readframes(n)
    except wave.Error as e:
        sys.exit(f"[오류] WAV(PCM)로 읽을 수 없습니다: {path} ({e})\n"
                 f"       m4a 등은 먼저 변환하세요: ffmpeg -i in.m4a -ar 16000 -ac 1 -sample_fmt s16 out.wav")

    original = {"channels": ch, "sample_width": width, "rate": rate, "seconds": round(n / rate, 3)}

    if width == 1:  # 8bit WAV는 unsigned
        data = audioop.bias(data, 1, -128)
    if width != TARGET_WIDTH:
        data = audioop.lin2lin(data, width, TARGET_WIDTH)
    if ch == 2:
        data = audioop.tomono(data, TARGET_WIDTH, 0.5, 0.5)
    elif ch != 1:
        sys.exit(f"[오류] 채널 수 {ch}는 지원하지 않습니다 (1 또는 2)")
    if rate != TARGET_RATE:
        data, _ = audioop.ratecv(data, TARGET_WIDTH, 1, rate, TARGET_RATE, None)

    return data, original


def split_chunks(pcm: bytes, chunk_sec: float, overlap_sec: float) -> list[dict]:
    """
    방식 B: 길이 chunk_sec 의 창을 (chunk_sec − overlap_sec) 간격으로 민다.
    마지막 창이 녹음 끝을 넘으면 남은 만큼만 짧게 자른다.
    """
    bytes_per_sec = TARGET_RATE * TARGET_WIDTH
    win = int(chunk_sec * TARGET_RATE) * TARGET_WIDTH
    step = int((chunk_sec - overlap_sec) * TARGET_RATE) * TARGET_WIDTH

    chunks, seq, start = [], 0, 0
    while start < len(pcm):
        end = min(len(pcm), start + win)
        chunks.append({
            "seq": seq,
            "start_sec": round(start / bytes_per_sec, 3),
            "end_sec": round(end / bytes_per_sec, 3),
            "pcm": pcm[start:end],
        })
        if end == len(pcm):
            break
        seq += 1
        start += step
    return chunks


def to_wav_bytes(pcm: bytes) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(TARGET_WIDTH)
        w.setframerate(TARGET_RATE)
        w.writeframes(pcm)
    return buf.getvalue()


def make_tone(path: Path, seconds: float = 7.3):
    """분할 확인용 440Hz 톤. 변환 경로까지 확인하려고 일부러 44.1kHz 스테레오로 만든다."""
    rate = 44100
    frames = bytearray()
    for i in range(int(rate * seconds)):
        v = int(8000 * math.sin(2 * math.pi * 440 * i / rate))
        frames += struct.pack("<hh", v, v)
    with wave.open(str(path), "wb") as w:
        w.setnchannels(2)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(bytes(frames))
    print(f"[생성] {path} ({seconds}초, 44.1kHz 스테레오)")


# ------------------------------------------------------------
# 네트워크 — 스레드별 keep-alive 연결
# ------------------------------------------------------------
_STALE = (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError, ConnectionAbortedError)


class Client:
    def __init__(self, server: str, api_key: str | None, timeout: float):
        u = urlsplit(server)
        self.https = u.scheme == "https"
        self.host = u.hostname
        self.port = u.port
        self.prefix = u.path.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.local = threading.local()

    def headers(self, extra: dict | None = None) -> dict:
        h = {"ngrok-skip-browser-warning": "1", "Connection": "keep-alive"}
        if self.api_key:
            h["X-API-Key"] = self.api_key
        if extra:
            h.update(extra)
        return h

    def _conn(self):
        c = getattr(self.local, "conn", None)
        if c is None:
            cls = http.client.HTTPSConnection if self.https else http.client.HTTPConnection
            c = cls(self.host, self.port, timeout=self.timeout)
            self.local.conn = c
        return c

    def _drop(self):
        c = getattr(self.local, "conn", None)
        if c is not None:
            c.close()
        self.local.conn = None

    def send(self, method: str, path: str, body: bytes | None, headers: dict) -> tuple[int | None, str]:
        # 유지하던 연결이 서버 쪽에서 끊겨 있으면 한 번만 새로 연결해 다시 보낸다
        for attempt in range(2):
            c = self._conn()
            try:
                c.request(method, self.prefix + path, body=body, headers=headers)
                r = c.getresponse()
                return r.status, r.read().decode("utf-8", errors="replace")
            except _STALE as e:
                self._drop()
                if attempt == 1:
                    return None, f"연결 끊김: {e}"
            except (http.client.HTTPException, OSError) as e:
                self._drop()
                return None, str(e)
        return None, "알 수 없는 오류"

    def post_chunk(self, session_id, seq, recorded_at, wav_bytes):
        boundary = uuid.uuid4().hex
        body = b"".join([
            f'--{boundary}\r\nContent-Disposition: form-data; name="seq"\r\n\r\n{seq}\r\n'.encode(),
            f'--{boundary}\r\nContent-Disposition: form-data; name="recorded_at"\r\n\r\n{recorded_at}\r\n'.encode(),
            (f'--{boundary}\r\nContent-Disposition: form-data; name="audio"; filename="chunk_{seq:04d}.wav"\r\n'
             f'Content-Type: audio/wav\r\n\r\n').encode() + wav_bytes + b"\r\n",
            f"--{boundary}--\r\n".encode(),
        ])
        h = self.headers({"Content-Type": f"multipart/form-data; boundary={boundary}"})
        return self.send("POST", f"/sessions/{session_id}/chunks", body, h)

    def post_end(self, session_id, last_seq):
        body = json.dumps({"ended_at": now_iso(), "last_seq": last_seq}).encode()
        return self.send("POST", f"/sessions/{session_id}/end", body,
                         self.headers({"Content-Type": "application/json"}))


class SSEListener(threading.Thread):
    """GET /sessions/{id}/stream 을 열어두고 이벤트를 기록한다."""

    def __init__(self, server, session_id, client: Client, sent_at: dict, log_path: Path):
        super().__init__(daemon=True)
        self.url = f"{server}/sessions/{session_id}/stream"
        self.client = client
        self.sent_at = sent_at          # seq → 전송 시작 시각(perf_counter)
        self.log_path = log_path
        self.connected = threading.Event()
        self.error = None
        self.count = 0

    def run(self):
        req = urllib.request.Request(self.url, headers=self.client.headers({"Accept": "text/event-stream"}))
        try:
            with urllib.request.urlopen(req, timeout=None) as r, \
                    open(self.log_path, "a", encoding="utf-8") as log:
                self.connected.set()
                data_lines = []
                for raw in r:
                    line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                    if line.startswith(":"):
                        continue  # keep-alive 주석
                    if line.startswith("data:"):
                        data_lines.append(line[5:].lstrip())
                        continue
                    if line == "" and data_lines:
                        self.handle("\n".join(data_lines), log)
                        data_lines = []
        except Exception as e:  # noqa: BLE001 — 연결 실패 원인을 그대로 보여준다
            self.error = e
            self.connected.set()

    def handle(self, payload: str, log):
        received = time.perf_counter()
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            event = {"raw": payload}

        seq = event.get("seq")
        sent = self.sent_at.get(seq)
        client_ms = round((received - sent) * 1000) if sent is not None else None

        log.write(json.dumps({"received_at": now_iso(), "client_latency_ms": client_ms, "event": event},
                             ensure_ascii=False) + "\n")
        log.flush()
        self.count += 1

        script = event.get("script")
        lines = [f"  ◀ seq {seq} | Lv{event.get('level')} | 점수 {event.get('risk_score')} | "
                 f"서버 {event.get('latency_ms')}ms / 수신 {client_ms}ms",
                 f"      STT: {event.get('transcript')}"]
        if script:
            lines.append(f"      대본: {script}")
        print("\n".join(lines), flush=True)


# ------------------------------------------------------------
# 실행
# ------------------------------------------------------------
def now_iso() -> str:
    return dt.datetime.now(KST).isoformat(timespec="milliseconds")


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    p = argparse.ArgumentParser(description="온새미 녹취 파일 주입 하네스 (4.1.1)")
    p.add_argument("wav", nargs="?", type=Path, help="주입할 WAV 파일")
    p.add_argument("--server", default="http://127.0.0.1:8000", help="서버 주소 (ngrok 주소 가능)")
    p.add_argument("--chunk-sec", type=float, default=2.0, help="청크 길이(초), 기본 2.0")
    p.add_argument("--overlap", type=float, default=0.0, help="앞 청크와 겹칠 길이(초), 기본 0")
    p.add_argument("--session-id", help="지정하지 않으면 UUID v4 자동 생성")
    p.add_argument("--fast", action="store_true", help="실시간 간격 없이 연속 전송 (기본은 실시간)")
    p.add_argument("--listen", action="store_true", help="SSE 스트림을 열어 결과 수신")
    p.add_argument("--drain", type=float, default=15.0, help="마지막 청크 후 SSE 대기 시간(초)")
    p.add_argument("--no-end", action="store_true", help="종료 통보(/end)를 보내지 않음")
    p.add_argument("--retries", type=int, default=3, help="청크 전송 재시도 횟수 (1→2→4초)")
    p.add_argument("--workers", type=int, default=4, help="동시 전송 스레드 수")
    p.add_argument("--timeout", type=float, default=10.0, help="요청 타임아웃(초)")
    p.add_argument("--api-key", default=os.environ.get("ONSAEMI_API_KEY"), help="X-API-Key (기본: 환경변수)")
    p.add_argument("--out", type=Path, default=Path("runs"), help="결과 저장 상위 폴더")
    p.add_argument("--dry-run", action="store_true", help="전송하지 않고 분할 결과만 저장")
    p.add_argument("--make-tone", type=Path, metavar="PATH", help="테스트용 톤 WAV를 만들고 종료")
    args = p.parse_args()

    if args.make_tone:
        make_tone(args.make_tone)
        return
    if not args.wav:
        p.error("WAV 파일 경로가 필요합니다")
    if args.overlap < 0 or args.overlap >= args.chunk_sec:
        p.error("--overlap 은 0 이상, --chunk-sec 미만이어야 합니다")

    server = args.server.rstrip("/")
    session_id = args.session_id or str(uuid.uuid4())
    step_sec = args.chunk_sec - args.overlap

    pcm, original = load_wav(args.wav)
    chunks = split_chunks(pcm, args.chunk_sec, args.overlap)

    stamp = dt.datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    run_dir = args.out / f"{stamp}_ov{args.overlap:g}"
    (run_dir / "chunks").mkdir(parents=True, exist_ok=True)

    (run_dir / "run.json").write_text(json.dumps({
        "wav": str(args.wav), "original": original, "session_id": session_id,
        "server": None if args.dry_run else server, "chunk_sec": args.chunk_sec, "overlap_sec": args.overlap,
        "step_sec": round(step_sec, 3), "realtime": not args.fast, "workers": args.workers,
        "chunks": len(chunks), "started_at": now_iso(),
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    # 전송 전에 모든 청크를 WAV로 만들어 둔다
    with open(run_dir / "manifest.csv", "w", newline="", encoding="utf-8-sig") as f:
        wr = csv.writer(f)
        wr.writerow(["seq", "start_sec", "end_sec", "length_sec", "file"])
        for c in chunks:
            name = f"chunk_{c['seq']:04d}.wav"
            c["wav"] = to_wav_bytes(c["pcm"])
            (run_dir / "chunks" / name).write_bytes(c["wav"])
            wr.writerow([c["seq"], c["start_sec"], c["end_sec"], round(c["end_sec"] - c["start_sec"], 3), name])

    print(f"[입력] {args.wav}  원본 {original['rate']}Hz·{original['channels']}ch·{original['seconds']}초")
    print(f"[분할] {len(chunks)}개 청크, 길이 {args.chunk_sec}초, 중첩 {args.overlap}초, 간격 {step_sec:g}초")
    print(f"[저장] {run_dir}")

    if args.dry_run:
        print("[완료] dry-run — 전송하지 않았습니다")
        return

    print(f"[세션] {session_id}")
    print(f"[서버] {server}")

    client = Client(server, args.api_key, args.timeout)
    sent_at: dict[int, float] = {}
    listener = None
    if args.listen:
        listener = SSEListener(server, session_id, client, sent_at, run_dir / "events.jsonl")
        listener.start()
        listener.connected.wait(timeout=5)
        if listener.error:
            print(f"[경고] SSE 연결 실패: {listener.error} — 전송은 계속합니다")
        else:
            print("[SSE] 연결됨")

    log_lock = threading.Lock()
    log_file = open(run_dir / "send_log.csv", "w", newline="", encoding="utf-8-sig")
    log = csv.writer(log_file)
    log.writerow(["seq", "due_sec", "recorded_at", "sent_at", "send_delay_ms",
                  "status", "attempts", "send_ms", "response"])
    stats = {"failed": 0, "delays": [], "send_ms": []}

    def send_task(c, recorded_at, sent_iso, delay_ms):
        attempt = 0
        while True:
            t0 = time.perf_counter()
            status, text = client.post_chunk(session_id, c["seq"], recorded_at, c["wav"])
            send_ms = (time.perf_counter() - t0) * 1000
            ok = status is not None and 200 <= status < 300
            if ok or attempt >= args.retries:
                break
            wait = 2 ** attempt  # 1 → 2 → 4초
            print(f"    seq {c['seq']} 실패({status}) → {wait}초 뒤 재시도", flush=True)
            time.sleep(wait)
            attempt += 1

        mark = "▶" if ok else "✖"
        print(f"  {mark} seq {c['seq']:>3} [{c['start_sec']:>6.2f}~{c['end_sec']:>6.2f}s] → {status} "
              f"(전송 {send_ms:.0f}ms, 예정 대비 +{delay_ms:.0f}ms)" + ("" if ok else f"  {text[:120]}"),
              flush=True)
        with log_lock:
            if not ok:
                stats["failed"] += 1
            stats["send_ms"].append(send_ms)
            log.writerow([c["seq"], c["end_sec"], recorded_at, sent_iso, round(delay_ms),
                          status, attempt + 1, round(send_ms), text[:300]])
            log_file.flush()

    base_wall = dt.datetime.now(KST)
    base_perf = time.perf_counter()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for c in chunks:
            due = base_perf + c["end_sec"]  # 창이 끝나는 시각 = 앱이 청크를 보낼 수 있는 가장 빠른 시각
            if not args.fast:
                while True:
                    remain = due - time.perf_counter()
                    if remain <= 0:
                        break
                    time.sleep(remain if remain > 0.005 else 0)
            now = time.perf_counter()
            delay_ms = max(0.0, (now - due) * 1000) if not args.fast else 0.0
            stats["delays"].append(delay_ms)

            recorded_at = (base_wall + dt.timedelta(seconds=c["start_sec"])).isoformat(timespec="milliseconds")
            sent_at[c["seq"]] = now  # SSE가 202보다 먼저 올 수 있어 전송 시작 시각을 기준으로 잡는다
            pool.submit(send_task, c, recorded_at, now_iso(), delay_ms)

    log_file.close()
    last_seq = chunks[-1]["seq"]

    if listener and not listener.error:
        print(f"[SSE] 남은 결과 최대 {args.drain:g}초 대기")
        deadline = time.perf_counter() + args.drain
        while time.perf_counter() < deadline and listener.count < len(chunks):
            time.sleep(0.1)

    if not args.no_end:
        status, text = client.post_end(session_id, last_seq)
        print(f"[종료] /end → {status}  {text[:200]}")

    sm = stats["send_ms"]
    print(f"[요약] 전송 {len(chunks)}개, 실패 {stats['failed']}개"
          + (f", SSE 수신 {listener.count}개" if listener else ""))
    if sm:
        print(f"[속도] 전송 소요 평균 {sum(sm) / len(sm):.0f}ms · 최대 {max(sm):.0f}ms"
              + ("" if args.fast else f" | 예정 대비 전송 지연 최대 {max(stats['delays']):.0f}ms"))
    print(f"[저장] {run_dir}")


if __name__ == "__main__":
    main()
