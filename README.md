# 온새미 서버 — WBS 3.1.1 골격

## 내일 할 일 순서

### 1. 설치 (10분)

```bash
cd D:\onsaemi
# 기존 venv 활성화
venv\Scripts\activate

cd "onsaemi clude"
pip install -r requirements.txt
copy .env.example .env
```

### 2. DB 생성 (5분)

```bash
sqlite3 onsaemi.db < schema.sql
```

sqlite3 명령이 없으면:

```bash
python -c "import sqlite3; sqlite3.connect('onsaemi.db').executescript(open('schema.sql',encoding='utf-8').read())"
```

확인:

```bash
python -c "import sqlite3; print([r[0] for r in sqlite3.connect('onsaemi.db').execute(\"SELECT name FROM sqlite_master WHERE type='table'\")])"
```

`session, experiment, utterance, script, feedback` 이 나오면 성공.

### 3. 서버 실행 (10분)

`__init__.py` 가 이 폴더 자체를 패키지로 만들기 때문에, 부모 디렉터리(`D:\onsaemi`)에서
폴더명을 그대로 패키지 경로로 넘겨 실행한다 (폴더명에 공백이 있으므로 따옴표 필수):

```bash
cd D:\onsaemi
uvicorn "onsaemi clude.main:app" --reload --host 0.0.0.0 --port 8000
```

브라우저에서 확인:
- http://localhost:8000/health → `{"status":"ok", ...}`
- http://localhost:8000/docs → 엔드포인트 3개가 보이는 자동 문서

### 4. 로컬에서 업로드 테스트 (20분)

아무 WAV 파일이나 하나 준비해서:

```bash
curl -X POST "http://localhost:8000/sessions/test-001/chunks" ^
  -F "seq=0" ^
  -F "recorded_at=2026-09-16T10:00:00+09:00" ^
  -F "audio=@test.wav"
```

응답에 `{"seq":0,"accepted":true,"bytes":...}` 가 오고,
`audio/test-001/00000.wav` 파일이 생겼으면 성공.

DB 확인:

```bash
python -c "import sqlite3; print(list(sqlite3.connect('onsaemi.db').execute('SELECT * FROM utterance')))"
```

### 5. SSE 확인 (10분)

터미널 두 개를 띄운다.

터미널 A — 스트림 구독:
```bash
curl -N "http://localhost:8000/sessions/test-002/stream"
```

터미널 B — 청크 업로드:
```bash
curl -X POST "http://localhost:8000/sessions/test-002/chunks" -F "seq=0" -F "recorded_at=2026-09-16T10:00:00+09:00" -F "audio=@test.wav"
```

터미널 A에 `data: {...}` 가 즉시 찍히면 성공. 업로드와 결과 수신이 분리돼 동작한다는 확인이다.

### 6. ngrok 터널 (30분)

```bash
ngrok http 8000
```

나온 주소(`https://xxxx.ngrok-free.app`)를 폰 브라우저에서 열어 `/health` 가 뜨는지 확인.
여기까지 되면 3.1.1 완료.

### 7. 여유 있으면 — 보조폰에서 업로드

녹음 앱에서 이 주소로 POST 를 붙여 WAV 가 서버에 떨어지는지 확인.
안 되면 다음 작업으로 넘겨도 된다.

---

## 파일 구조

```
onsaemi clude/            (이 폴더 자체가 패키지 — __init__.py 로 표시)
├── __init__.py
├── main.py      엔드포인트 3개 + 타임아웃 워처
├── db.py        SQLite 접속·저장 헬퍼
├── sse.py       세션별 SSE 큐
├── config.py    .env 읽기
├── schema.sql
├── requirements.txt
└── .env.example
```

## 지금 비어 있는 것 (TODO)

`main.py` 의 `upload_chunk` 안에 주석으로 표시돼 있다.

| 표시 | 작업 | WBS |
|---|---|---|
| TODO 3.1.2 | Google STT 연동 → `transcript` 생성 | 이번 주 |
| TODO 3.1.3 | 민감정보 탐지 → 신호 기록 → 치환 | S2 |
| TODO 3.2.1 | 위험점수 산출 + 스크립트 생성 | S2 |

지금은 청크를 받아 저장하고 빈 결과를 SSE로 내보내는 것까지만 동작한다.
경로가 전부 뚫려 있으므로 STT를 붙이면 바로 값이 흐른다.

## 이미 반영된 명세

- 세션 자동 생성 (앱이 만든 UUID, 별도 생성 엔드포인트 없음)
- multipart 업로드, 202 즉시 응답
- `UNIQUE(session_id, seq)` 로 재전송 청크 중복 차단
- API 키 검증 — `API_KEY` 가 비어 있으면 생략
- 세션 종료 3경로 중 `app_notified` / `chunk_timeout` 구현
- 실험용 빌드 음성 보관 (`KEEP_AUDIO=1`)
