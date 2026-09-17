"""환경 설정. 키는 코드에 박지 않고 .env 에서 읽는다."""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

# DB 파일 경로
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "onsaemi.db"))

# 청크 WAV 저장 경로 (실험용 빌드에서만 보관)
AUDIO_DIR = Path(os.getenv("AUDIO_DIR", str(BASE_DIR / "audio")))
AUDIO_DIR.mkdir(parents=True, exist_ok=True)
KEEP_AUDIO = os.getenv("KEEP_AUDIO", "1") == "1"

# 인증 — 비어 있으면 검증 생략 (ngrok 개발 구간)
API_KEY = os.getenv("API_KEY", "")

# 세션 종료 조건
CHUNK_TIMEOUT_SEC = int(os.getenv("CHUNK_TIMEOUT_SEC", "60"))
SESSION_LIMIT_SEC = int(os.getenv("SESSION_LIMIT_SEC", "300"))

# 스크립트 출력 임계값
SCRIPT_THRESHOLD = int(os.getenv("SCRIPT_THRESHOLD", "65"))

# 외부 API 키 (3.1.2 이후 사용)
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
