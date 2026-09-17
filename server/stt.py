"""Google Cloud STT 연동. 청크(WAV, 16kHz mono 16bit) 하나를 텍스트로 변환한다."""
from google.cloud import speech

from .config import GOOGLE_APPLICATION_CREDENTIALS

_client = (
    speech.SpeechClient.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
    if GOOGLE_APPLICATION_CREDENTIALS
    else None
)

_config = speech.RecognitionConfig(
    encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
    sample_rate_hertz=16000,
    language_code="ko-KR",
)

_WAV_HEADER_LEN = 44  # 앱·서버 양쪽 다 표준 44바이트 PCM 헤더로 통일돼 있다.


def transcribe_wav(wav_bytes: bytes) -> str | None:
    """WAV 청크를 텍스트로 변환한다. 무음·인식 실패·키 미설정 시 None."""
    if _client is None:
        return None

    audio = speech.RecognitionAudio(content=wav_bytes[_WAV_HEADER_LEN:])
    try:
        response = _client.recognize(config=_config, audio=audio)
    except Exception as e:  # noqa: BLE001
        print(f"[stt] recognize 실패: {e}")
        return None

    texts = [r.alternatives[0].transcript for r in response.results if r.alternatives]
    return " ".join(texts) if texts else None
