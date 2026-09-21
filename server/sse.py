"""세션별 SSE 큐 관리.

업로드 응답은 즉시 202 로 끝내고, 분석 결과는 여기 큐를 통해 앱으로 밀어낸다.
"""
import asyncio
import json
from typing import Dict

# session_id -> 구독자 큐 목록
_subscribers: Dict[str, list[asyncio.Queue]] = {}


def subscribe(session_id: str) -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    _subscribers.setdefault(session_id, []).append(q)
    return q


def unsubscribe(session_id: str, q: asyncio.Queue) -> None:
    queues = _subscribers.get(session_id)
    if not queues:
        return
    if q in queues:
        queues.remove(q)
    if not queues:
        _subscribers.pop(session_id, None)


async def publish(session_id: str, payload: dict) -> None:
    """해당 세션의 모든 구독자에게 전달."""
    for q in _subscribers.get(session_id, []):
        await q.put(payload)


def format_event(payload: dict) -> str:
    """SSE 프레임 형식으로 직렬화.

    seq 가 있는 페이로드는 id 필드에 seq 를 실어, 앱이 재연결할 때
    Last-Event-ID 헤더로 보내 재전송 시작점을 알려줄 수 있게 한다.
    """
    lines = []
    if "seq" in payload:
        lines.append(f"id: {payload['seq']}")
    lines.append(f"data: {json.dumps(payload, ensure_ascii=False)}")
    return "\n".join(lines) + "\n\n"
