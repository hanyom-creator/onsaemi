-- ============================================================
-- 온새미(Onsaemi) SQLite 스키마 v0.2
-- 작성일: 2026-09-21
-- WBS 1.2.3 — API 명세 · SQLite 스키마 · 아키텍처 설계도
--
-- 실행:  sqlite3 onsaemi.db < schema.sql
-- ============================================================

PRAGMA foreign_keys = ON;

-- ------------------------------------------------------------
-- 1. session — 통화 단위
-- ------------------------------------------------------------
-- session_id 는 앱이 통화 감지 시 생성한 UUID v4 문자열.
-- 서버는 처음 보는 session_id 가 오면 이 행을 자동 생성한다.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS session (
    session_id      TEXT PRIMARY KEY,
    started_at      TEXT NOT NULL,              -- ISO 8601
    ended_at        TEXT,                       -- 종료 전에는 NULL
    end_reason      TEXT,                       -- app_notified | chunk_timeout | session_limit
    total_chunks    INTEGER DEFAULT 0,
    max_risk_score  INTEGER DEFAULT 0,          -- 세션 종료 시 집계
    max_level       INTEGER DEFAULT 0           -- 세션 종료 시 집계
);

-- ------------------------------------------------------------
-- 2. experiment — 실험 조건 (session 과 1:1)
-- ------------------------------------------------------------
-- 운영 로직과 무관. 실험 세션에만 행이 생긴다.
-- 항목이 늘어도 session 테이블은 건드리지 않는다.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS experiment (
    session_id      TEXT PRIMARY KEY,
    input_mode      TEXT NOT NULL,              -- recording | live_call
    response_mode   TEXT NOT NULL,              -- ctrl_lv1 | exp_full
    scenario_id     TEXT NOT NULL,              -- 시나리오 식별자 (6.1.1에서 확정)
    trial_no        INTEGER NOT NULL,           -- 반복 회차
    notes           TEXT,                       -- 자유 기술 (상세 기록은 수기 기록지)
    FOREIGN KEY (session_id) REFERENCES session(session_id) ON DELETE CASCADE
);

-- ------------------------------------------------------------
-- 3. utterance — 청크 단위 STT 결과
-- ------------------------------------------------------------
-- 문맥 구성의 기준 테이블.
-- 분석 시 session_id 로 묶어 seq 오름차순으로 읽어 합친다.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS utterance (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    seq             INTEGER NOT NULL,           -- 청크 순번, 0부터
    recorded_at     TEXT NOT NULL,              -- 앱의 녹음 시작 시각
    received_at     TEXT NOT NULL,              -- 서버 도착 시각
    transcript      TEXT,                       -- 마스킹 처리 후. STT 실패/무음이면 NULL
    signals         TEXT,                       -- JSON 배열 문자열  예: ["기관사칭","이체요구"]
    score_delta     INTEGER DEFAULT 0,          -- 이 청크에서 오른 점수 (6.3.2 지표)
    score_total     INTEGER,                    -- 이 시점까지 누적 총점 (5장 그래프 y축)
    latency_ms      INTEGER,                    -- 도착 → 결과 생성 (6.3.3 지표)
    audio_path      TEXT,                       -- 실험용 빌드만. 최종 빌드는 NULL
    UNIQUE (session_id, seq),                   -- 재전송 청크 중복 방지
    FOREIGN KEY (session_id) REFERENCES session(session_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_utterance_session_seq
    ON utterance (session_id, seq);

-- ------------------------------------------------------------
-- 4. script — 생성된 대응 스크립트
-- ------------------------------------------------------------
-- script_followed 는 생성 직후에는 알 수 없으므로 NULL 로 들어가고,
-- 이후 청크에서 판정되면 갱신된다.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS script (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    seq             INTEGER NOT NULL,           -- 근거가 된 청크 순번
    level           INTEGER NOT NULL,           -- 1~5
    level_reason    TEXT NOT NULL,              -- score | timeout | sensitive_pattern | critical_repeat
    content         TEXT NOT NULL,
    sent_at         TEXT NOT NULL,
    script_followed TEXT,                       -- yes | no | alternative | dismissive
    followed_at_seq INTEGER,                    -- 이행 판정이 일어난 청크 순번
    FOREIGN KEY (session_id) REFERENCES session(session_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_script_session
    ON script (session_id, seq);

-- ------------------------------------------------------------
-- 5. feedback — 사용자 조작 기록
-- ------------------------------------------------------------
-- reaction 값은 2.3.2 인터랙션 구현 시 확정.
-- script_id 가 NULL 이면 스크립트와 무관한 조작.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS feedback (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    script_id       INTEGER,
    reaction        TEXT NOT NULL,              -- report | dismiss | end_call | ...
    created_at      TEXT NOT NULL,
    FOREIGN KEY (session_id) REFERENCES session(session_id) ON DELETE CASCADE,
    FOREIGN KEY (script_id)  REFERENCES script(id)
);

-- ============================================================
-- 집계 예시 (논문 5장용)
-- ============================================================
-- 조건별 최고 위험점수 평균:
--   SELECT e.input_mode, e.response_mode, AVG(s.max_risk_score)
--   FROM session s JOIN experiment e ON s.session_id = e.session_id
--   GROUP BY e.input_mode, e.response_mode;
--
-- 스크립트 이행 여부별 건수:
--   SELECT script_followed, COUNT(*) FROM script GROUP BY script_followed;
--
-- 종단 지연 분포:
--   SELECT AVG(latency_ms), MAX(latency_ms) FROM utterance;
-- ============================================================
