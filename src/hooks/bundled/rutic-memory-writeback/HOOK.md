---
name: rutic-memory-writeback
description: "Write conversation turns to Chroma (vector) + Postgres (text) on message:sent"
metadata:
  {
    "openclaw":
      {
        "emoji": "💾",
        "events": ["message:sent"],
        "requires": { "env": ["RUTIC_PG_URL", "RUTIC_CHROMA_URL", "RUTIC_AGENT_ID"] },
        "install": [{ "id": "bundled", "kind": "bundled", "label": "Bundled with RUTIC" }],
      },
  }
---

# RUTIC Memory Writeback Hook

`message:sent` 이벤트마다 대화 내용을 Chroma + Postgres에 저장한다.

- **Chroma**: `rutic_memory_{agentId}` 컬렉션에 벡터 임베딩
- **Postgres**: `agent_memory` 테이블에 텍스트 + 메타데이터

## 환경변수

- `RUTIC_PG_URL` (필수) — Postgres 연결 문자열
- `RUTIC_CHROMA_URL` (필수) — Chroma HTTP URL
- `RUTIC_AGENT_ID` (필수) — 에이전트 ID
