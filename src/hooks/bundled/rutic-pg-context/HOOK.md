---
name: rutic-pg-context
description: "Inject agent context (bootstrap files, hook configs) from Postgres"
metadata:
  {
    "openclaw":
      {
        "emoji": "🗄️",
        "events": ["agent:bootstrap"],
        "requires": { "env": ["RUTIC_PG_URL"] },
        "install": [{ "id": "bundled", "kind": "bundled", "label": "Bundled with RUTIC" }],
      },
  }
---

# RUTIC Postgres Context Hook

`agent:bootstrap` 이벤트에서 Postgres `agents` 테이블과
`agent_hook_configs` 테이블을 조회하여 에이전트 컨텍스트를 주입한다.

## 주입 항목

- **SOUL.md** → `agents.soul_md`
- **AGENTS.md** → `agents.agents_md`
- **TOOLS.md** → `agents.tools_md`
- **IDENTITY.md** → `agents.identity_md`
- **USER.md** → `agents.user_md`
- **훅 설정** → `agent_hook_configs` (enabled, config per hook)

## 스킬

스킬(SKILL.md)은 `attempt.ts`의 `pg-skills-shim`에서 별도 처리된다
(스킬 로딩이 bootstrap 훅보다 먼저 실행되기 때문).

## 환경변수

- `RUTIC_PG_URL` (필수) — Postgres 연결 문자열
