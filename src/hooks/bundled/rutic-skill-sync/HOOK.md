---
name: rutic-skill-sync
description: "Sync filesystem SKILL.md changes (clawhub install 등) to Postgres agent_skills on gateway:startup"
metadata:
  {
    "openclaw":
      {
        "emoji": "🔄",
        "events": ["gateway:startup"],
        "requires": { "env": ["RUTIC_PG_URL"] },
        "install": [{ "id": "bundled", "kind": "bundled", "label": "Bundled with RUTIC" }],
      },
  }
---

# RUTIC Skill Sync Hook

`gateway:startup` 시 파일시스템 SKILL.md 변경을 감지하여 Postgres `agent_skills` 테이블에 자동 동기화한다.

- `clawhub install` 등으로 SKILL.md 추가/수정 → `agent_skills` UPSERT
- SKILL.md 삭제 → `agent_skills.enabled = false`

## 동작

1. `gateway:startup` 이벤트 시 `registerSkillsChangeListener` 등록
2. chokidar watcher가 SKILL.md 변경 감지 → `SkillsChangeEvent` 발행
3. `changedPath`에서 `skillName` 추출, `workspaceDir`에서 `agentId` 추출
4. 파일 존재 여부에 따라 upsert / disable

## 환경변수

- `RUTIC_PG_URL` (필수) — Postgres 연결 문자열
