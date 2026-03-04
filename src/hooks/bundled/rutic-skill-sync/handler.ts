/**
 * RUTIC Skill Sync Hook
 *
 * gateway:startup 이벤트 시 파일시스템 SKILL.md 변경을 감지하여
 * Postgres agent_skills 테이블에 자동 동기화한다.
 *
 * - clawhub install 등으로 SKILL.md 추가/수정 → agent_skills UPSERT
 * - SKILL.md 삭제 → agent_skills.enabled = false
 *
 * 환경변수: RUTIC_PG_URL
 */

import fs from "node:fs/promises";
import path from "node:path";
import pg from "pg";
import { registerSkillsChangeListener } from "../../../agents/skills/refresh.js";
import { createSubsystemLogger } from "../../../logging/subsystem.js";
import type { HookHandler } from "../../hooks.js";
import { isGatewayStartupEvent } from "../../internal-hooks.js";

const log = createSubsystemLogger("rutic-skill-sync");

let _pool: pg.Pool | undefined;

function getPool(): pg.Pool | undefined {
  const url = process.env.RUTIC_PG_URL?.trim();
  if (!url) {
    return undefined;
  }
  if (!_pool) {
    _pool = new pg.Pool({ connectionString: url });
  }
  return _pool;
}

/**
 * workspaceDir에서 agentId 추출:
 *   {stateDir}/workspace-{agentId} → agentId
 * 실패 시 RUTIC_AGENT_ID 환경변수 사용
 */
function extractAgentId(workspaceDir?: string): string | null {
  if (workspaceDir) {
    const base = path.basename(workspaceDir);
    if (base.startsWith("workspace-")) {
      const id = base.slice("workspace-".length).trim();
      if (id) {
        return id;
      }
    }
  }
  return process.env.RUTIC_AGENT_ID?.trim() || null;
}

/**
 * changedPath에서 skillName 추출:
 *   .../skills/{skillName}/SKILL.md → skillName
 */
function extractSkillName(changedPath: string): string | null {
  const normalized = changedPath.replace(/\\/g, "/");
  const parts = normalized.split("/");
  const idx = parts.lastIndexOf("SKILL.md");
  if (idx < 1) {
    return null;
  }
  return parts[idx - 1] ?? null;
}

async function upsertSkill(agentId: string, skillName: string, skillMd: string): Promise<void> {
  const pool = getPool();
  if (!pool) {
    return;
  }
  const client = await pool.connect();
  try {
    await client.query(
      `INSERT INTO agent_skills (agent_id, skill_name, skill_md, enabled)
            VALUES ($1, $2, $3, true)
       ON CONFLICT (agent_id, skill_name)
       DO UPDATE SET skill_md = EXCLUDED.skill_md, enabled = true`,
      [agentId, skillName, skillMd],
    );
    log.info(`${agentId}/${skillName} — Postgres 스킬 upsert 완료`);
  } finally {
    client.release();
  }
}

async function disableSkill(agentId: string, skillName: string): Promise<void> {
  const pool = getPool();
  if (!pool) {
    return;
  }
  const client = await pool.connect();
  try {
    await client.query(
      `UPDATE agent_skills SET enabled = false
        WHERE agent_id = $1 AND skill_name = $2`,
      [agentId, skillName],
    );
    log.info(`${agentId}/${skillName} — 스킬 비활성화 (파일 삭제)`);
  } finally {
    client.release();
  }
}

const ruticSkillSyncHook: HookHandler = async (event) => {
  if (!isGatewayStartupEvent(event)) {
    return;
  }
  if (!process.env.RUTIC_PG_URL) {
    return;
  }

  registerSkillsChangeListener(async ({ workspaceDir, changedPath }) => {
    if (!changedPath) {
      return;
    }

    const normalized = changedPath.replace(/\\/g, "/");
    if (!normalized.endsWith("/SKILL.md")) {
      return;
    }

    const agentId = extractAgentId(workspaceDir);
    if (!agentId) {
      log.warn(`스킬 변경 감지 — agentId 추출 실패: ${changedPath}`);
      return;
    }

    const skillName = extractSkillName(changedPath);
    if (!skillName) {
      log.warn(`스킬 이름 추출 실패: ${changedPath}`);
      return;
    }

    // 파일 존재 여부로 add/change vs unlink 구분
    let fileExists = false;
    try {
      await fs.access(changedPath);
      fileExists = true;
    } catch {
      fileExists = false;
    }

    try {
      if (fileExists) {
        const skillMd = await fs.readFile(changedPath, "utf-8");
        await upsertSkill(agentId, skillName, skillMd);
      } else {
        await disableSkill(agentId, skillName);
      }
    } catch (err) {
      log.error(`스킬 DB 동기화 실패 (${agentId}/${skillName}): ${String(err)}`);
    }
  });

  log.info("RUTIC 스킬 변경 감지 리스너 등록 완료 (Postgres 자동 동기화 활성화)");
};

export default ruticSkillSyncHook;
