/**
 * RUTIC Postgres Skills Shim
 *
 * Postgres agent_skills 테이블에서 스킬을 읽어
 * /tmp/rutic-skills/{agentId}/{skillName}/SKILL.md 로 기록한다.
 * resolveEmbeddedRunSkillEntries() 호출 전에 실행하여
 * config.skills.load.extraDirs 에 임시 디렉토리를 추가한다.
 *
 * 환경변수: RUTIC_PG_URL
 */

import fs from "node:fs/promises";
import path from "node:path";
import pg from "pg";
import { createSubsystemLogger } from "../logging/subsystem.js";

const log = createSubsystemLogger("pg-skills-shim");

const TMP_SKILLS_BASE = "/tmp/rutic-skills";

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

type SkillRow = {
  skill_name: string;
  skill_md: string;
};

type HookConfigRow = {
  hook_name: string;
  enabled: boolean;
  config: Record<string, unknown>;
};

/**
 * Postgres agent_skills → /tmp/rutic-skills/{agentId}/{skillName}/SKILL.md
 * 반환값: 임시 스킬 디렉토리 경로 (스킬 없으면 null)
 */
export async function writePgSkillsToTemp(agentId: string): Promise<string | null> {
  const pool = getPool();
  if (!pool) {
    return null;
  }

  const client = await pool.connect();
  let rows: SkillRow[];
  try {
    const res = await client.query<SkillRow>(
      `SELECT skill_name, skill_md
         FROM agent_skills
        WHERE agent_id = $1 AND enabled = true`,
      [agentId],
    );
    rows = res.rows;
  } finally {
    client.release();
  }

  if (rows.length === 0) {
    return null;
  }

  const agentSkillsDir = path.join(TMP_SKILLS_BASE, agentId);

  for (const row of rows) {
    // skill_name을 안전한 디렉토리명으로 변환
    const safeDir = row.skill_name.replace(/[^a-z0-9_-]/gi, "_");
    const skillDir = path.join(agentSkillsDir, safeDir);
    await fs.mkdir(skillDir, { recursive: true });
    await fs.writeFile(path.join(skillDir, "SKILL.md"), row.skill_md, {
      encoding: "utf-8",
      mode: 0o644,
    });
  }

  log.debug(`${agentId} — Postgres에서 스킬 ${rows.length}개 임시 디렉토리에 기록`);
  return agentSkillsDir;
}

/**
 * Postgres agent_hook_configs → { hookName: { enabled, config } } 맵 반환
 * rutic-pg-context 훅에서 context.cfg 패치에 사용.
 */
export async function loadPgHookConfigs(
  agentId: string,
): Promise<Record<string, { enabled: boolean; config: Record<string, unknown> }> | null> {
  const pool = getPool();
  if (!pool) {
    return null;
  }

  const client = await pool.connect();
  try {
    const res = await client.query<HookConfigRow>(
      `SELECT hook_name, enabled, config
         FROM agent_hook_configs
        WHERE agent_id = $1`,
      [agentId],
    );
    if (res.rows.length === 0) {
      return null;
    }

    const result: Record<string, { enabled: boolean; config: Record<string, unknown> }> = {};
    for (const row of res.rows) {
      result[row.hook_name] = { enabled: row.enabled, config: row.config ?? {} };
    }
    return result;
  } finally {
    client.release();
  }
}

/**
 * 임시 스킬 디렉토리 정리 (실행 완료 후 선택적으로 호출)
 */
export async function cleanupPgSkillsTemp(agentId: string): Promise<void> {
  const dir = path.join(TMP_SKILLS_BASE, agentId);
  await fs.rm(dir, { recursive: true, force: true }).catch(() => undefined);
}
