/**
 * RUTIC Postgres Context Hook
 *
 * agent:bootstrap 이벤트에서 Postgres를 조회하여 아래 항목을 주입한다:
 *   1. Bootstrap 파일 (SOUL.md, AGENTS.md, TOOLS.md, IDENTITY.md, USER.md)
 *      → agents 테이블에서 가상 파일로 push
 *   2. 훅 설정 오버라이드 (enabled/config per hook)
 *      → agent_hook_configs 테이블 → context.cfg.hooks.internal.entries 패치
 *
 * 스킬(SKILL.md)은 pg-skills-shim.ts + attempt.ts에서 별도 처리
 * (스킬 로딩이 bootstrap 훅보다 먼저 실행되므로)
 *
 * 환경변수: RUTIC_PG_URL
 */

import pg from "pg";
import { loadPgHookConfigs } from "../../../agents/pg-skills-shim.js";
import type { WorkspaceBootstrapFile } from "../../../agents/workspace.js";
import { createSubsystemLogger } from "../../../logging/subsystem.js";
import type { HookHandler } from "../../hooks.js";
import { isAgentBootstrapEvent } from "../../internal-hooks.js";

const log = createSubsystemLogger("rutic-pg-context");

type AgentRow = {
  soul_md: string | null;
  agents_md: string | null;
  tools_md: string | null;
  identity_md: string | null;
  user_md: string | null;
};

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

async function queryAgentRow(agentId: string): Promise<AgentRow | null> {
  const pool = getPool();
  if (!pool) {
    return null;
  }
  const client = await pool.connect();
  try {
    const res = await client.query<AgentRow>(
      `SELECT soul_md, agents_md, tools_md, identity_md, user_md
         FROM agents
        WHERE id = $1 AND enabled = true`,
      [agentId],
    );
    return res.rows[0] ?? null;
  } finally {
    client.release();
  }
}

function makeVirtualFile(
  name: WorkspaceBootstrapFile["name"],
  content: string,
): WorkspaceBootstrapFile {
  return { name, path: `pg:agents/${name}`, content, missing: false };
}

const ruticPgContextHook: HookHandler = async (event) => {
  if (!isAgentBootstrapEvent(event)) {
    return;
  }

  const context = event.context;
  // RUTIC_AGENT_ID 우선 — OpenClaw config의 agentId("main" 등)와 분리
  const agentId = process.env["RUTIC_AGENT_ID"]?.trim() || context.agentId?.trim();
  if (!agentId || !process.env.RUTIC_PG_URL) {
    return;
  }

  // ─── 1. Bootstrap 파일 주입 ─────────────────────────────────────────────
  let row: AgentRow | null = null;
  try {
    row = await queryAgentRow(agentId);
  } catch (err) {
    log.warn(`Postgres agents 조회 실패 (${agentId}): ${String(err)}`);
  }

  if (row) {
    const pgNames = new Set(["SOUL.md", "AGENTS.md", "TOOLS.md", "IDENTITY.md", "USER.md"]);
    const filtered = context.bootstrapFiles.filter((f) => !pgNames.has(f.name));
    const injected: WorkspaceBootstrapFile[] = [];
    if (row.soul_md) {
      injected.push(makeVirtualFile("SOUL.md", row.soul_md));
    }
    if (row.agents_md) {
      injected.push(makeVirtualFile("AGENTS.md", row.agents_md));
    }
    if (row.tools_md) {
      injected.push(makeVirtualFile("TOOLS.md", row.tools_md));
    }
    if (row.identity_md) {
      injected.push(makeVirtualFile("IDENTITY.md", row.identity_md));
    }
    if (row.user_md) {
      injected.push(makeVirtualFile("USER.md", row.user_md));
    }
    context.bootstrapFiles = [...injected, ...filtered];
    log.info(`${agentId} — bootstrap 파일 ${injected.length}개 주입 (Postgres)`);
  }

  // ─── 2. 훅 설정 오버라이드 ──────────────────────────────────────────────
  let hookConfigs: Awaited<ReturnType<typeof loadPgHookConfigs>> = null;
  try {
    hookConfigs = await loadPgHookConfigs(agentId);
  } catch (err) {
    log.warn(`Postgres hook_configs 조회 실패 (${agentId}): ${String(err)}`);
  }

  if (hookConfigs && context.cfg) {
    const cfg = context.cfg;
    const existingEntries =
      (cfg.hooks?.internal?.entries as Record<string, unknown> | undefined) ?? {};

    const mergedEntries: Record<string, unknown> = { ...existingEntries };
    for (const [hookName, { enabled, config }] of Object.entries(hookConfigs)) {
      mergedEntries[hookName] = {
        ...(typeof existingEntries[hookName] === "object" && existingEntries[hookName] !== null
          ? (existingEntries[hookName] as Record<string, unknown>)
          : {}),
        ...config,
        enabled,
      };
    }

    // context.cfg는 readonly가 아니므로 직접 패치
    if (!cfg.hooks) {
      cfg.hooks = {};
    }
    if (!cfg.hooks.internal) {
      cfg.hooks.internal = { enabled: true };
    }
    (cfg.hooks.internal as Record<string, unknown>).entries = mergedEntries;

    log.info(`${agentId} — 훅 설정 ${Object.keys(hookConfigs).length}개 오버라이드 (Postgres)`);
  }
};

export default ruticPgContextHook;
