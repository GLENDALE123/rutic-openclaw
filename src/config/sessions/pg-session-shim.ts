/**
 * RUTIC Postgres Session Shim
 *
 * SessionManager.open(filePath)는 컴파일 패키지로 수정 불가.
 * 대신 실행 전/후에 Postgres ↔ /tmp 임시 파일 동기화로 영속성 구현.
 *
 * 흐름:
 *   실행 전: Postgres session_entries → JSONL 직렬화 → /tmp 기록
 *   실행 후: /tmp JSONL → Postgres session_entries UPSERT → 파일 삭제
 *
 * 환경변수: RUTIC_PG_URL
 */

import fs from "node:fs/promises";
import path from "node:path";
import pg from "pg";
import { createSubsystemLogger } from "../../logging/subsystem.js";

const log = createSubsystemLogger("pg-session-shim");

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

export function isPgSessionEnabled(): boolean {
  return Boolean(process.env.RUTIC_PG_URL?.trim());
}

/**
 * JSONL 문자열 → JSON 객체 배열
 */
function parseJsonl(raw: string): unknown[] {
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter((item): item is unknown => item !== null);
}

/**
 * JSON 객체 배열 → JSONL 문자열
 */
function serializeJsonl(entries: unknown[]): string {
  return entries.map((e) => JSON.stringify(e)).join("\n") + "\n";
}

/**
 * Postgres agent_sessions + session_entries에서 세션 데이터를 불러와
 * 임시 JSONL 파일에 기록한다.
 * 세션이 없으면 파일을 생성하지 않는다 (SessionManager가 새 세션 생성).
 */
export async function restoreSessionFromPostgres(params: {
  agentId: string;
  sessionKey: string;
  sessionId: string;
  sessionFile: string;
}): Promise<void> {
  const pool = getPool();
  if (!pool) {
    return;
  }

  const client = await pool.connect();
  try {
    const res = await client.query<{ entries: unknown[] }>(
      `SELECT se.entries
         FROM agent_sessions AS s
         JOIN session_entries AS se ON se.session_id = s.id
        WHERE s.agent_id = $1 AND s.session_key = $2`,
      [params.agentId, params.sessionKey],
    );
    const row = res.rows[0];
    if (!row) {
      log.debug(`세션 없음 (신규): ${params.agentId}/${params.sessionKey}`);
      return;
    }

    const jsonlContent = serializeJsonl(row.entries);
    await fs.mkdir(path.dirname(params.sessionFile), { recursive: true });
    await fs.writeFile(params.sessionFile, jsonlContent, { encoding: "utf-8", mode: 0o600 });
    log.debug(`세션 복원 완료: ${params.agentId}/${params.sessionKey} → ${params.sessionFile}`);
  } finally {
    client.release();
  }
}

/**
 * 임시 JSONL 파일을 읽어 Postgres session_entries에 UPSERT한다.
 * 완료 후 임시 파일을 삭제한다.
 */
export async function syncSessionToPostgres(params: {
  agentId: string;
  sessionKey: string;
  sessionId: string;
  sessionFile: string;
}): Promise<void> {
  const pool = getPool();
  if (!pool) {
    return;
  }

  let raw: string;
  try {
    raw = await fs.readFile(params.sessionFile, "utf-8");
  } catch {
    log.debug(`임시 세션 파일 없음, 스킵: ${params.sessionFile}`);
    return;
  }

  const entries = parseJsonl(raw);

  const client = await pool.connect();
  try {
    // agent_sessions upsert
    const sessionRes = await client.query<{ id: string }>(
      `INSERT INTO agent_sessions (agent_id, session_key, session_id)
            VALUES ($1, $2, $3)
       ON CONFLICT (agent_id, session_key)
       DO UPDATE SET session_id = $3, updated_at = now()
       RETURNING id`,
      [params.agentId, params.sessionKey, params.sessionId],
    );
    const sessionUuid = sessionRes.rows[0]?.id;
    if (!sessionUuid) {
      throw new Error("agent_sessions upsert 실패");
    }

    // session_entries upsert
    await client.query(
      `INSERT INTO session_entries (session_id, entries)
            VALUES ($1, $2::jsonb)
       ON CONFLICT (session_id)
       DO UPDATE SET entries = $2::jsonb, updated_at = now()`,
      [sessionUuid, JSON.stringify(entries)],
    );
    log.debug(
      `세션 동기화 완료: ${params.agentId}/${params.sessionKey} (entries=${entries.length})`,
    );
  } finally {
    client.release();
  }

  // 임시 파일 삭제
  try {
    await fs.unlink(params.sessionFile);
  } catch {
    // ignore
  }
}

/**
 * Postgres agent_sessions를 읽어 sessions.json 형식으로 반환한다.
 * store.ts의 loadSessionStore를 Postgres로 초기화할 때 사용.
 */
export async function loadSessionStoreFromPostgres(
  agentId: string,
): Promise<Record<string, { sessionId: string; sessionKey: string }> | null> {
  const pool = getPool();
  if (!pool) {
    return null;
  }

  const client = await pool.connect();
  try {
    const res = await client.query<{ session_key: string; session_id: string }>(
      `SELECT session_key, session_id FROM agent_sessions WHERE agent_id = $1`,
      [agentId],
    );
    const store: Record<string, { sessionId: string; sessionKey: string }> = {};
    for (const row of res.rows) {
      store[row.session_key] = { sessionId: row.session_id, sessionKey: row.session_key };
    }
    return store;
  } finally {
    client.release();
  }
}
