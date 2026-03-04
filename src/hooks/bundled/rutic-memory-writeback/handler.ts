/**
 * RUTIC Memory Writeback Hook
 *
 * message:sent 이벤트 시 대화 내용을 Chroma + Postgres에 저장한다.
 * PgChromaMemoryManager를 통해 벡터 임베딩 + 텍스트 인덱싱을 처리한다.
 *
 * 환경변수: RUTIC_PG_URL, RUTIC_CHROMA_URL
 */

import crypto from "node:crypto";
import { createSubsystemLogger } from "../../../logging/subsystem.js";
import { PgChromaMemoryManager } from "../../../memory/pg-chroma-manager.js";
import type { HookHandler } from "../../hooks.js";
import { isMessageSentEvent } from "../../internal-hooks.js";

const log = createSubsystemLogger("rutic-memory-writeback");

/** agentId → PgChromaMemoryManager 싱글턴 캐시 */
const managerCache = new Map<string, PgChromaMemoryManager>();

function getManager(agentId: string): PgChromaMemoryManager {
  let mgr = managerCache.get(agentId);
  if (!mgr) {
    mgr = new PgChromaMemoryManager(agentId);
    managerCache.set(agentId, mgr);
  }
  return mgr;
}

/**
 * 결정론적 UUID v5 생성 (agentId + conversationId + content hash)
 * 동일 메시지가 중복 저장되지 않도록 한다.
 */
function deriveMemoryId(agentId: string, conversationId: string, content: string): string {
  const hash = crypto
    .createHash("sha256")
    .update(`${agentId}:${conversationId}:${content}`)
    .digest("hex");
  // UUID v4 형식으로 포맷 (랜덤이지만 결정론적)
  return [
    hash.slice(0, 8),
    hash.slice(8, 12),
    "4" + hash.slice(13, 16),
    ((parseInt(hash[16], 16) & 0x3) | 0x8).toString(16) + hash.slice(17, 20),
    hash.slice(20, 32),
  ].join("-");
}

const ruticMemoryWritebackHook: HookHandler = async (event) => {
  if (!isMessageSentEvent(event)) {
    return;
  }

  // 환경변수 체크
  if (!process.env.RUTIC_PG_URL || !process.env.RUTIC_CHROMA_URL) {
    return;
  }

  const context = event.context;
  const content = context.content?.trim();
  if (!content || !context.success) {
    return;
  }

  // agentId는 환경변수에서 읽기 (RUTIC_AGENT_ID)
  const agentId = process.env.RUTIC_AGENT_ID?.trim();
  if (!agentId) {
    log.debug("RUTIC_AGENT_ID 미설정 — 메모리 writeback 스킵");
    return;
  }

  const conversationId = context.conversationId ?? context.to ?? "unknown";
  const memoryId = deriveMemoryId(agentId, conversationId, content);

  try {
    const mgr = getManager(agentId);
    await mgr.upsert({
      id: memoryId,
      content,
      sessionKey: conversationId,
      source: "session",
      metadata: {
        channelId: context.channelId,
        to: context.to,
        conversationId,
        timestamp: Date.now(),
      },
    });
    log.debug(`메모리 저장: agentId=${agentId} conversationId=${conversationId}`);
  } catch (err) {
    log.warn(`메모리 writeback 실패: ${String(err)}`);
  }
};

export default ruticMemoryWritebackHook;
