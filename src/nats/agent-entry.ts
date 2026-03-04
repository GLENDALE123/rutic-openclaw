#!/usr/bin/env node
/**
 * RUTIC Standalone Agent Entry Point
 *
 * HTTP/WebSocket 서버 없이 순수 NATS subscriber로 동작하는 독립 에이전트.
 * OpenClaw 에이전트 런타임을 최소 의존성으로 부팅한다.
 *
 * 시작 순서:
 *   1. 환경 변수 확인 (RUTIC_AGENT_ID, RUTIC_NATS_URL)
 *   2. NATS 연결
 *   3. Config 로드:
 *      a. OPENCLAW_CONFIG env → 로컬 파일 직접 사용
 *      b. 없으면 → gateway _rpc.config.request 로 수신
 *   4. Agent 등록 + heartbeat 시작
 *   5. task.{agentId} 구독 시작
 *   6. SIGINT/SIGTERM 대기 후 graceful shutdown
 *
 * 환경 변수:
 *   RUTIC_AGENT_ID      에이전트 ID (필수)
 *   RUTIC_NATS_URL      NATS 서버 (기본: nats://localhost:4222)
 *   RUTIC_NATS_CREDS    NATS credentials 파일 경로 (선택)
 *   OPENCLAW_CONFIG     로컬 config 파일 경로 (없으면 gateway에서 수신)
 */

import {
  loadConfig,
  parseConfigJson5,
  setRuntimeConfigSnapshot,
} from "../config/config.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { startAgentRegistration } from "./agent-register.js";
import { createConfigSync } from "./config-sync.js";
import { createNatsClient } from "./client.js";
import { startNatsAgentSubscriber } from "./subscriber.js";
import type { NatsTransportConfig } from "./types.js";

const log = createSubsystemLogger("agent-entry");

async function main(): Promise<void> {
  // ── 1. 에이전트 ID 확인 ────────────────────────────────────────────────
  const agentId = process.env["RUTIC_AGENT_ID"]?.trim();
  if (!agentId) {
    log.error("agent-entry: RUTIC_AGENT_ID 환경 변수가 필요합니다.");
    process.exit(1);
  }

  log.info(`agent-entry [${agentId}]: 시작 중...`);

  // ── 2. NATS 설정 해석 (env 변수에서 직접 구성) ──────────────────────
  // OpenClaw config가 아직 없으므로 NATS 설정은 env 변수에서만 읽는다.
  const natsCfg: NatsTransportConfig = {
    servers: process.env["RUTIC_NATS_URL"]?.trim() ?? "nats://localhost:4222",
    agentId,
    credentialsPath: process.env["RUTIC_NATS_CREDS"]?.trim(),
    taskSubjectPrefix: "task",
    replySubject: "reply.send",
    queueGroup: `rutic-agents-${agentId}`,
  };

  const conn = await createNatsClient(natsCfg);
  log.info(`agent-entry [${agentId}]: NATS 연결 완료`);

  // ── 3. Config 로드 ─────────────────────────────────────────────────────
  const localConfigPath = process.env["OPENCLAW_CONFIG"]?.trim();

  if (localConfigPath) {
    // 3a. 로컬 파일 사용 — OPENCLAW_CONFIG 환경 변수를 통해 loadConfig()가 읽음
    // OpenClaw는 XDG_CONFIG_HOME 또는 HOME 기반 config 경로를 사용하므로
    // 로컬 파일이 있는 경우 loadConfig()가 자동으로 찾음
    log.info(`agent-entry [${agentId}]: 로컬 config 사용: ${localConfigPath}`);
  } else {
    // 3b. gateway에서 config 수신
    const configSync = createConfigSync({
      conn,
      natsCfg,
      onUpdated: (yaml) => {
        log.info(`agent-entry [${agentId}]: config 업데이트 적용`);
        applyConfigYaml(agentId, yaml);
      },
    });

    const configYaml = await configSync.fetchConfig();
    if (!configYaml) {
      log.error(
        `agent-entry [${agentId}]: gateway에서 config를 받지 못했습니다. ` +
          "OPENCLAW_CONFIG 또는 gateway 연결을 확인하세요.",
      );
      await conn.drain();
      process.exit(1);
    }

    applyConfigYaml(agentId, configYaml);
    configSync.subscribe(); // config.updated 구독 (핫리로드)
  }

  // ── 4. Agent 등록 + heartbeat ──────────────────────────────────────────
  const registration = startAgentRegistration(conn, natsCfg);

  // ── 5. task.{agentId} 구독 ─────────────────────────────────────────────
  // loadConfig()는 setRuntimeConfigSnapshot으로 주입된 config를 반환함
  const cfg = loadConfig();

  const subscription = startNatsAgentSubscriber({ conn, cfg, natsCfg });
  log.info(`agent-entry [${agentId}]: 준비 완료 — task.${agentId} 대기 중`);

  // ── 6. 종료 처리 ───────────────────────────────────────────────────────
  const shutdown = async (signal: string): Promise<void> => {
    log.info(`agent-entry [${agentId}]: 종료 시그널(${signal}) 수신`);
    registration.close();
    await subscription.close();
    await conn.drain();
    log.info(`agent-entry [${agentId}]: 종료 완료`);
  };

  process.once("SIGINT", () => void shutdown("SIGINT").then(() => process.exit(0)));
  process.once("SIGTERM", () => void shutdown("SIGTERM").then(() => process.exit(0)));

  // 프로세스가 종료되지 않도록 NATS 연결이 닫힐 때까지 대기
  await conn.closed();
}

function applyConfigYaml(agentId: string, yaml: string): void {
  const result = parseConfigJson5(yaml);
  if (!result.ok || !result.parsed) {
    log.error(`agent-entry [${agentId}]: config 파싱 실패 — ${result.error}`);
    return;
  }
  setRuntimeConfigSnapshot(result.parsed as Parameters<typeof setRuntimeConfigSnapshot>[0]);
  log.info(`agent-entry [${agentId}]: config 적용 완료`);
}

main().catch((err) => {
  console.error("[agent-entry] 치명적 오류:", err);
  process.exit(1);
});
