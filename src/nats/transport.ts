/**
 * RUTIC NATS 트랜스포트
 *
 * WebSocket 서버 대체: OpenClaw 에이전트를 NATS queue subscriber로 기동.
 * env 우선 → config.nats 순으로 설정 해석.
 *
 * 환경 변수:
 *   RUTIC_NATS_URL      NATS 서버 주소 (기본: nats://localhost:4222)
 *   RUTIC_AGENT_ID      에이전트 ID (필수)
 *   RUTIC_NATS_CREDS    NATS credentials 파일 경로 (선택)
 */

import type { NatsConnection } from "nats";
import type { OpenClawConfig } from "../config/config.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { createNatsClient } from "./client.js";
import { startNatsAgentSubscriber, type NatsAgentSubscription } from "./subscriber.js";
import type { NatsTransportConfig } from "./types.js";

const log = createSubsystemLogger("nats-transport");

export type NatsTransport = {
  connection: NatsConnection;
  subscription: NatsAgentSubscription;
  close: () => Promise<void>;
};

/** 환경 변수 + config.nats 기반으로 NatsTransportConfig 생성 */
export function resolveNatsTransportConfig(cfg: OpenClawConfig): NatsTransportConfig | null {
  const agentId =
    process.env["RUTIC_AGENT_ID"]?.trim() || cfg.agents?.defaultAgentId?.trim() || "";
  if (!agentId) {
    log.warn("nats-transport: RUTIC_AGENT_ID 미설정 — NATS 트랜스포트 비활성");
    return null;
  }

  const servers =
    process.env["RUTIC_NATS_URL"]?.trim() ||
    cfg.nats?.servers ||
    "nats://localhost:4222";

  const credentialsPath =
    process.env["RUTIC_NATS_CREDS"]?.trim() || cfg.nats?.credentialsPath;

  const taskSubjectPrefix = cfg.nats?.taskSubjectPrefix ?? "task";
  const replySubject = cfg.nats?.replySubject ?? "reply.send";
  const queueGroup = `rutic-agents-${agentId}`;

  return {
    servers,
    agentId,
    credentialsPath,
    taskSubjectPrefix,
    replySubject,
    queueGroup,
  };
}

/**
 * NATS 트랜스포트 시작.
 * startGatewayServer 내부 또는 독립 엔트리에서 호출.
 */
export async function startNatsTransport(cfg: OpenClawConfig): Promise<NatsTransport | null> {
  const natsCfg = resolveNatsTransportConfig(cfg);
  if (!natsCfg) return null;

  const connection = await createNatsClient(natsCfg);
  const subscription = startNatsAgentSubscriber({ conn: connection, cfg, natsCfg });

  log.info(`nats-transport: agent=${natsCfg.agentId} ready`);

  return {
    connection,
    subscription,
    close: async () => {
      await subscription.close();
      await connection.drain();
      log.info(`nats-transport: agent=${natsCfg.agentId} closed`);
    },
  };
}
