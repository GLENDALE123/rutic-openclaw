/**
 * RUTIC NATS 트랜스포트
 *
 * WebSocket 서버 대체: OpenClaw 에이전트를 NATS queue subscriber로 기동.
 * env 우선 → config.nats 순으로 설정 해석.
 *
 * 환경 변수 (우선순위 순):
 *   RUTIC_NATS_URL             NATS 서버 전체 URL (예: nats://oracle:4222)
 *   RUTIC_NATS_TAILSCALE_HOST  Tailscale 호스트명만 입력 → nats://{host}:4222 자동 조립
 *                              (MagicDNS 사용 시 머신명만 입력 가능, 예: oracle)
 *   RUTIC_AGENT_ID             에이전트 ID (필수)
 *   RUTIC_NATS_CREDS           NATS credentials 파일 경로 (선택)
 *
 * NATS URL 자동 해석 순서:
 *   1. RUTIC_NATS_URL
 *   2. RUTIC_NATS_TAILSCALE_HOST → nats://{host}:4222
 *   3. config.nats.servers
 *   4. 로컬 Tailscale IP 자동 감지 (100.64.0.0/10)
 *   5. nats://localhost:4222
 */

import type { NatsConnection } from "nats";
import type { OpenClawConfig } from "../config/config.js";
import { pickPrimaryTailnetIPv4 } from "../infra/tailnet.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { createNatsClient } from "./client.js";
import { startGatewayConfigServer, type GatewayConfigServer } from "./gateway-config-server.js";
import { startNatsAgentSubscriber, type NatsAgentSubscription } from "./subscriber.js";
import type { NatsTransportConfig } from "./types.js";

const log = createSubsystemLogger("nats-transport");

/**
 * NATS 서버 주소 해석 (우선순위 적용).
 * configServers: OpenClaw config.nats.servers 값 (없으면 undefined)
 */
export function resolveNatsServers(configServers?: string | string[]): string | string[] {
  // 1. 명시적 URL
  const explicit = process.env["RUTIC_NATS_URL"]?.trim();
  if (explicit) {
    return explicit;
  }

  // 2. Tailscale 호스트명 → URL 조립
  const tsHost = process.env["RUTIC_NATS_TAILSCALE_HOST"]?.trim();
  if (tsHost) {
    const url = `nats://${tsHost}:4222`;
    log.info(`nats: Tailscale 호스트 사용 → ${url}`);
    return url;
  }

  // 3. OpenClaw config
  if (configServers) {
    return configServers;
  }

  // 4. 로컬 Tailscale IP 자동 감지 (게이트웨이가 Tailscale 네트워크에 있을 때)
  const tailnetIp = pickPrimaryTailnetIPv4();
  if (tailnetIp) {
    const url = `nats://${tailnetIp}:4222`;
    log.info(`nats: Tailscale IP 자동 감지 → ${url}`);
    return url;
  }

  // 5. 최종 폴백
  return "nats://localhost:4222";
}

export type NatsTransport = {
  connection: NatsConnection;
  subscription: NatsAgentSubscription;
  configServer: GatewayConfigServer;
  close: () => Promise<void>;
};

/** 환경 변수 + config.nats 기반으로 NatsTransportConfig 생성 */
export function resolveNatsTransportConfig(cfg: OpenClawConfig): NatsTransportConfig | null {
  const agentId = process.env["RUTIC_AGENT_ID"]?.trim() || cfg.agents?.defaultAgentId?.trim() || "";
  if (!agentId) {
    log.warn("nats-transport: RUTIC_AGENT_ID 미설정 — NATS 트랜스포트 비활성");
    return null;
  }

  const servers = resolveNatsServers(cfg.nats?.servers);

  const credentialsPath = process.env["RUTIC_NATS_CREDS"]?.trim() || cfg.nats?.credentialsPath;

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
  if (!natsCfg) {
    return null;
  }

  const connection = await createNatsClient(natsCfg);
  const subscription = startNatsAgentSubscriber({ conn: connection, cfg, natsCfg });
  const configServer = startGatewayConfigServer(connection);

  log.info(`nats-transport: agent=${natsCfg.agentId} ready`);

  return {
    connection,
    subscription,
    configServer,
    close: async () => {
      await configServer.close();
      await subscription.close();
      await connection.drain();
      log.info(`nats-transport: agent=${natsCfg.agentId} closed`);
    },
  };
}
