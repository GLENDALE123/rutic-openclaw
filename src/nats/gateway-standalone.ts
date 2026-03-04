#!/usr/bin/env node
/**
 * RUTIC NATS Gateway Standalone
 *
 * Full OpenClaw 게이트웨이 없이 NATS config server만 독립 실행.
 * E2E 테스트 및 개발용. 프로덕션에서는 OpenClaw 전체 게이트웨이가 이 역할을 담당.
 *
 * 동작:
 *   - Oracle NATS에 연결
 *   - _rpc.config.request 구독 → ~/.openclaw/openclaw.json 내용 응답
 *   - config.updated 브로드캐스트 지원 (SIGUSR1 수신 시)
 *   - SIGINT/SIGTERM으로 graceful shutdown
 *
 * 환경 변수:
 *   RUTIC_NATS_URL             NATS 서버 전체 URL
 *   RUTIC_NATS_TAILSCALE_HOST  Tailscale 호스트명 → nats://{host}:4222 자동 조립
 *   RUTIC_NATS_CREDS           NATS credentials 파일 경로 (선택)
 */

import { connect } from "nats";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { startGatewayConfigServer } from "./gateway-config-server.js";
import { resolveNatsServers } from "./transport.js";

const log = createSubsystemLogger("gateway-standalone");

async function main(): Promise<void> {
  const servers = resolveNatsServers();
  const credentialsPath = process.env["RUTIC_NATS_CREDS"]?.trim();

  log.info(`gateway-standalone: NATS → ${Array.isArray(servers) ? servers.join(", ") : servers}`);

  const opts: Parameters<typeof connect>[0] = {
    servers,
    name: "rutic-gateway-standalone",
    reconnect: true,
    maxReconnectAttempts: -1,
  };

  if (credentialsPath) {
    const { credsAuthenticator } = await import("nats");
    const { readFile } = await import("node:fs/promises");
    const creds = await readFile(credentialsPath);
    opts.authenticator = credsAuthenticator(creds);
  }

  const conn = await connect(opts);
  log.info("gateway-standalone: NATS 연결 완료");

  const configServer = startGatewayConfigServer(conn);
  log.info("gateway-standalone: config server 시작 — _rpc.config.request 대기 중");
  log.info("gateway-standalone: 종료하려면 Ctrl+C");

  // SIGUSR1 → config.updated 브로드캐스트 (수동 트리거)
  process.on("SIGUSR1", () => {
    log.info("gateway-standalone: SIGUSR1 수신 — config.updated 브로드캐스트 예정 (미구현)");
  });

  const shutdown = async (signal: string): Promise<void> => {
    log.info(`gateway-standalone: 종료 시그널(${signal}) 수신`);
    await configServer.close();
    await conn.drain();
    log.info("gateway-standalone: 종료 완료");
  };

  process.once("SIGINT", () => void shutdown("SIGINT").then(() => process.exit(0)));
  process.once("SIGTERM", () => void shutdown("SIGTERM").then(() => process.exit(0)));

  await conn.closed();
}

main().catch((err) => {
  console.error("[gateway-standalone] 치명적 오류:", err);
  process.exit(1);
});
