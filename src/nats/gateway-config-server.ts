/**
 * RUTIC NATS Gateway Config Server
 *
 * OpenClaw 전체 게이트웨이 모드에서 실행.
 * 원격 에이전트(agent-entry.ts)의 _rpc.config.request에 응답하여
 * OpenClaw config 파일 내용을 배포한다.
 *
 * Subject:
 *   수신: _rpc.config.request (request-reply)
 *   발행: config.updated (브로드캐스트)
 */

import { StringCodec, type NatsConnection, type Subscription } from "nats";
import { readConfigFileSnapshot } from "../config/io.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import type { NatsConfigResponse } from "./types.js";

const log = createSubsystemLogger("nats-config-server");
const sc = StringCodec();

export type GatewayConfigServer = {
  close: () => Promise<void>;
  broadcastConfigUpdate: (raw: string) => void;
};

/**
 * _rpc.config.request 구독을 시작하고 config 파일 내용으로 응답한다.
 * NATS 연결만 있으면 agentId 없이도 동작.
 */
export function startGatewayConfigServer(conn: NatsConnection): GatewayConfigServer {
  const sub: Subscription = conn.subscribe("_rpc.config.request");

  log.info("nats-config-server: started (_rpc.config.request 대기 중)");

  void (async () => {
    for await (const msg of sub) {
      let res: NatsConfigResponse;
      try {
        const snapshot = await readConfigFileSnapshot();
        res = { ok: true, configYaml: snapshot.raw, timestamp: Date.now() };
        log.debug("nats-config-server: config 응답 전송");
      } catch (err) {
        res = {
          ok: false,
          error: err instanceof Error ? err.message : String(err),
          timestamp: Date.now(),
        };
        log.error("nats-config-server: config 읽기 실패", err);
      }
      msg.respond(sc.encode(JSON.stringify(res)));
    }
    log.info("nats-config-server: 구독 종료");
  })();

  return {
    close: async () => {
      sub.unsubscribe();
    },
    broadcastConfigUpdate: (raw: string) => {
      const payload: NatsConfigResponse = { ok: true, configYaml: raw, timestamp: Date.now() };
      conn.publish("config.updated", sc.encode(JSON.stringify(payload)));
      log.info("nats-config-server: config.updated 브로드캐스트");
    },
  };
}
