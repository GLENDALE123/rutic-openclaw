/**
 * RUTIC NATS Config Sync
 *
 * Gateway(_rpc.config.request)로부터 OpenClaw config를 가져온다.
 * config.updated 구독으로 핫리로드 지원.
 *
 * 환경 변수:
 *   OPENCLAW_CONFIG   로컬 config 파일 경로 (있으면 NATS 조회 생략)
 */

import { StringCodec, type NatsConnection, type Subscription } from "nats";
import { createSubsystemLogger } from "../logging/subsystem.js";
import type { NatsConfigRequest, NatsConfigResponse, NatsTransportConfig } from "./types.js";

const log = createSubsystemLogger("nats-config-sync");
const sc = StringCodec();

const CONFIG_REQUEST_SUBJECT = "_rpc.config.request";
const CONFIG_UPDATED_SUBJECT = "config.updated";
const REQUEST_TIMEOUT_MS = 10_000;

export type ConfigSyncOptions = {
  conn: NatsConnection;
  natsCfg: NatsTransportConfig;
  /** config 업데이트 시 호출되는 콜백 */
  onUpdated?: (configYaml: string) => void;
};

export type ConfigSync = {
  /** Gateway에서 config YAML 수신. 실패 시 null 반환 */
  fetchConfig: () => Promise<string | null>;
  /** config.updated 구독 시작 */
  subscribe: () => Subscription;
  close: () => void;
};

/** NATS를 통해 Gateway로부터 OpenClaw config를 동기화하는 객체를 생성한다 */
export function createConfigSync(opts: ConfigSyncOptions): ConfigSync {
  const { conn, natsCfg, onUpdated } = opts;
  const { agentId } = natsCfg;
  let updatedSub: Subscription | null = null;

  const fetchConfig = async (): Promise<string | null> => {
    const req: NatsConfigRequest = { agentId, timestamp: Date.now() };
    log.info(`config-sync [${agentId}]: requesting config from gateway...`);

    try {
      const reply = await conn.request(
        CONFIG_REQUEST_SUBJECT,
        sc.encode(JSON.stringify(req)),
        { timeout: REQUEST_TIMEOUT_MS },
      );

      const res: NatsConfigResponse = JSON.parse(sc.decode(reply.data));
      if (!res.ok || !res.configYaml) {
        log.warn(`config-sync [${agentId}]: gateway error — ${res.error ?? "no config"}`);
        return null;
      }

      log.info(`config-sync [${agentId}]: config received (${res.configYaml.length} bytes)`);
      return res.configYaml;
    } catch (err) {
      log.warn(`config-sync [${agentId}]: request timeout or failed — ${String(err)}`);
      return null;
    }
  };

  const subscribe = (): Subscription => {
    updatedSub = conn.subscribe(CONFIG_UPDATED_SUBJECT);
    log.info(`config-sync [${agentId}]: subscribed to ${CONFIG_UPDATED_SUBJECT}`);

    void (async () => {
      for await (const msg of updatedSub!) {
        if (!onUpdated) continue;
        try {
          const res: NatsConfigResponse = JSON.parse(sc.decode(msg.data));
          if (res.ok && res.configYaml) {
            log.info(`config-sync [${agentId}]: config.updated received`);
            onUpdated(res.configYaml);
          }
        } catch (err) {
          log.warn(`config-sync [${agentId}]: config.updated parse error — ${String(err)}`);
        }
      }
    })();

    return updatedSub;
  };

  const close = (): void => {
    updatedSub?.unsubscribe();
  };

  return { fetchConfig, subscribe, close };
}
