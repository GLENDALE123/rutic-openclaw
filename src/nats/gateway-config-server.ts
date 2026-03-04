/**
 * RUTIC NATS Gateway Config Server
 *
 * OpenClaw 전체 게이트웨이 모드에서 실행.
 * 원격 에이전트(agent-entry.ts)의 _rpc.config.request에 응답하여
 * OpenClaw config 파일 내용을 배포한다.
 *
 * agentId가 payload에 포함된 경우 Postgres agents 테이블에서
 * per-agent model/config를 조회하여 응답에 반영한다.
 *
 * Subject:
 *   수신: _rpc.config.request (request-reply)
 *   발행: config.updated (브로드캐스트)
 */

import { StringCodec, type NatsConnection, type Subscription } from "nats";
import pg from "pg";
import { readConfigFileSnapshot } from "../config/io.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import type { NatsConfigResponse } from "./types.js";

const log = createSubsystemLogger("nats-config-server");
const sc = StringCodec();

export type GatewayConfigServer = {
  close: () => Promise<void>;
  broadcastConfigUpdate: (raw: string) => void;
};

type AgentConfigRow = {
  model_primary: string | null;
  model_fallbacks: unknown[] | null;
  enabled: boolean | null;
};

type ConfigRequestPayload = {
  agentId?: string;
};

let pool: pg.Pool | undefined;

function getPool(): pg.Pool | undefined {
  const url = process.env.RUTIC_PG_URL?.trim();
  if (!url) {
    return undefined;
  }
  if (!pool) {
    pool = new pg.Pool({ connectionString: url });
  }
  return pool;
}

async function queryAgentConfig(agentId: string): Promise<AgentConfigRow | null> {
  const p = getPool();
  if (!p) {
    return null;
  }
  const client = await p.connect();
  try {
    const res = await client.query<AgentConfigRow>(
      `SELECT model_primary, model_fallbacks, enabled
         FROM agents
        WHERE id = $1 AND enabled = true`,
      [agentId],
    );
    return res.rows[0] ?? null;
  } finally {
    client.release();
  }
}

/**
 * configYaml 문자열에서 model 설정을 per-agent 값으로 덮어씀.
 * 단순 문자열 치환 방식 — YAML 파싱 오버헤드 없이 처리.
 */
function patchModelInYaml(yaml: string, model: string): string {
  // model: "..." 라인을 교체. 없으면 끝에 추가.
  if (/^model:/m.test(yaml)) {
    return yaml.replace(/^model:.*$/m, `model: "${model}"`);
  }
  return yaml + `\nmodel: "${model}"\n`;
}

/**
 * configYaml에서 workspace/agentDir 경로를 제거한다.
 *
 * Gateway는 어느 기기의 로컬 경로를 모르므로 경로를 배포하지 않는다.
 * 각 에이전트 기기는 OPENCLAW_STATE_DIR 환경변수로 로컬 workspace를 결정한다.
 * SOUL.md/세션/메모리는 Postgres이므로 workspace는 임시 디렉토리로 충분.
 */
function stripWorkspacePathsFromYaml(yaml: string): string {
  return yaml
    .replace(/^[ \t]*workspace:[ \t]*"[^"]*"[ \t]*\n?/gm, "")
    .replace(/^[ \t]*agentDir:[ \t]*"[^"]*"[ \t]*\n?/gm, "");
}

/**
 * _rpc.config.request 구독을 시작하고 config 파일 내용으로 응답한다.
 * payload에 agentId가 있으면 Postgres에서 per-agent 설정을 조회해 반영한다.
 */
export function startGatewayConfigServer(conn: NatsConnection): GatewayConfigServer {
  const sub: Subscription = conn.subscribe("_rpc.config.request");

  log.info("nats-config-server: started (_rpc.config.request 대기 중)");

  void (async () => {
    for await (const msg of sub) {
      let res: NatsConfigResponse;
      try {
        const snapshot = await readConfigFileSnapshot();
        let configYaml: string = stripWorkspacePathsFromYaml(snapshot.raw ?? "");

        // agentId가 payload에 포함된 경우 Postgres에서 per-agent model 조회
        const rawPayload = sc.decode(msg.data);
        if (rawPayload) {
          try {
            const payload = JSON.parse(rawPayload) as ConfigRequestPayload;
            const agentId = payload.agentId;
            if (agentId) {
              const agentCfg = await queryAgentConfig(agentId).catch((err) => {
                log.warn(`Postgres 에이전트 조회 실패 (${agentId}): ${String(err)}`);
                return null;
              });
              if (agentCfg?.model_primary) {
                configYaml = patchModelInYaml(configYaml, agentCfg.model_primary);
                log.debug(
                  `nats-config-server: ${agentId} per-agent model 반영 (${agentCfg.model_primary})`,
                );
              }
            }
          } catch {
            // payload 파싱 실패 시 기본 config 반환
          }
        }

        res = { ok: true, configYaml, timestamp: Date.now() };
        log.debug("nats-config-server: config 응답 전송");
      } catch (err) {
        res = {
          ok: false,
          error: err instanceof Error ? err.message : String(err),
          timestamp: Date.now(),
        };
        log.error("nats-config-server: config 읽기 실패", { error: String(err) });
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
      const payload: NatsConfigResponse = {
        ok: true,
        configYaml: stripWorkspacePathsFromYaml(raw),
        timestamp: Date.now(),
      };
      conn.publish("config.updated", sc.encode(JSON.stringify(payload)));
      log.info("nats-config-server: config.updated 브로드캐스트");
    },
  };
}
