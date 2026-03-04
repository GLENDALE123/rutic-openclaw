/**
 * RUTIC NATS Agent Registration
 *
 * _rpc.agent.register로 gateway에 에이전트를 등록하고
 * 주기적으로 system.heartbeat를 전송한다.
 */

import { StringCodec, type NatsConnection } from "nats";
import { createSubsystemLogger } from "../logging/subsystem.js";
import type { NatsAgentRegister, NatsHeartbeat, NatsTransportConfig } from "./types.js";

const log = createSubsystemLogger("nats-agent-register");
const sc = StringCodec();

const REGISTER_SUBJECT = "_rpc.agent.register";
const HEARTBEAT_SUBJECT = "system.heartbeat";
const HEARTBEAT_INTERVAL_MS = 30_000;

const AGENT_VERSION = "1.0.0";

export type AgentRegistration = {
  /** 등록 해제 + heartbeat 정지 */
  close: () => void;
};

/**
 * Gateway에 에이전트를 등록하고 heartbeat를 주기적으로 전송한다.
 * 네트워크 오류는 warn으로만 기록하고 에이전트 실행을 중단하지 않는다.
 */
export function startAgentRegistration(
  conn: NatsConnection,
  natsCfg: NatsTransportConfig,
): AgentRegistration {
  const { agentId } = natsCfg;

  // 초기 등록
  const doRegister = (): void => {
    const payload: NatsAgentRegister = {
      agentId,
      version: AGENT_VERSION,
      capabilities: ["text", "tools"],
      timestamp: Date.now(),
    };
    try {
      conn.publish(REGISTER_SUBJECT, sc.encode(JSON.stringify(payload)));
      log.info(`agent-register [${agentId}]: registered`);
    } catch (err) {
      log.warn(`agent-register [${agentId}]: register failed — ${String(err)}`);
    }
  };

  doRegister();

  // 주기적 heartbeat
  const timer = setInterval(() => {
    const hb: NatsHeartbeat = { agentId, timestamp: Date.now() };
    try {
      conn.publish(HEARTBEAT_SUBJECT, sc.encode(JSON.stringify(hb)));
      log.debug(`agent-register [${agentId}]: heartbeat`);
    } catch (err) {
      log.warn(`agent-register [${agentId}]: heartbeat failed — ${String(err)}`);
    }
  }, HEARTBEAT_INTERVAL_MS);

  return {
    close: () => {
      clearInterval(timer);
      log.info(`agent-register [${agentId}]: stopped`);
    },
  };
}
