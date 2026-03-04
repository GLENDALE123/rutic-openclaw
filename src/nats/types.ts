/**
 * RUTIC-v2 Event Registry 기반 NATS 페이로드 타입
 *
 * Subject 구조 (registry: task.> = queue, reply.> = queue):
 *   수신: task.{agentId}
 *   발신: reply.send
 */

/** NATS 트랜스포트 런타임 설정 */
export type NatsTransportConfig = {
  servers: string | string[];
  agentId: string;
  credentialsPath?: string;
  taskSubjectPrefix: string;   // default: "task"
  replySubject: string;        // default: "reply.send"
  queueGroup: string;          // default: "rutic-agents-{agentId}"
};

/** task.{agentId} 수신 페이로드 (RUTIC-v2 → OpenClaw 에이전트) */
export type NatsTaskPayload = {
  code: "EVT_TASK_CREATED";
  taskId: string;
  agentId: string;
  sessionKey?: string;
  body: string;
  from?: string;
  timestamp: number;
  correlationId?: string;
};

/** reply.send 발신 페이로드 (OpenClaw 에이전트 → RUTIC-v2) */
export type NatsReplyPayload = {
  code: "EVT_REPLY_SEND";
  taskId: string;
  agentId: string;
  sessionKey?: string;
  text: string;
  isFinal: boolean;
  isError?: boolean;
  timestamp: number;
  correlationId?: string;
};

/** _rpc.config.request 요청 페이로드 (에이전트 → gateway) */
export type NatsConfigRequest = {
  agentId: string;
  timestamp: number;
};

/** _rpc.config.request 응답 페이로드 (gateway → 에이전트) */
export type NatsConfigResponse = {
  ok: boolean;
  configYaml?: string;  // OpenClaw config YAML 원문
  error?: string;
  timestamp: number;
};

/** _rpc.agent.register 페이로드 (에이전트 → gateway) */
export type NatsAgentRegister = {
  agentId: string;
  version?: string;
  capabilities?: string[];
  timestamp: number;
};

/** system.heartbeat 페이로드 */
export type NatsHeartbeat = {
  agentId: string;
  timestamp: number;
};
