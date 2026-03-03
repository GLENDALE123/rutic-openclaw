/**
 * NATS Transport Config (rutic-dev 브랜치)
 *
 * RUTIC-v2 Event Registry 기반 subject 구조:
 *   수신: task.{agentId}  (queue delivery, no persistence)
 *   발신: reply.send       (queue delivery, no persistence)
 */

export type NatsConfig = {
  /** NATS 서버 주소 (e.g. "nats://localhost:4222") */
  servers: string | string[];
  /** NATS credentials 파일 경로 (선택) */
  credentialsPath?: string;
  /** 태스크 수신 subject prefix (기본: "task") */
  taskSubjectPrefix?: string;
  /** 응답 발신 subject (기본: "reply.send") */
  replySubject?: string;
  /** 연결 타임아웃 ms (기본: 5000) */
  connectTimeoutMs?: number;
};
