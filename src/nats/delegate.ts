#!/usr/bin/env node
/**
 * RUTIC Agent Task Delegate CLI
 *
 * 다른 에이전트에게 태스크를 위임하고 결과를 stdout으로 출력.
 * 에이전트의 bash tool에서 호출하는 용도.
 *
 * 사용법:
 *   node --import tsx src/nats/delegate.ts <agentId> "<message>"
 *   node --import tsx src/nats/delegate.ts <agentId> "<message>" --session <key>
 *   node --import tsx src/nats/delegate.ts <agentId> "<message>" --timeout 180000
 *
 * 환경 변수:
 *   RUTIC_NATS_URL  — NATS 주소 (기본: nats://localhost:4222)
 *   RUTIC_AGENT_ID  — 호출자 에이전트 ID
 */

import { connect, StringCodec } from "nats";
import { resolveNatsServers } from "./transport.js";

const sc = StringCodec();

async function main(): Promise<void> {
  const args = process.argv.slice(2);

  const agentId = args[0];
  const message = args[1];

  if (!agentId || !message) {
    process.stderr.write(
      "사용법: delegate <agentId> <message> [--session <key>] [--timeout <ms>]\n",
    );
    process.exit(1);
  }

  let sessionKey: string | undefined;
  let timeoutMs = 120_000;

  for (let i = 2; i < args.length; i++) {
    if (args[i] === "--session" && args[i + 1]) {
      sessionKey = args[++i];
    } else if (args[i] === "--timeout" && args[i + 1]) {
      timeoutMs = parseInt(args[++i] ?? "120000", 10);
    }
  }

  const callerId = process.env["RUTIC_AGENT_ID"] ?? "unknown";
  const taskId = `delegate-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
  const resolvedSession = sessionKey ?? `agent:${callerId}:delegate:${taskId}`;

  const servers = resolveNatsServers();
  const nc = await connect({ servers, name: `delegate-${callerId}` });

  // reply.send 구독을 발행보다 먼저 등록 (레이스 컨디션 방지)
  const sub = nc.subscribe("reply.send");

  nc.publish(
    `task.${agentId}`,
    sc.encode(
      JSON.stringify({
        code: "EVT_TASK_CREATED",
        taskId,
        agentId,
        sessionKey: resolvedSession,
        from: callerId,
        body: message,
        timestamp: Date.now(),
        channelType: "agent",
      }),
    ),
  );

  process.stderr.write(
    `[delegate] ${callerId} → ${agentId} | taskId=${taskId} timeout=${timeoutMs}ms\n`,
  );

  const result = await new Promise<string | null>((resolve) => {
    const timer = setTimeout(() => {
      sub.unsubscribe();
      resolve(null);
    }, timeoutMs);

    (async () => {
      for await (const m of sub) {
        try {
          const data = JSON.parse(sc.decode(m.data)) as {
            taskId?: string;
            isFinal?: boolean;
            text?: string;
            content?: string;
          };
          if (data.taskId !== taskId) {
            continue;
          }
          if (!data.isFinal) {
            continue;
          }
          clearTimeout(timer);
          sub.unsubscribe();
          resolve(data.text ?? data.content ?? null);
          return;
        } catch {
          // JSON 파싱 실패 무시
        }
      }
    })().catch(() => {});
  });

  await nc.drain();

  if (!result) {
    process.stderr.write(`[delegate] 타임아웃: ${agentId} 가 ${timeoutMs}ms 내에 응답하지 않음\n`);
    process.exit(1);
  }

  process.stdout.write(result);
}

main().catch((err: Error) => {
  process.stderr.write(`[delegate] 오류: ${err.message}\n`);
  process.exit(1);
});
