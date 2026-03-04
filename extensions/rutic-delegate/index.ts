/**
 * RUTIC Delegate Task Plugin
 *
 * delegate_task 네이티브 툴 등록.
 * LLM이 구조화된 tool_use 호출로 다른 RUTIC 에이전트에게 태스크를 위임함.
 * SOUL.md/SKILL.md 컨텍스트 의존 없이 코드 레벨에서 NATS 직접 통신.
 */

import { Type } from "@sinclair/typebox";
import { connect, StringCodec, type NatsConnection } from "nats";
import type { AnyAgentTool, OpenClawPluginApi } from "../../src/plugins/types.js";

const sc = StringCodec();

// 모듈 레벨 싱글톤 NATS 연결 (에이전트당 1개 유지)
let _nc: NatsConnection | null = null;

async function getNatsConnection(): Promise<NatsConnection> {
  if (_nc && !_nc.isClosed() && !_nc.isDraining()) {
    return _nc;
  }
  const servers = process.env["RUTIC_NATS_URL"] ?? "nats://localhost:4222";
  const credPath = process.env["RUTIC_NATS_CREDS"]?.trim();

  const opts: Parameters<typeof connect>[0] = {
    servers,
    name: "rutic-delegate-plugin",
    reconnect: true,
    maxReconnectAttempts: -1,
  };

  if (credPath) {
    const { credsAuthenticator } = await import("nats");
    const { readFile } = await import("node:fs/promises");
    const creds = await readFile(credPath);
    opts.authenticator = credsAuthenticator(creds);
  }

  _nc = await connect(opts);
  _nc
    .closed()
    .then(() => {
      _nc = null;
    })
    .catch(() => {
      _nc = null;
    });
  return _nc;
}

const DelegateParams = Type.Object({
  agentId: Type.String({
    description:
      "위임할 에이전트 ID. 가능한 값: researcher, ceo, cfo, cto, coo, cmo, chro, pm, risk",
  }),
  message: Type.String({
    description: "에이전트에게 전달할 태스크 메시지",
  }),
  sessionKey: Type.Optional(
    Type.String({
      description: "세션 키 (지정 시 히스토리 공유 가능). 생략 시 독립 세션 생성",
    }),
  ),
  timeoutMs: Type.Optional(
    Type.Number({
      description: "응답 대기 타임아웃 밀리초 (기본 120000)",
    }),
  ),
});

type DelegateInput = {
  agentId: string;
  message: string;
  sessionKey?: string;
  timeoutMs?: number;
};

const delegateTaskTool = {
  name: "delegate_task",
  label: "에이전트 태스크 위임",
  description: `다른 RUTIC 에이전트에게 태스크를 위임하고 결과를 반환합니다.

사용 가능한 에이전트:
- researcher: 시장조사, 데이터 분석, 트렌드
- ceo: 전략 결정, 최종 판단
- cfo: 재무 분석, 예산, ROI
- cto: 기술 검토, 아키텍처
- coo: 운영 계획, 프로세스
- cmo: 마케팅 전략, 브랜딩
- chro: 인사, 채용, 조직
- pm: 프로젝트 관리, 일정
- risk: 리스크 분석

주의: 위임받는 에이전트가 현재 실행 중이어야 응답 가능합니다.`,
  parameters: DelegateParams,
  execute: async (_toolCallId: string, params: DelegateInput, signal?: AbortSignal) => {
    const callerId = process.env["RUTIC_AGENT_ID"] ?? "unknown";
    const taskId = `delegate-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const resolvedSession = params.sessionKey ?? `agent:${callerId}:delegate:${taskId}`;
    const timeoutMs = params.timeoutMs ?? 120_000;

    let nc: NatsConnection;
    try {
      nc = await getNatsConnection();
    } catch (err) {
      return {
        content: [{ type: "text" as const, text: `NATS 연결 실패: ${String(err)}` }],
        details: { error: "nats_connect_failed" },
      };
    }

    // reply.send 구독을 발행보다 먼저 등록 (레이스 컨디션 방지)
    const sub = nc.subscribe("reply.send");

    nc.publish(
      `task.${params.agentId}`,
      sc.encode(
        JSON.stringify({
          code: "EVT_TASK_CREATED",
          taskId,
          agentId: params.agentId,
          sessionKey: resolvedSession,
          from: callerId,
          body: params.message,
          timestamp: Date.now(),
          channelType: "agent",
        }),
      ),
    );

    const result = await new Promise<string | null>((resolve) => {
      const timer = setTimeout(() => {
        sub.unsubscribe();
        resolve(null);
      }, timeoutMs);

      // abort signal 지원
      signal?.addEventListener("abort", () => {
        clearTimeout(timer);
        sub.unsubscribe();
        resolve(null);
      });

      (async () => {
        for await (const m of sub) {
          try {
            const data = JSON.parse(sc.decode(m.data)) as {
              taskId?: string;
              isFinal?: boolean;
              text?: string;
              content?: string;
            };
            if (data.taskId !== taskId) continue;
            if (!data.isFinal) continue;
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

    if (!result) {
      const reason = signal?.aborted ? "취소됨" : `${timeoutMs}ms 타임아웃`;
      return {
        content: [
          {
            type: "text" as const,
            text: `${params.agentId} 에이전트가 응답하지 않았습니다 (${reason}).`,
          },
        ],
        details: { error: "no_response", agentId: params.agentId },
      };
    }

    return {
      content: [{ type: "text" as const, text: result }],
      details: { agentId: params.agentId, taskId, result },
    };
  },
};

const DELEGATE_SYSTEM_HINT = `\n\n## RUTIC 에이전트 협업
다른 에이전트의 전문성이 필요할 때는 \`delegate_task\` 툴을 사용하세요.
- researcher: 시장조사, 데이터 분석, 트렌드
- ceo: 전략 결정, 최종 판단
- cfo: 재무 분석, 예산, ROI
- cto: 기술 검토, 아키텍처
- coo: 운영 계획 / cmo: 마케팅 / chro: 인사 / pm: 일정 / risk: 리스크
에이전트가 실행 중이어야 응답 가능합니다.`;

export default function register(api: OpenClawPluginApi) {
  api.registerTool(delegateTaskTool as unknown as AnyAgentTool);

  // 모든 에이전트 실행 전 delegate_task 사용 지침을 시스템 프롬프트에 주입
  api.on("before_agent_start", () => {
    return { prependContext: DELEGATE_SYSTEM_HINT };
  });
}
