import { StringCodec, type NatsConnection, type Subscription } from "nats";
import { dispatchInboundMessage } from "../auto-reply/dispatch.js";
import { createReplyDispatcher } from "../auto-reply/reply/reply-dispatcher.js";
import type { ReplyPayload } from "../auto-reply/types.js";
import type { OpenClawConfig } from "../config/config.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import type { NatsReplyPayload, NatsTaskPayload, NatsTransportConfig } from "./types.js";

const log = createSubsystemLogger("nats-agent");
const sc = StringCodec();

export type NatsAgentSubscription = {
  subscription: Subscription;
  close: () => Promise<void>;
};

/**
 * NATS task subscriber — task.{agentId} 수신 후 OpenClaw dispatchInboundMessage 호출
 */
export function startNatsAgentSubscriber(params: {
  conn: NatsConnection;
  cfg: OpenClawConfig;
  natsCfg: NatsTransportConfig;
}): NatsAgentSubscription {
  const { conn, cfg, natsCfg } = params;
  const { agentId, taskSubjectPrefix, replySubject, queueGroup } = natsCfg;
  const taskSubject = `${taskSubjectPrefix}.${agentId}`;

  log.info(`nats-agent [${agentId}]: subscribe ${taskSubject} queue=${queueGroup}`);

  const sub = conn.subscribe(taskSubject, { queue: queueGroup });

  void (async () => {
    for await (const msg of sub) {
      let payload: NatsTaskPayload;
      try {
        payload = JSON.parse(sc.decode(msg.data)) as NatsTaskPayload;
      } catch (err) {
        log.warn(`nats-agent [${agentId}]: invalid payload — ${String(err)}`);
        continue;
      }

      void handleTask({ conn, cfg, natsCfg, payload, replySubject }).catch((err) => {
        log.error(`nats-agent [${agentId}]: task ${payload.taskId} — ${String(err)}`);
      });
    }
    log.info(`nats-agent [${agentId}]: subscription ended`);
  })();

  return {
    subscription: sub,
    close: async () => {
      sub.unsubscribe();
    },
  };
}

async function handleTask(params: {
  conn: NatsConnection;
  cfg: OpenClawConfig;
  natsCfg: NatsTransportConfig;
  payload: NatsTaskPayload;
  replySubject: string;
}): Promise<void> {
  const { conn, cfg, payload, replySubject } = params;
  const { taskId, agentId, sessionKey, body, from, correlationId } = payload;

  const startedAt = Date.now();
  let firstReplyAt: number | null = null;
  let replyCount = 0;

  // ── [1] task 수신 ─────────────────────────────────────────────────────────
  log.info(
    `[TRACE] task 수신 | taskId=${taskId} agentId=${agentId} ` +
      `from=${from ?? "nats"} sessionKey=${sessionKey ?? "-"} ` +
      `body="${body.slice(0, 120)}"`,
  );

  const publish = (reply: NatsReplyPayload): void => {
    try {
      conn.publish(replySubject, sc.encode(JSON.stringify(reply)));
    } catch (err) {
      log.warn(`nats-agent [${agentId}]: publish failed — ${String(err)}`);
    }
  };

  const publishError = (text: string): void => {
    publish({
      code: "EVT_REPLY_SEND",
      taskId,
      agentId,
      sessionKey,
      text,
      isFinal: true,
      isError: true,
      timestamp: Date.now(),
      correlationId,
    });
  };

  const dispatcher = createReplyDispatcher({
    deliver: async (replyPayload: ReplyPayload, info) => {
      if (replyPayload.isReasoning) {
        return;
      }

      const now = Date.now();
      replyCount++;
      if (firstReplyAt === null) {
        firstReplyAt = now;
      }

      const text = replyPayload.text ?? "";

      // ── [2] LLM 청크/최종 응답 ───────────────────────────────────────────
      log.info(
        `[TRACE] reply #${replyCount} | kind=${info.kind} isFinal=${info.kind === "final"} ` +
          `ttfr=${firstReplyAt - startedAt}ms elapsed=${now - startedAt}ms ` +
          `chars=${text.length} preview="${text.slice(0, 80)}"`,
      );

      publish({
        code: "EVT_REPLY_SEND",
        taskId,
        agentId,
        sessionKey,
        text,
        isFinal: info.kind === "final",
        isError: replyPayload.isError,
        timestamp: now,
        correlationId,
      });
    },
    onError: (err, info) => {
      const msg = err instanceof Error ? err.message : String(err);
      log.error(`nats-agent [${agentId}]: ${info.kind} reply error — ${msg}`);
      publishError(`Error: ${msg}`);
    },
  });

  // ── [3] dispatchInboundMessage 진입 ──────────────────────────────────────
  log.info(
    `[TRACE] dispatch 시작 | taskId=${taskId} ` +
      `ctx.From=${from ?? `nats:task:${taskId}`} ctx.To=agent:${agentId}`,
  );

  const result = await dispatchInboundMessage({
    ctx: {
      Body: body,
      RawBody: body,
      CommandBody: body,
      From: from ?? `nats:task:${taskId}`,
      To: `agent:${agentId}`,
      SessionKey: sessionKey,
      ChatType: "direct" as const,
    },
    cfg,
    dispatcher,
  });

  // ── [4] dispatch 완료 ─────────────────────────────────────────────────────
  const totalMs = Date.now() - startedAt;
  log.info(
    `[TRACE] dispatch 완료 | taskId=${taskId} ` +
      `queuedFinal=${result.queuedFinal} counts=${JSON.stringify(result.counts)} ` +
      `replies=${replyCount} totalMs=${totalMs}`,
  );

  if (!result.queuedFinal && replyCount === 0) {
    log.warn(
      `[TRACE] 응답 없음 | taskId=${taskId} — ` +
        `LLM 미호출 가능성 (provider 미설정 또는 라우팅 차단)`,
    );
  }
}
