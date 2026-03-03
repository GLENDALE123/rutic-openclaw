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
  const { conn, cfg, natsCfg, payload, replySubject } = params;
  const { taskId, agentId, sessionKey, body, from, correlationId } = payload;

  log.verbose(`nats-agent [${agentId}]: task=${taskId} body="${body.slice(0, 80)}"`);

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
      if (replyPayload.isReasoning) return;
      const text = replyPayload.text ?? "";
      publish({
        code: "EVT_REPLY_SEND",
        taskId,
        agentId,
        sessionKey,
        text,
        isFinal: info.kind === "final",
        isError: replyPayload.isError,
        timestamp: Date.now(),
        correlationId,
      });
    },
    onError: (err, info) => {
      const msg = err instanceof Error ? err.message : String(err);
      log.error(`nats-agent [${agentId}]: ${info.kind} reply error — ${msg}`);
      publishError(`Error: ${msg}`);
    },
  });

  await dispatchInboundMessage({
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
}
