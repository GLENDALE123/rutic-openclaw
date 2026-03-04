/**
 * RUTIC Discord ↔ NATS Bridge
 *
 * 여러 Discord 봇 토큰을 동시에 연결하여 각 봇의 메시지를
 * 대응하는 NATS 에이전트(task.{agentId})로 라우팅한다.
 *
 * 환경 변수:
 *   RUTIC_NATS_URL       — NATS 서버 주소 (기본: nats://localhost:4222)
 *   RUTIC_PG_URL         — (선택) Postgres 연결
 *   DISCORD_BOT_*        — 봇 설정 (아래 형식 참조)
 *
 * 봇 설정 방식 (JSON 파일 또는 환경 변수):
 *   RUTIC_DISCORD_BOTS=JSON 배열
 *   예: '[{"token":"...","agentId":"ceo"},{"token":"...","agentId":"cto"}]'
 *
 * 또는 개별 환경 변수:
 *   DISCORD_BOT_CEO=토큰
 *   DISCORD_BOT_CTO=토큰
 *   ...
 *
 * Discord 봇 Developer Portal 필수 설정:
 *   - MESSAGE CONTENT INTENT 활성화
 *   - SERVER MEMBERS INTENT 활성화 (선택)
 */

import { createRequire } from "node:module";
import { connect, StringCodec, type NatsConnection } from "nats";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { resolveNatsServers } from "./transport.js";

const require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
const WebSocket = require("ws");

const log = createSubsystemLogger("discord-bridge");
const sc = StringCodec();

// ── 타입 ──────────────────────────────────────────────────────────────────

type BotConfig = {
  token: string;
  agentId: string;
  allowedGuildIds?: string[]; // 비어있으면 모든 서버 허용
  allowedUserIds?: string[]; // 비어있으면 모든 사용자 허용 (봇 제외)
};

type DiscordMessage = {
  id: string;
  channel_id: string;
  guild_id?: string;
  content: string;
  author: { id: string; username: string; bot?: boolean };
  mentions: Array<{ id: string }>;
};

type DiscordInteraction = {
  id: string;
  token: string;
  application_id: string;
  type: number; // 2 = APPLICATION_COMMAND
  channel_id: string;
  guild_id?: string;
  user?: { id: string; username: string };
  member?: { user: { id: string; username: string } };
  data: {
    name: string;
    options?: Array<{ name: string; value: unknown }>;
  };
};

// ── 설정 로드 ─────────────────────────────────────────────────────────────

const ALLOWED_GUILD_IDS = ["1477725209116016753"]; // Oracle Discord 서버
const ALLOWED_USER_IDS = ["1397490231023894541"]; // 허용된 사용자 ID

/** 환경 변수에서 봇 설정 목록 로드 */
function loadBotConfigs(): BotConfig[] {
  // 방법 1: RUTIC_DISCORD_BOTS JSON 배열
  const jsonBots = process.env["RUTIC_DISCORD_BOTS"]?.trim();
  if (jsonBots) {
    try {
      const parsed = JSON.parse(jsonBots) as BotConfig[];
      log.info(`봇 설정 로드: ${parsed.length}개 (RUTIC_DISCORD_BOTS)`);
      return parsed.map((b) => ({
        ...b,
        allowedGuildIds: b.allowedGuildIds ?? ALLOWED_GUILD_IDS,
        allowedUserIds: b.allowedUserIds ?? ALLOWED_USER_IDS,
      }));
    } catch {
      log.warn("RUTIC_DISCORD_BOTS JSON 파싱 실패");
    }
  }

  // 방법 2: DISCORD_BOT_{AGENTID} 개별 환경 변수
  const agentIds = [
    "ceo",
    "cto",
    "cfo",
    "coo",
    "cmo",
    "chro",
    "researcher",
    "pm",
    "risk",
    "system",
  ];
  const configs: BotConfig[] = [];
  for (const agentId of agentIds) {
    const token = process.env[`DISCORD_BOT_${agentId.toUpperCase()}`]?.trim();
    if (token) {
      configs.push({
        token,
        agentId,
        allowedGuildIds: ALLOWED_GUILD_IDS,
        allowedUserIds: agentId === "system" ? undefined : ALLOWED_USER_IDS,
      });
    }
  }
  if (configs.length > 0) {
    log.info(`봇 설정 로드: ${configs.length}개 (DISCORD_BOT_* 환경 변수)`);
    return configs;
  }

  log.warn("봇 설정 없음 — RUTIC_DISCORD_BOTS 또는 DISCORD_BOT_{AGENTID} 환경 변수 필요");
  return [];
}

// ── Discord API 헬퍼 ──────────────────────────────────────────────────────

const DISCORD_API = "https://discord.com/api/v10";
const INTENTS =
  (1 << 0) | // GUILDS
  (1 << 9) | // GUILD_MESSAGES
  (1 << 12) | // DIRECT_MESSAGES
  (1 << 15); // MESSAGE_CONTENT (privileged — Developer Portal에서 활성화 필요)

// Discord 슬래시 커맨드 정의 (OpenClaw 내부 커맨드 매핑)
const SLASH_COMMANDS = [
  { name: "compact", description: "대화 컨텍스트를 압축합니다 (/compact)" },
  { name: "reset", description: "현재 세션을 초기화합니다 (/reset)" },
  { name: "new", description: "새 세션을 시작합니다 (/new)" },
  {
    name: "think",
    description: "추론 깊이를 설정합니다 (/think)",
    options: [
      {
        name: "level",
        description: "추론 수준 (off / low / medium / high / xhigh)",
        type: 3, // STRING
        required: true,
        choices: [
          { name: "off", value: "off" },
          { name: "low", value: "low" },
          { name: "medium", value: "medium" },
          { name: "high", value: "high" },
          { name: "xhigh", value: "xhigh" },
        ],
      },
    ],
  },
];

async function discordRest(
  token: string,
  method: string,
  path: string,
  body?: unknown,
): Promise<unknown> {
  const res = await fetch(`${DISCORD_API}${path}`, {
    method,
    headers: {
      Authorization: `Bot ${token}`,
      "Content-Type": "application/json",
    },
    body: body != null ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Discord REST ${method} ${path} → ${res.status}: ${text}`);
  }
  return res.status === 204 ? null : res.json();
}

/** Discord 메시지 전송 (2000자 제한 자동 분할) */
async function sendDiscordMessage(
  token: string,
  channelId: string,
  content: string,
  replyToMessageId?: string,
): Promise<void> {
  const chunks = splitMessage(content, 1990);
  for (let i = 0; i < chunks.length; i++) {
    const body: Record<string, unknown> = { content: chunks[i] };
    if (i === 0 && replyToMessageId) {
      body["message_reference"] = { message_id: replyToMessageId };
      body["allowed_mentions"] = { replied_user: false };
    }
    await discordRest(token, "POST", `/channels/${channelId}/messages`, body);
  }
}

function splitMessage(text: string, maxLen: number): string[] {
  if (text.length <= maxLen) {
    return [text];
  }
  const chunks: string[] = [];
  let remaining = text;
  while (remaining.length > maxLen) {
    // 마지막 개행에서 분할
    const idx = remaining.lastIndexOf("\n", maxLen);
    const cutAt = idx > 0 ? idx : maxLen;
    chunks.push(remaining.slice(0, cutAt));
    remaining = remaining.slice(cutAt).trimStart();
  }
  if (remaining) {
    chunks.push(remaining);
  }
  return chunks;
}

// ── NATS 태스크 발행 ─────────────────────────────────────────────────────

async function publishToAgent(
  nc: NatsConnection,
  agentId: string,
  content: string,
  sessionKey: string,
  from: string,
): Promise<string> {
  const taskId = `discord-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
  nc.publish(
    `task.${agentId}`,
    sc.encode(
      JSON.stringify({
        taskId,
        sessionKey,
        from,
        to: `agent:${agentId}`,
        body: content,
        channelType: "discord",
      }),
    ),
  );
  log.debug(`→ task.${agentId} | taskId=${taskId} session=${sessionKey}`);
  return taskId;
}

/** reply.send 에서 특정 taskId의 최종 응답 대기 (타임아웃: 180초) */
async function waitForReply(
  nc: NatsConnection,
  taskId: string,
  timeoutMs = 180_000,
): Promise<string | null> {
  return new Promise((resolve) => {
    const sub = nc.subscribe("reply.send");
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
            content?: string;
            text?: string;
          };
          if (data.taskId !== taskId) {
            continue;
          }
          if (!data.isFinal) {
            continue;
          }
          clearTimeout(timer);
          sub.unsubscribe();
          resolve(data.content ?? data.text ?? null);
          return;
        } catch {
          // JSON 파싱 실패 무시
        }
      }
    })().catch(() => {});
  });
}

// ── Discord Gateway WebSocket ─────────────────────────────────────────────

type DiscordBot = {
  token: string;
  agentId: string;
  allowedGuildIds?: string[];
  allowedUserIds?: string[];
  ws?: typeof WebSocket;
  heartbeatInterval?: NodeJS.Timeout;
  sessionId?: string;
  sequence?: number;
  botUserId?: string; // READY 이벤트에서 설정
  botApplicationId?: string; // READY 이벤트에서 설정
};

/** 길드에 슬래시 커맨드 등록 (READY 시 1회 실행) */
async function registerSlashCommands(bot: DiscordBot, guildIds: string[]): Promise<void> {
  if (!bot.botApplicationId) {
    return;
  }
  for (const guildId of guildIds) {
    try {
      await discordRest(
        bot.token,
        "PUT",
        `/applications/${bot.botApplicationId}/guilds/${guildId}/commands`,
        SLASH_COMMANDS,
      );
      log.info(`[${bot.agentId}] 슬래시 커맨드 등록 완료 — guild=${guildId}`);
    } catch (err) {
      log.warn(`[${bot.agentId}] 슬래시 커맨드 등록 실패 guild=${guildId}: ${String(err)}`);
    }
  }
}

function createDiscordGateway(bot: DiscordBot, nc: NatsConnection, gatewayUrl: string): void {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
  const ws: typeof WebSocket = new WebSocket(`${gatewayUrl}?v=10&encoding=json`);
  bot.ws = ws;

  ws.on("open", () => {
    log.info(`[${bot.agentId}] Discord Gateway 연결됨`);
  });

  ws.on("message", (data: Buffer) => {
    let payload: {
      op: number;
      d?: unknown;
      s?: number;
      t?: string;
    };
    try {
      payload = JSON.parse(data.toString()) as typeof payload;
    } catch {
      return;
    }

    if (payload.s != null) {
      bot.sequence = payload.s;
    }

    switch (payload.op) {
      case 10: {
        // Hello — 하트비트 시작 + Identify
        const hello = payload.d as { heartbeat_interval: number };
        const interval = hello.heartbeat_interval;

        // 초기 heartbeat는 랜덤 지연 후 전송
        setTimeout(
          () => {
            ws.send(JSON.stringify({ op: 1, d: bot.sequence ?? null }));
          },
          Math.floor(Math.random() * interval),
        );

        bot.heartbeatInterval = setInterval(() => {
          ws.send(JSON.stringify({ op: 1, d: bot.sequence ?? null }));
        }, interval);

        // Identify
        ws.send(
          JSON.stringify({
            op: 2,
            d: {
              token: bot.token,
              intents: INTENTS,
              properties: { os: "linux", browser: "rutic", device: "rutic" },
              presence: {
                status: "online",
                activities: [{ name: `RUTIC ${bot.agentId.toUpperCase()}`, type: 0 }],
              },
            },
          }),
        );
        break;
      }

      case 11:
        // Heartbeat ACK
        break;

      case 0: {
        // Dispatch
        const event = payload.t;
        if (event === "READY") {
          const ready = payload.d as {
            session_id: string;
            user: { id: string; username: string };
            application: { id: string };
          };
          bot.sessionId = ready.session_id;
          bot.botUserId = ready.user.id;
          bot.botApplicationId = ready.application.id;
          log.info(`[${bot.agentId}] 준비 완료 — 봇: ${ready.user.username} (id=${ready.user.id})`);
          // 허용된 길드에 슬래시 커맨드 등록
          if (bot.allowedGuildIds && bot.allowedGuildIds.length > 0) {
            void registerSlashCommands(bot, bot.allowedGuildIds);
          }
        } else if (event === "MESSAGE_CREATE") {
          void handleMessage(bot, nc, payload.d as DiscordMessage);
        } else if (event === "INTERACTION_CREATE") {
          void handleInteraction(bot, nc, payload.d as DiscordInteraction);
        }
        break;
      }

      case 7:
        // Reconnect
        log.info(`[${bot.agentId}] 재연결 요청`);
        ws.close();
        break;

      case 9:
        // Invalid Session
        log.warn(`[${bot.agentId}] 세션 무효 — 재연결 중...`);
        ws.close();
        break;
    }
  });

  ws.on("close", () => {
    log.warn(`[${bot.agentId}] 연결 끊김 — 5초 후 재연결`);
    if (bot.heartbeatInterval) {
      clearInterval(bot.heartbeatInterval);
    }
    setTimeout(() => createDiscordGateway(bot, nc, gatewayUrl), 5_000);
  });

  ws.on("error", (err: Error) => {
    log.warn(`[${bot.agentId}] WebSocket 오류: ${err.message}`);
  });
}

async function handleMessage(
  bot: DiscordBot,
  nc: NatsConnection,
  msg: DiscordMessage,
): Promise<void> {
  // 봇 메시지 무시
  if (msg.author.bot) {
    return;
  }

  // 서버 필터
  if (
    bot.allowedGuildIds &&
    bot.allowedGuildIds.length > 0 &&
    msg.guild_id &&
    !bot.allowedGuildIds.includes(msg.guild_id)
  ) {
    return;
  }

  // 사용자 필터
  if (
    bot.allowedUserIds &&
    bot.allowedUserIds.length > 0 &&
    !bot.allowedUserIds.includes(msg.author.id)
  ) {
    return;
  }

  // 멘션 필터 — 길드 채널에서는 이 봇이 멘션된 경우에만 응답
  // DM(guild_id 없음)은 멘션 없이도 응답
  if (msg.guild_id && bot.botUserId) {
    const mentioned = msg.mentions.some((m) => m.id === bot.botUserId);
    if (!mentioned) {
      return;
    }
  }

  // 멘션 태그(<@id>) 제거 후 실제 내용 추출
  const content = msg.content.replace(/<@!?\d+>/g, "").trim();
  if (!content) {
    return;
  }

  log.info(`[${bot.agentId}] 메시지 수신: "${content.slice(0, 60)}" from=${msg.author.id}`);

  // 세션 키 = guild:channel:user 또는 dm:channel
  const sessionKey = msg.guild_id
    ? `discord:${msg.guild_id}:${msg.channel_id}:${msg.author.id}`
    : `discord:dm:${msg.channel_id}`;

  const taskId = await publishToAgent(nc, bot.agentId, content, sessionKey, msg.author.id);

  // 입력 중 표시
  discordRest(bot.token, "POST", `/channels/${msg.channel_id}/typing`, null).catch(() => {});

  const reply = await waitForReply(nc, taskId);
  if (!reply) {
    log.warn(`[${bot.agentId}] 응답 타임아웃 — taskId=${taskId}`);
    await sendDiscordMessage(
      bot.token,
      msg.channel_id,
      "⏱️ 응답 시간이 초과됐습니다. 잠시 후 다시 시도해주세요.",
      msg.id,
    ).catch(() => {});
    return;
  }

  await sendDiscordMessage(bot.token, msg.channel_id, reply, msg.id).catch((err: Error) => {
    log.warn(`[${bot.agentId}] Discord 응답 전송 실패: ${err.message}`);
  });
}

/** Discord 슬래시 커맨드 처리 */
async function handleInteraction(
  bot: DiscordBot,
  nc: NatsConnection,
  interaction: DiscordInteraction,
): Promise<void> {
  // APPLICATION_COMMAND(2)만 처리
  if (interaction.type !== 2) {
    return;
  }

  const user = interaction.user ?? interaction.member?.user;
  if (!user) {
    return;
  }

  // 사용자 필터
  if (
    bot.allowedUserIds &&
    bot.allowedUserIds.length > 0 &&
    !bot.allowedUserIds.includes(user.id)
  ) {
    // 권한 없는 사용자 — ephemeral 오류 메시지
    await discordRest(
      bot.token,
      "POST",
      `/interactions/${interaction.id}/${interaction.token}/callback`,
      { type: 4, data: { content: "❌ 이 봇을 사용할 권한이 없습니다.", flags: 64 } },
    ).catch(() => {});
    return;
  }

  // OpenClaw 커맨드 body 조립
  const cmdName = interaction.data.name;
  const options = interaction.data.options ?? [];
  const args = options.map((o) => String(o.value)).join(" ");
  const body = args ? `/${cmdName} ${args}` : `/${cmdName}`;

  log.info(`[${bot.agentId}] 슬래시 커맨드: ${body} from=${user.id}`);

  // Discord에 즉시 "처리 중" 응답 (3초 내 필수)
  await discordRest(
    bot.token,
    "POST",
    `/interactions/${interaction.id}/${interaction.token}/callback`,
    { type: 5 }, // DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE
  ).catch((err) => {
    log.warn(`[${bot.agentId}] interaction defer 실패: ${String(err)}`);
  });

  const sessionKey = interaction.guild_id
    ? `discord:${interaction.guild_id}:${interaction.channel_id}:${user.id}`
    : `discord:dm:${interaction.channel_id}`;

  const taskId = await publishToAgent(nc, bot.agentId, body, sessionKey, user.id);

  const reply = await waitForReply(nc, taskId);
  const replyContent = reply ?? "⏱️ 응답 시간이 초과됐습니다.";

  // 원본 interaction 메시지 업데이트
  const chunks = splitMessage(replyContent, 1990);
  await discordRest(
    bot.token,
    "PATCH",
    `/webhooks/${interaction.application_id}/${interaction.token}/messages/@original`,
    { content: chunks[0] },
  ).catch((err) => {
    log.warn(`[${bot.agentId}] interaction 응답 전송 실패: ${String(err)}`);
  });

  // 2000자 초과 시 후속 메시지
  for (let i = 1; i < chunks.length; i++) {
    await discordRest(
      bot.token,
      "POST",
      `/webhooks/${interaction.application_id}/${interaction.token}`,
      { content: chunks[i] },
    ).catch(() => {});
  }
}

// ── 메인 ─────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const configs = loadBotConfigs();
  if (configs.length === 0) {
    log.warn("연결할 봇이 없습니다. 종료합니다.");
    process.exit(1);
  }

  // NATS 연결
  const servers = resolveNatsServers();
  const nc = await connect({ servers });
  log.info(`NATS 연결: ${typeof servers === "string" ? servers : servers[0]}`);

  // Discord Gateway URL 조회 (토큰 하나로 충분)
  const firstToken = configs[0].token;
  const gwInfo = (await discordRest(firstToken, "GET", "/gateway/bot", null)) as {
    url: string;
    shards: number;
  };
  const gatewayUrl = gwInfo.url;
  log.info(`Discord Gateway: ${gatewayUrl} (shards: ${gwInfo.shards})`);

  // 각 봇 연결
  for (const cfg of configs) {
    const bot: DiscordBot = { ...cfg };
    createDiscordGateway(bot, nc, gatewayUrl);
    // rate limit 방지: 연결 사이 500ms 간격
    await new Promise((r) => setTimeout(r, 500));
  }

  log.info(`Discord-NATS 브릿지 실행 중 — 봇 ${configs.length}개`);

  // Graceful shutdown
  process.on("SIGINT", async () => {
    log.info("종료 중...");
    await nc.drain();
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    log.info("종료 중...");
    await nc.drain();
    process.exit(0);
  });
}

main().catch((err: Error) => {
  log.warn(`치명적 오류: ${err.message}`);
  process.exit(1);
});
