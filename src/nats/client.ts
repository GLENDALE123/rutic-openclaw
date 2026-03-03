import { connect, type ConnectionOptions, type NatsConnection } from "nats";
import { createSubsystemLogger } from "../logging/subsystem.js";
import type { NatsTransportConfig } from "./types.js";

const log = createSubsystemLogger("nats");

export async function createNatsClient(cfg: NatsTransportConfig): Promise<NatsConnection> {
  const opts: ConnectionOptions = {
    servers: cfg.servers,
    name: `rutic-openclaw-${cfg.agentId}`,
    reconnect: true,
    maxReconnectAttempts: -1,
  };

  if (cfg.credentialsPath) {
    const { credsAuthenticator } = await import("nats");
    const { readFile } = await import("node:fs/promises");
    const creds = await readFile(cfg.credentialsPath);
    opts.authenticator = credsAuthenticator(creds);
  }

  const servers = Array.isArray(cfg.servers) ? cfg.servers.join(", ") : cfg.servers;
  log.info(`nats: ${cfg.agentId} → ${servers}`);

  const conn = await connect(opts);
  void conn.closed().then(() => {
    log.info(`nats: connection closed (agent=${cfg.agentId})`);
  });

  return conn;
}
