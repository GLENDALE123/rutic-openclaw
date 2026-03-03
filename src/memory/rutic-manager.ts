/**
 * RUTIC Memory Backend
 *
 * sqlite-vec 대신 RUTIC memory service (Chroma + Postgres)를 사용하는 백엔드.
 * 설정: memory.backend = "rutic", memory.rutic.url = "http://rutic-memory:8080"
 */

import { createSubsystemLogger } from "../logging/subsystem.js";
import type { ResolvedRuticConfig } from "./backend-config.js";
import type {
  MemoryEmbeddingProbeResult,
  MemoryProviderStatus,
  MemorySearchManager,
  MemorySearchResult,
} from "./types.js";

const log = createSubsystemLogger("memory-rutic");

type RuticSearchResponse = {
  results: Array<{
    id: string;
    score: number;
    snippet: string;
    source: "memory" | "sessions";
    path?: string;
    startLine?: number;
    endLine?: number;
    citation?: string;
  }>;
};

type RuticReadResponse = {
  text: string;
  path: string;
};

type RuticHealthResponse = {
  ok: boolean;
  chroma: boolean;
  postgres: boolean;
};

export class RuticMemoryManager implements MemorySearchManager {
  private readonly cfg: ResolvedRuticConfig;

  constructor(cfg: ResolvedRuticConfig) {
    this.cfg = cfg;
  }

  async search(
    query: string,
    opts?: { maxResults?: number; minScore?: number; sessionKey?: string },
  ): Promise<MemorySearchResult[]> {
    const url = `${this.cfg.url}/memory/search`;
    const body = {
      query,
      namespace: this.cfg.namespace,
      maxResults: opts?.maxResults ?? this.cfg.maxResults,
      minScore: opts?.minScore,
      sessionKey: opts?.sessionKey,
    };

    let res: Response;
    try {
      res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(this.cfg.timeoutMs),
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log.warn(`rutic memory search failed: ${msg}`);
      return [];
    }

    if (!res.ok) {
      log.warn(`rutic memory search HTTP ${res.status}`);
      return [];
    }

    const data = (await res.json()) as RuticSearchResponse;
    return data.results.map((r) => ({
      path: r.path ?? "",
      startLine: r.startLine ?? 0,
      endLine: r.endLine ?? 0,
      score: r.score,
      snippet: r.snippet,
      source: r.source,
      citation: r.citation,
    }));
  }

  async readFile(params: {
    relPath: string;
    from?: number;
    lines?: number;
  }): Promise<{ text: string; path: string }> {
    const url = `${this.cfg.url}/memory/read`;
    const body = {
      namespace: this.cfg.namespace,
      relPath: params.relPath,
      from: params.from,
      lines: params.lines,
    };

    let res: Response;
    try {
      res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(this.cfg.timeoutMs),
      });
    } catch (err) {
      throw new Error(`rutic memory read failed: ${err instanceof Error ? err.message : String(err)}`);
    }

    if (!res.ok) {
      throw new Error(`rutic memory read HTTP ${res.status}`);
    }

    return (await res.json()) as RuticReadResponse;
  }

  status(): MemoryProviderStatus {
    return {
      backend: "builtin", // MemoryProviderStatus 타입 호환용 (rutic 추가 전까지)
      provider: "rutic",
      workspaceDir: this.cfg.namespace,
      custom: {
        url: this.cfg.url,
        namespace: this.cfg.namespace,
        maxResults: this.cfg.maxResults,
      },
    };
  }

  async probeEmbeddingAvailability(): Promise<MemoryEmbeddingProbeResult> {
    try {
      const res = await fetch(`${this.cfg.url}/health`, {
        signal: AbortSignal.timeout(this.cfg.timeoutMs),
      });
      if (!res.ok) {
        return { ok: false, error: `HTTP ${res.status}` };
      }
      const data = (await res.json()) as RuticHealthResponse;
      return data.ok ? { ok: true } : { ok: false, error: "rutic service unhealthy" };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return { ok: false, error: msg };
    }
  }

  async probeVectorAvailability(): Promise<boolean> {
    const probe = await this.probeEmbeddingAvailability();
    return probe.ok;
  }
}
