/**
 * RUTIC Postgres + Chroma 메모리 매니저
 *
 * MemorySearchManager 인터페이스를 구현한다:
 * - search()   → Chroma HTTP API (rutic_memory_{agentId} 컬렉션) 벡터 검색
 * - readFile() → Postgres agent_memory 텍스트 조회
 * - upsert()   → Chroma 임베딩 + Postgres 저장 (외부 호출용)
 *
 * 환경변수:
 *   RUTIC_PG_URL      — Postgres 연결 문자열
 *   RUTIC_CHROMA_URL  — Chroma HTTP URL (예: http://100.75.107.81:8000)
 */

import pg from "pg";
import { createSubsystemLogger } from "../logging/subsystem.js";
import type {
  MemoryEmbeddingProbeResult,
  MemoryProviderStatus,
  MemorySearchManager,
  MemorySearchResult,
} from "./types.js";

const log = createSubsystemLogger("memory-pg-chroma");

// ============================================================================
// Chroma HTTP API 타입
// ============================================================================

type ChromaCollection = {
  id: string;
  name: string;
};

type ChromaQueryResponse = {
  ids: string[][];
  distances: number[][];
  documents: (string | null)[][];
  metadatas: (Record<string, unknown> | null)[][];
};

type ChromaEmbedResponse = {
  embeddings: number[][];
};

// ============================================================================
// Postgres Pool (싱글턴)
// ============================================================================

let _pgPool: pg.Pool | undefined;

function getPgPool(): pg.Pool {
  const url = process.env.RUTIC_PG_URL?.trim();
  if (!url) {
    throw new Error("RUTIC_PG_URL 환경변수가 설정되지 않았습니다");
  }
  if (!_pgPool) {
    _pgPool = new pg.Pool({ connectionString: url });
  }
  return _pgPool;
}

// ============================================================================
// Chroma HTTP 헬퍼
// ============================================================================

function chromaUrl(): string {
  const url = process.env.RUTIC_CHROMA_URL?.trim();
  if (!url) {
    throw new Error("RUTIC_CHROMA_URL 환경변수가 설정되지 않았습니다");
  }
  return url.replace(/\/$/, "");
}

async function chromaFetch<T>(path: string, opts: RequestInit = {}): Promise<T> {
  const res = await fetch(`${chromaUrl()}${path}`, {
    ...opts,
    headers: {
      "Content-Type": "application/json",
      ...(opts.headers as Record<string, string> | undefined),
    },
    signal: opts.signal ?? AbortSignal.timeout(10_000),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Chroma HTTP ${res.status}: ${body}`);
  }
  return res.json() as Promise<T>;
}

async function ensureCollection(collectionName: string): Promise<string> {
  try {
    const col = await chromaFetch<ChromaCollection>(`/api/v1/collections/${collectionName}`);
    return col.id;
  } catch {
    const col = await chromaFetch<ChromaCollection>("/api/v1/collections", {
      method: "POST",
      body: JSON.stringify({ name: collectionName, get_or_create: true }),
    });
    return col.id;
  }
}

async function embedText(text: string): Promise<number[]> {
  // Chroma의 기본 임베딩 함수 사용 (sentence-transformers)
  const res = await chromaFetch<ChromaEmbedResponse>("/api/v1/embed", {
    method: "POST",
    body: JSON.stringify({ texts: [text] }),
  });
  const embedding = res.embeddings[0];
  if (!embedding) {
    throw new Error("임베딩 결과 없음");
  }
  return embedding;
}

// ============================================================================
// PgChromaMemoryManager
// ============================================================================

export class PgChromaMemoryManager implements MemorySearchManager {
  private readonly agentId: string;
  private readonly collectionName: string;

  constructor(agentId: string) {
    this.agentId = agentId;
    this.collectionName = `rutic_memory_${agentId.replace(/[^a-z0-9_-]/gi, "_")}`;
  }

  // --------------------------------------------------------------------------
  // search: Chroma 벡터 검색
  // --------------------------------------------------------------------------
  async search(
    query: string,
    opts?: { maxResults?: number; minScore?: number; sessionKey?: string },
  ): Promise<MemorySearchResult[]> {
    const nResults = opts?.maxResults ?? 5;
    const minScore = opts?.minScore ?? 0;

    let embedding: number[];
    try {
      embedding = await embedText(query);
    } catch (err) {
      log.warn(`임베딩 실패: ${String(err)}`);
      return [];
    }

    let collectionId: string;
    try {
      collectionId = await ensureCollection(this.collectionName);
    } catch (err) {
      log.warn(`Chroma 컬렉션 접근 실패: ${String(err)}`);
      return [];
    }

    let qRes: ChromaQueryResponse;
    try {
      qRes = await chromaFetch<ChromaQueryResponse>(`/api/v1/collections/${collectionId}/query`, {
        method: "POST",
        body: JSON.stringify({
          query_embeddings: [embedding],
          n_results: nResults,
          include: ["documents", "distances", "metadatas"],
        }),
      });
    } catch (err) {
      log.warn(`Chroma 검색 실패: ${String(err)}`);
      return [];
    }

    const results: MemorySearchResult[] = [];
    const ids = qRes.ids[0] ?? [];
    const distances = qRes.distances[0] ?? [];
    const documents = qRes.documents[0] ?? [];
    const metadatas = qRes.metadatas[0] ?? [];

    for (let i = 0; i < ids.length; i++) {
      const distance = distances[i] ?? 1;
      // Chroma는 L2 distance 반환 (낮을수록 유사). cosine distance로 변환.
      const score = 1 - Math.min(distance, 1);
      if (score < minScore) {
        continue;
      }

      const meta = metadatas[i] ?? {};
      const sessionKey = typeof meta.sessionKey === "string" ? meta.sessionKey : undefined;
      if (opts?.sessionKey && sessionKey && sessionKey !== opts.sessionKey) {
        continue;
      }

      results.push({
        path: typeof meta.path === "string" ? meta.path : `memory/${ids[i]}`,
        startLine: typeof meta.startLine === "number" ? meta.startLine : 0,
        endLine: typeof meta.endLine === "number" ? meta.endLine : 0,
        score,
        snippet: documents[i] ?? "",
        source: "memory",
        citation: typeof meta.citation === "string" ? meta.citation : undefined,
      });
    }

    return results;
  }

  // --------------------------------------------------------------------------
  // readFile: Postgres agent_memory 조회
  // --------------------------------------------------------------------------
  async readFile(params: {
    relPath: string;
    from?: number;
    lines?: number;
  }): Promise<{ text: string; path: string }> {
    const client = await getPgPool().connect();
    try {
      const res = await client.query<{ content: string }>(
        `SELECT content FROM agent_memory
          WHERE agent_id = $1
            AND (metadata->>'path' = $2 OR source = $2)
          ORDER BY created_at DESC
          LIMIT 1`,
        [this.agentId, params.relPath],
      );
      const content = res.rows[0]?.content ?? "";

      if (params.from !== undefined || params.lines !== undefined) {
        const linesList = content.split("\n");
        const start = Math.max(0, (params.from ?? 1) - 1);
        const end = params.lines !== undefined ? start + params.lines : linesList.length;
        return { text: linesList.slice(start, end).join("\n"), path: params.relPath };
      }

      return { text: content, path: params.relPath };
    } finally {
      client.release();
    }
  }

  // --------------------------------------------------------------------------
  // upsert: Chroma 임베딩 + Postgres 저장
  // --------------------------------------------------------------------------
  async upsert(params: {
    id: string;
    content: string;
    sessionKey?: string;
    source?: string;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    let embedding: number[];
    try {
      embedding = await embedText(params.content);
    } catch (err) {
      log.warn(`임베딩 실패, Chroma 저장 스킵: ${String(err)}`);
      embedding = [];
    }

    const collectionId = await ensureCollection(this.collectionName);

    // Chroma upsert
    if (embedding.length > 0) {
      await chromaFetch(`/api/v1/collections/${collectionId}/upsert`, {
        method: "POST",
        body: JSON.stringify({
          ids: [params.id],
          embeddings: [embedding],
          documents: [params.content],
          metadatas: [
            {
              ...params.metadata,
              agentId: this.agentId,
              sessionKey: params.sessionKey ?? "",
              source: params.source ?? "manual",
            },
          ],
        }),
      });
    }

    // Postgres 저장
    const client = await getPgPool().connect();
    try {
      await client.query(
        `INSERT INTO agent_memory (id, agent_id, session_key, content, source, metadata)
              VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (id) DO UPDATE
               SET content = $4, metadata = $6, session_key = $3`,
        [
          params.id,
          this.agentId,
          params.sessionKey ?? null,
          params.content,
          params.source ?? "manual",
          JSON.stringify(params.metadata ?? {}),
        ],
      );
    } finally {
      client.release();
    }
  }

  // --------------------------------------------------------------------------
  // status
  // --------------------------------------------------------------------------
  status(): MemoryProviderStatus {
    return {
      backend: "builtin",
      provider: "rutic-pg-chroma",
      workspaceDir: this.agentId,
      custom: {
        collection: this.collectionName,
        pgUrl: process.env.RUTIC_PG_URL ? "(set)" : "(unset)",
        chromaUrl: process.env.RUTIC_CHROMA_URL ?? "(unset)",
      },
    };
  }

  async probeEmbeddingAvailability(): Promise<MemoryEmbeddingProbeResult> {
    try {
      await chromaFetch("/api/v1/heartbeat");
      return { ok: true };
    } catch (err) {
      return { ok: false, error: String(err) };
    }
  }

  async probeVectorAvailability(): Promise<boolean> {
    const probe = await this.probeEmbeddingAvailability();
    return probe.ok;
  }
}

// ============================================================================
// SQL 수정: agent_memory PRIMARY KEY가 UUID이므로 ON CONFLICT (id) 사용 위해
// upsert의 id를 PK로 취급하도록 migrations에 추가 필요:
//   ALTER TABLE agent_memory ADD CONSTRAINT agent_memory_pkey_id UNIQUE (id);
// 현재 id는 UUID DEFAULT gen_random_uuid() — 호출 시 직접 id 제공
// ============================================================================
