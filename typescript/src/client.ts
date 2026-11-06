import {
  createPromiseClient,
  Interceptor,
  PromiseClient,
  Transport,
} from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { isIP } from "node:net";
import type { PartialMessage } from "@bufbuild/protobuf";
import { WorkflowEngineService } from "./cellaflow/v1/service_connect.js";
import type {
  StartSessionResponse,
  CommitStepResponse,
} from "./cellaflow/v1/service_pb.js";
import {
  StartSessionRequest,
  CommitStepRequest,
  GetGraphRequest,
} from "./cellaflow/v1/service_pb.js";
import { StepStatus } from "./cellaflow/v1/common_pb.js";
import type {
  CheckCacheResponse,
  RenewLeaseResponse,
  ReleaseLeaseResponse,
} from "./cellaflow/v1/idempotency_pb.js";
import {
  CheckCacheRequest,
  RenewLeaseRequest,
  ReleaseLeaseRequest,
} from "./cellaflow/v1/idempotency_pb.js";
import { serialize, deserialize } from "./serialization.js";

export interface CellaflowClientOptions {
  target?: string;
  secure?: boolean;
  /**
   * Authenticates every call, sent as `authorization: Bearer <apiKey>`. When it
   * is not given, it is read from `CELLAFLOW_AUTH_TOKEN`, then `CELLAFLOW_API_KEY`,
   * the engine's own order; with neither set, calls go unauthenticated, which an
   * engine started without a token accepts. A blank string means no key, even
   * with the variables set.
   */
  apiKey?: string;
}

/**
 * Where an API key is read from when none is passed, in the engine's own order,
 * so the SDK sends the token the engine expects when both are set. The engine
 * also falls back to the generic `AUTH_TOKEN` and `API_KEY`; the SDK does not,
 * because a variable that generic often holds another service's secret, which
 * would then be sent to the engine.
 */
export const API_KEY_ENV_VARS = ["CELLAFLOW_AUTH_TOKEN", "CELLAFLOW_API_KEY"] as const;

/**
 * The key to send and where it came from. Exactly the engine's rule: the first
 * variable that is set decides, even if it is blank, and a blank value means no
 * key. Skipping a blank one to read the next would send a key to an engine that,
 * reading the same environment, has none.
 */
function apiKeyAndSource(apiKey?: string): [string | undefined, string] {
  if (apiKey !== undefined) return [apiKey.trim() ? apiKey : undefined, "the apiKey option"];
  for (const name of API_KEY_ENV_VARS) {
    const value = process.env[name];
    if (value !== undefined) return [value.trim() ? value : undefined, name];
  }
  return [undefined, ""];
}

/**
 * Refuses a key that cannot be sent as a header, naming where it came from but
 * never what it is. The same rule as the Python SDK's `_check_api_key`.
 */
function checkApiKey(apiKey: string, source: string): void {
  if (apiKey !== apiKey.trim() || !/^[\x20-\x7e]*$/.test(apiKey)) {
    throw new Error(
      `The Cellaflow API key from ${source} cannot be sent in a header: it has ` +
        "surrounding whitespace or a character outside printable ASCII (a " +
        "trailing newline from a secret file, say).",
    );
  }
}

/** The API key to send: `apiKey` if given, else the first variable that is set. */
export function resolveApiKey(apiKey?: string): string | undefined {
  return apiKeyAndSource(apiKey)[0];
}

const warnedPlaintext = new Set<string>();

/** Whether `host` is this machine: `localhost`, any of 127.0.0.0/8, or `::1`. */
function hostIsLocal(host: string): boolean {
  const name = host.toLowerCase().replace(/\.+$/, "");
  if (name === "localhost") return true;
  if (isIP(name) === 4) return name.startsWith("127.");
  // The URL parser normalises every spelling of the IPv6 loopback to `[::1]`.
  if (isIP(name) === 6) return new URL(`http://[${name}]`).hostname === "[::1]";
  return false;
}

/** Whether `target` (`host:port` or `[v6]:port`) names this machine. */
function isLocal(target: string): boolean {
  let host: string;
  if (target.startsWith("[")) {
    const end = target.indexOf("]");
    host = end === -1 ? target : target.slice(1, end);
  } else if ((target.match(/:/g) ?? []).length > 1) {
    host = target; // a bare IPv6 address, with no port
  } else {
    host = target.includes(":") ? target.slice(0, target.lastIndexOf(":")) : target;
  }
  return hostIsLocal(host);
}

function warnIfSentInTheClear(target: string): void {
  if (isLocal(target) || warnedPlaintext.has(target)) return;
  warnedPlaintext.add(target);
  console.warn(
    `Cellaflow: sending an API key to ${target} without TLS, so anyone on the path ` +
      "can read it. Pass secure: true to send it encrypted.",
  );
}

/** Adds `authorization: Bearer <key>` to every call, so no call site can forget it. */
function bearerKey(apiKey: string): Interceptor {
  const header = `Bearer ${apiKey}`;
  return (next) => async (req) => {
    req.header.set("authorization", header);
    return next(req);
  };
}

export class CellaflowClient {
  /**
   * gRPC Client for the Cellaflow Engine (Connect-ES transport).
   * Handles communication with the engine and strictly uses MessagePack
   * for state payloads.
   */
  private client: PromiseClient<typeof WorkflowEngineService>;

  constructor(options: CellaflowClientOptions = {}) {
    const { target = "localhost:50051", secure = false } = options;

    // Use http/https based on the secure flag
    const baseUrl = secure ? `https://${target}` : `http://${target}`;

    const [apiKey, source] = apiKeyAndSource(options.apiKey);
    const interceptors: Interceptor[] = [];
    if (apiKey !== undefined) {
      checkApiKey(apiKey, source);
      if (!secure) warnIfSentInTheClear(target);
      interceptors.push(bearerKey(apiKey));
    }

    const transport: Transport = createGrpcTransport({
      baseUrl,
      httpVersion: "2",
      interceptors,
    });

    this.client = createPromiseClient(WorkflowEngineService, transport);
  }

  /**
   * Starts a new workflow execution session.
   *
   * @param workflowId - The workflow definition ID.
   * @param version - The workflow version string.
   * @param sessionId - Optional client-proposed session ID. When provided, the
   *   engine performs a transactional check-and-insert to prevent concurrent
   *   race conditions. Omit to let the engine assign one.
   * @param coordinationId - Optional default coordination id for the session.
   *   The engine stores it and records every step under it unless the step
   *   names its own. On resume, omitting it adopts the stored one, which the
   *   response returns; naming a different one is refused.
   */
  async startSession(
    workflowId: string,
    version: string,
    sessionId?: string,
    coordinationId?: string
  ): Promise<StartSessionResponse> {
    const req: PartialMessage<StartSessionRequest> = { workflowId, version };
    // Only set sessionId when truthy, matching Python's `if session_id:` guard.
    // Sending an empty string may trigger the engine's custom-ID validation path.
    if (sessionId) {
      req.sessionId = sessionId;
    }
    if (coordinationId) {
      req.coordinationId = coordinationId;
    }
    return await this.client.startSession(req);
  }

  /**
   * Commits a completed step result to the session graph.
   *
   * @param sessionId - The session to commit to.
   * @param sequence - The step sequence number (1-based).
   * @param name - Human-readable step name.
   * @param status - The step outcome status.
   * @param outputPayload - Arbitrary step output. Serialized as MessagePack.
   * @param idempotencyKey - Optional idempotency lease key.
   * @param idempotencyFencingToken - Required when `idempotencyKey` is set.
   * @param coordinationId - Optional coordination id for this step alone.
   *   Omitted, the engine records the step under the session's default.
   */
  async commitStep(
    sessionId: string,
    sequence: number,
    name: string,
    status: StepStatus,
    outputPayload: Record<string, any>,
    idempotencyKey?: string,
    idempotencyFencingToken?: number,
    coordinationId?: string
  ): Promise<CommitStepResponse> {
    // Strictly serialize object to MessagePack
    const serializedState = serialize(outputPayload);

    const req: PartialMessage<CommitStepRequest> = {
      sessionId,
      stepResult: {
        sequence: BigInt(sequence),
        name,
        status,
        outputPayload: Buffer.from(serializedState) as unknown as Uint8Array<ArrayBuffer>,
      },
    };
    if (coordinationId) {
      req.stepResult!.coordinationId = coordinationId;
    }

    if (idempotencyKey !== undefined) {
      req.idempotencyKey = idempotencyKey;
      if (idempotencyFencingToken === undefined) {
        throw new Error(
          "idempotencyFencingToken required if idempotencyKey is set"
        );
      }
      req.idempotencyFencingToken = BigInt(idempotencyFencingToken);
    }

    return await this.client.commitStep(req);
  }

  /**
   * Returns a paginated list of committed step results for a session.
   *
   * @returns A tuple of (step results, next_cursor). `next_cursor` is
   *   `undefined` when there are no more pages. Each step result's
   *   `outputPayload` is fully deserialized from MessagePack.
   */
  async getGraph(
    sessionId: string,
    limit?: number,
    cursor?: string
  ): Promise<[Record<string, any>[], string | undefined]> {
    const req: PartialMessage<GetGraphRequest> = { sessionId };
    if (limit !== undefined) {
      req.limit = limit;
    }
    if (cursor !== undefined) {
      req.cursor = cursor;
    }

    const resp = await this.client.getGraph(req);

    const results = resp.steps.map((step) => ({
      sequence: step.sequence,
      name: step.name,
      status: step.status,
      // Guard against empty payload (default Uint8Array(0)) to prevent
      // msgpack from throwing on an empty buffer.
      outputPayload:
        step.outputPayload.length > 0 ? deserialize(step.outputPayload) : {},
      idempotencyKey: step.idempotencyKey,
      // The coordination id the engine recorded the step under, or undefined
      // when it has none.
      coordinationId: step.coordinationId,
    }));

    const nextCursor = resp.nextCursor ? resp.nextCursor : undefined;
    return [results, nextCursor];
  }

  /**
   * Arbitrates the idempotency lease for `idempotencyKey`.
   *
   * Supplying `sessionId` also asks the engine for the session's committed
   * position, returned as `currentSequence` on every status. The idempotency
   * key is opaque to the engine, so the session cannot be inferred from it —
   * without this the engine has nothing to answer from.
   *
   * Supplying `sequence` — the position this caller intends to write to —
   * additionally lets the engine refuse a lease that would authorise a side
   * effect at an already-committed position, instead of rejecting the commit
   * afterwards once the side effect has happened. The engine raises
   * `FAILED_PRECONDITION` when refused. Both fields are optional on the wire;
   * omitting `sequence` keeps the position unguarded.
   */
  async checkIdempotencyCache(
    agentId: string,
    idempotencyKey: string,
    waitTimeoutMs?: number,
    leaseTtlMs?: number,
    sessionId?: string,
    sequence?: number
  ): Promise<CheckCacheResponse> {
    const req: PartialMessage<CheckCacheRequest> = { agentId, idempotencyKey };
    // These proto fields are uint64 → bigint; convert from the JS number API.
    if (waitTimeoutMs !== undefined) {
      req.waitTimeoutMs = BigInt(waitTimeoutMs);
    }
    if (leaseTtlMs !== undefined) {
      req.leaseTtlMs = BigInt(leaseTtlMs);
    }
    if (sessionId !== undefined) {
      req.sessionId = sessionId;
    }
    if (sequence !== undefined) {
      req.sequence = BigInt(sequence);
    }

    return await this.client.checkIdempotencyCache(req);
  }

  /**
   * Renews a held idempotency lease.
   *
   * @param timeoutSecs - Bounds the RPC in **seconds**. Heartbeat callers
   *   MUST supply a positive value: without a deadline a black-holed
   *   connection parks the calling thread indefinitely, and the shutdown
   *   path that joins that thread parks with it.
   */
  async renewLease(
    agentId: string,
    idempotencyKey: string,
    fencingToken: number,
    extendMs: number,
    timeoutSecs?: number
  ): Promise<RenewLeaseResponse> {
    const req: PartialMessage<RenewLeaseRequest> = {
      agentId,
      idempotencyKey,
      fencingToken: BigInt(fencingToken),
      extendMs: BigInt(extendMs),
    };

    return await this.client.renewLease(req, {
      // Guard against timeout=0 which would cause an instant timeout.
      // Whole milliseconds: the grpc-timeout header carries an integer, and a
      // fractional value is dropped by the server rather than rounded.
      timeoutMs:
        timeoutSecs !== undefined && timeoutSecs > 0
          ? Math.max(1, Math.round(timeoutSecs * 1000))
          : undefined,
    });
  }

  /**
   * Releases a held idempotency lease.
   *
   * @param timeoutSecs - Bounds the RPC in **seconds**.
   */
  async releaseLease(
    agentId: string,
    idempotencyKey: string,
    fencingToken: number,
    reason?: string,
    timeoutSecs?: number
  ): Promise<ReleaseLeaseResponse> {
    const req: PartialMessage<ReleaseLeaseRequest> = {
      agentId,
      idempotencyKey,
      fencingToken: BigInt(fencingToken),
    };
    if (reason !== undefined) {
      req.reason = reason;
    }

    return await this.client.releaseLease(req, {
      timeoutMs:
        timeoutSecs !== undefined && timeoutSecs > 0
          ? Math.max(1, Math.round(timeoutSecs * 1000))
          : undefined,
    });
  }

  /**
   * Provided for API compatibility with the Python client.
   *
   * Connect-ES v1 with `createGrpcTransport` does not expose an explicit
   * close/shutdown method — HTTP/2 sessions are managed by Node.js and are
   * cleaned up on process exit or when the client is garbage-collected.
   * Long-lived server processes with many short-lived clients should let the
   * garbage collector handle cleanup.
   */
  close(): void {
    // No-op: Connect-ES v1 does not provide a transport.close() API.
  }
}
