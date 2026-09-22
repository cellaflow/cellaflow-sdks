import {
  createPromiseClient,
  PromiseClient,
  Transport,
} from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
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

    const transport: Transport = createGrpcTransport({
      baseUrl,
      httpVersion: "2",
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
   */
  async startSession(
    workflowId: string,
    version: string,
    sessionId?: string
  ): Promise<StartSessionResponse> {
    const req: PartialMessage<StartSessionRequest> = { workflowId, version };
    // Only set sessionId when truthy — matches Python's `if session_id:` guard.
    // Sending an empty string may trigger the engine's custom-ID validation path.
    if (sessionId) {
      req.sessionId = sessionId;
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
   */
  async commitStep(
    sessionId: string,
    sequence: number,
    name: string,
    status: StepStatus,
    outputPayload: Record<string, any>,
    idempotencyKey?: string,
    idempotencyFencingToken?: number
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
      timeoutMs:
        timeoutSecs !== undefined && timeoutSecs > 0
          ? timeoutSecs * 1000
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
          ? timeoutSecs * 1000
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
