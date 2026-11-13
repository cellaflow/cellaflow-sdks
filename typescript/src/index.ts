export * from "./client.js";
export * from "./serialization.js";
export * from "./cellaflow/v1/common_pb.js";
export * from "./cellaflow/v1/idempotency_pb.js";
// The responses the client returns, with their schemas for `toJson` and
// `toBinary` from @bufbuild/protobuf. The rest of service_pb (the requests and
// the service descriptor) is the client's own business.
export {
  StartSessionResponseSchema,
  CommitStepResponseSchema,
} from "./cellaflow/v1/service_pb.js";
export type { StartSessionResponse, CommitStepResponse } from "./cellaflow/v1/service_pb.js";
// Note: internal_pb is intentionally NOT re-exported here. It contains
// engine-internal types (CacheRecord, LeaseRecord, etc.) that are not part
// of the public SDK surface.

// High-level API
export {
  IdempotencyScope,
  deriveIdempotencyKey,
  hashInputs,
  normalizeCoordinationId,
  MAX_COORDINATION_ID_BYTES,
} from "./idempotency.js";
export type { DeriveKeyOptions } from "./idempotency.js";
export { tool, step, DivergentStepError } from "./tool.js";
export type { ToolOptions, LeasedTool } from "./tool.js";
export { durableTools, toolSessionId, sessionCoordinationId } from "./durable.js";
export type { DurableToolsOptions, DurableSession, ThreadRef } from "./durable.js";
export {
  executionLease,
  taskLease,
  currentLease,
  LeaseHandle,
  LeaseNotAcquired,
  LeaseLostError,
} from "./execution.js";
export type { ExecutionLeaseOptions } from "./execution.js";
export type { LeaseEvent } from "./lease.js";
export { MIN_LEASE_TTL_MS } from "./lease.js";
export { WorkflowContext } from "./context.js";
