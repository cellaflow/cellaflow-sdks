import { durableTools, tool, IdempotencyScope } from "../dist/index.js";
let calls = 0;
const issueRefund = tool(
  async ({ order_id, amount }) => { calls++; return { refund_id: "RF-TS", amount }; },
  { toolName: "issue_refund", scope: IdempotencyScope.SHARED, sharedOn: ["order_id"] },
);
await durableTools("ts-thread-ticket-a86afb", { coordinationId: "ticket-a86afb" }, async () => {
  const r = await issueRefund({ order_id: "ORD-9", amount: 35 });
  console.log("  ts result     :", JSON.stringify(r));
});
console.log("  ts ran body   :", calls);
console.log(calls === 0
  ? "\n  CONVERGED: TypeScript took Python's committed result, refund issued once"
  : "\n  DIVERGED: both ran, keys did not match");
process.exit(calls === 0 ? 0 : 1);
