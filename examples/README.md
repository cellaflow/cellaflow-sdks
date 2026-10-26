# Examples

Four, in the order they are worth reading.

| Example | What it shows |
| --- | --- |
| [`quickstart/`](quickstart/) | One file. An order is placed, the process dies, and the resumed run is handed the original confirmation instead of placing the order again. |
| [`multi_agent_idempotency/`](multi_agent_idempotency/) | Five agents race to place the same customer order. One order reaches the shop; the others receive its confirmation. Five scenarios, including agents that disagree about the total and a LangGraph node killed after placing the order. |
| [`at_most_once_proof/`](at_most_once_proof/) | The same LangGraph graph run twice, identical apart from the lease. Arm A charges twice, arm B once. It exits non-zero unless it reproduces that. |
| [`crash_benchmark/`](crash_benchmark/) | Four ways to guard an irreversible tool call, measured against six failures. Two of them do not work, and the row where a lease does not help either is in the table. |

Each directory has its own README. All of them need a running engine:

```bash
docker run -d --name cellaflow-demo \
  -p 50051:50051 -p 9090:9090 \
  -e CELLAFLOW_ALLOW_UNAUTHENTICATED=true \
  -e CELLAFLOW_DB_PATH=/data/cellaflow \
  -e CELLAFLOW_HOST=:: \
  ghcr.io/cellaflow/cellaflow:latest
```

`CELLAFLOW_ALLOW_UNAUTHENTICATED` is not optional: the engine refuses to start
without either it or `CELLAFLOW_AUTH_TOKEN`, and the SDKs do not send a token.
Do not carry that setting into anything reachable.

and the SDK: `pip install cellaflow`. `at_most_once_proof` and `crash_benchmark`
pin their own dependencies in `requirements.txt` and run Postgres from the
compose file beside them.

Every example takes `CELLAFLOW_TARGET=host:port` to point at an engine that is
not on `localhost:50051`.
