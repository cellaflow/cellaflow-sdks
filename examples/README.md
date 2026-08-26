# Examples

| Example | What it shows |
| --- | --- |
| [`multi_agent_idempotency/`](multi_agent_idempotency/) | Five replicas of one agent race on the same refund. One charge reaches the payment provider; the other four receive its result. |

Each directory has its own README. All of them need a running engine:

```bash
docker run -d --name cellaflow-demo \
  -p 50051:50051 -p 9090:9090 \
  -e CELLAFLOW_DB_PATH=/data/cellaflow \
  -e CELLAFLOW_HOST=:: \
  ghcr.io/cellaflow/cellaflow:latest
```

and the SDK: `pip install cellaflow`.
