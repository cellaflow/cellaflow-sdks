# The three-minute quickstart

The example behind the quickstart on cellaflow.com. One file, no dependencies
beyond the SDK.

```bash
docker run -d --name cellaflow -p 50051:50051 -p 9090:9090 \
  ghcr.io/cellaflow/cellaflow:latest
pip install cellaflow

python research_agent.py                 # charges a card, then the pod dies
python research_agent.py <session-id>    # resumes -- does not charge again
cat charges.log                          # one line, after both runs
```

## What it shows

`charge_card` is a `@tool`, so it is leased: at most once per session. The first
run charges and then calls `os._exit(17)` — the worst moment for a pod to die,
because the money has moved and nothing durable records the receipt yet.

Resuming with the same session id replays `charge_card` from the durable log
instead of executing it. The receipt is built from the *original* charge.

## The detail worth noticing

The confirmation id is identical across both runs:

```
run 1   💳 CHARGED 2499 to ORD-1001 -> ch_2f198e0403
run 2   ✅ {'order_id': 'ORD-1001', 'confirmation': 'ch_2f198e0403'}
```

That is replay, not suppression. The resumed run receives the result the first
run committed — it does not skip the step and carry on with a hole where the
confirmation should be.

## Why the ledger file

`charges.log` is the adjudicator, not the script's own reporting. A step that
claims it did the right thing is not evidence; a file that only the charge path
appends to is. Same discipline as `examples/at_most_once_proof`.

## Things you can check yourself

- **Run it fresh again** — a *different* session charges again, as it should.
  Nothing is globally suppressed.
- **Resume the same id twice** — still one charge.
- **Kill the engine between the two runs** — the session is on disk; resuming
  after a restart works the same way.
