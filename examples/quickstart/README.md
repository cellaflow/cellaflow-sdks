# The three-minute quickstart

The example behind the quickstart on cellaflow.com. One file, no dependencies
beyond the SDK.

```bash
docker run -d --name cellaflow -p 50051:50051 -p 9090:9090 \
  -e CELLAFLOW_ALLOW_UNAUTHENTICATED=true \
  ghcr.io/cellaflow/cellaflow:latest
pip install cellaflow

python checkout_agent.py                 # places an order, then the pod dies
python checkout_agent.py <session-id>    # resumes: does not place it again
cat orders.log                           # one line, after both runs
```

## What it shows

`place_order` is a `@tool`, so it is leased. The shop it calls takes no
idempotency key, like many order and booking endpoints, so calling it twice
would place two orders.

The first run places the order and then calls `os._exit(17)` before the receipt
is built. The order exists and Cellaflow has recorded it; the checkout is half
done.

Resuming with the same session id replays `place_order` from the durable log
instead of executing it. The receipt is built from the *original* order.

## The detail worth noticing

The confirmation id is identical across both runs:

```
run 1   🛒 PLACED ORD-1001 for 2499 -> ord_2f198e0403
run 2   ✅ {'order_id': 'ORD-1001', 'confirmation': 'ord_2f198e0403'}
```

That is replay, not suppression. The resumed run receives the result the first
run committed. It does not skip the step and carry on with a hole where the
confirmation should be.

## Why the ledger file

`orders.log` is the adjudicator, not the script's own reporting. A step that
claims it did the right thing is not evidence; a file that only the order path
appends to is. Same discipline as `examples/at_most_once_proof`.

It is **append-only**, and every line carries its session id:

```
8fc58c27-8253-44e4-83d4-bc971787b2b6 ord_02eb56524f ORD-1001 2499
```

That matters because **the engine outlives any single run.** A ledger that
cleared itself on each fresh run would disagree with the engine as soon as you
resumed an older session: it would have no line for a confirmation the engine
still holds.

The script reports the orders for *this* session separately from the file
total, so the two numbers can never imply a relationship they don't have.

## Resuming a session created somewhere else

Sessions live in the engine, not in this directory. If you resume one that was
created by a run elsewhere, the replay is real but the local ledger has no line
for it, and the script says so:

```
place_order did NOT run -- replayed from the durable log.
(No ledger line here for that session: it was placed by a run
 in another directory. The engine still has the record, which
 is why the confirmation above is the original one.)
this session: 0 order(s)
```

## If you paste the wrong session id

Nothing bad happens, and the script tells you:

```
⚠️  place_order DID run. That session id had no history on this
    engine, so this started a new run rather than resuming one.
    Use the id printed by your own first run.
```

`_session_id` is create-or-resume, so an unknown id starts a fresh session rather
than failing. That is correct engine behaviour, but it means the id has to be
one *your* engine issued, not one copied from documentation.

The script knows which happened because the counter lives inside the tool body:
a replayed step never enters its body, so the count stays at zero. It reports
what it observed rather than what it hoped.

## Things you can check yourself

- **Run it fresh again.** A *different* session places a new order, as it
  should. Nothing is globally suppressed.
- **Resume the same id twice.** Still one order.
- **Restart the engine between the two runs.** The session is on disk; resuming
  after a restart works the same way.
