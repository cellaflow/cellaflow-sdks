"""Reads a session's recorded steps from the engine, one page at a time."""

from collections.abc import Iterator
from typing import Any, Dict, Optional

import grpc

from cellaflow.client import CellaflowClient


#: How many steps to ask the engine for per page of a session's history.
GRAPH_PAGE = 1000


def iter_session_steps(
    client: CellaflowClient,
    session_id: str,
    from_sequence: Optional[int] = None,
    limit: Optional[int] = None,
    missing_ok: bool = True,
    timeout: Optional[float] = None,
) -> Iterator[Dict[str, Any]]:
    """Yields a session's steps in order, from `from_sequence` on.

    `get_graph` returns a bounded page and a cursor for the rest, so a single
    call sees only the beginning of a long session. The cursor is the sequence
    of the first unread record and is inclusive, so passing it straight back
    resumes exactly where the previous page stopped, and passing a sequence of
    our own starts the read there.

    A session the engine has never seen answers NOT_FOUND rather than an empty
    page, and that is a real answer: it means no history, which is what every
    first run looks like. Every *other* failure means the history could not be
    read, which is a different thing entirely and is allowed to propagate.
    Treating it as no history would start a run with history over from the
    beginning.

    `missing_ok=False` is for a caller that knows the session exists, such as one
    the engine just reported as recovered. NOT_FOUND then propagates too, rather
    than reading as no history and running every step again. `timeout` bounds
    each page's RPC.
    """
    # Passed only when given, so a client written against the older signature
    # still works.
    bound: Dict[str, Any] = {} if timeout is None else {"timeout": timeout}
    cursor = str(from_sequence) if from_sequence is not None else None
    yielded = False
    while True:
        try:
            steps, cursor = client.get_graph(
                session_id=session_id, limit=limit, cursor=cursor, **bound
            )
        except grpc.RpcError as exc:
            # NOT_FOUND partway through means the session went away while it was
            # being read, which is a failure rather than an empty session.
            if exc.code() == grpc.StatusCode.NOT_FOUND and missing_ok and not yielded:
                return
            raise

        for step in steps:
            yielded = True
            yield step
        if not cursor:
            return


def latest_session_sequence(
    client: CellaflowClient,
    session_id: str,
    from_sequence: Optional[int] = None,
    limit: Optional[int] = None,
    missing_ok: bool = True,
    timeout: Optional[float] = None,
) -> int:
    """The session's highest committed sequence at or after `from_sequence`.

    0 if there is none. A caller that already knows a lower bound passes it as
    `from_sequence`, so only the tail of the session is read. `missing_ok` is
    as for `iter_session_steps`: False for a session the engine just reported
    as recovered, which must not read as empty.
    """
    latest = 0
    steps = iter_session_steps(
        client, session_id, from_sequence, limit, missing_ok, timeout
    )
    for step in steps:
        latest = max(latest, int(step.get("sequence") or 0))
    return latest
