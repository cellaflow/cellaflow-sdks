"""What the engine says when the organization is over its allowance.

A hosted Cellaflow engine refuses new work from an organization that has used
up its allowance (its free steps, say) until the allowance resets. It answers
RESOURCE_EXHAUSTED, the code it also answers for a full session mailbox, which
passes on its own and is worth asking again. What tells the two apart is in the
status details the engine sends in the `grpc-status-details-bin` trailer: a
`google.rpc.Status` whose details include a `google.rpc.QuotaFailure` with a
violation whose subject starts with `cellaflow.` (`cellaflow.steps` today). It
may also carry a `google.rpc.ErrorInfo` from domain `cellaflow.com` with reason
`ALLOWANCE_EXCEEDED`, saying when the allowance resets and what is blocked until
then. The TypeScript SDK reads the same details the same way.

`CellaflowClient` raises such a refusal as `AllowanceExceeded`, and nothing in
the SDK sends the request again: until the allowance resets, the engine gives
the same answer.

The details are decoded with message classes built here, from the few fields
this needs, in a descriptor pool of this module's own. Nothing is added to
protobuf's default pool, so an application that also imports the `google.rpc`
modules of `googleapis-common-protos` does not find them defined twice, and the
package needs nothing beyond protobuf to read them.
"""

import builtins
import collections
import functools
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

import grpc
from google.protobuf import any_pb2, descriptor_pb2, descriptor_pool, message_factory

#: The trailer gRPC carries a call's `google.rpc.Status` in.
STATUS_DETAILS_KEY = "grpc-status-details-bin"

#: What starts the subject of a violation that is about a Cellaflow allowance.
_SUBJECT_PREFIX = "cellaflow."
#: The `ErrorInfo` that says when the allowance resets, and what is blocked.
_DOMAIN = "cellaflow.com"
_REASON = "ALLOWANCE_EXCEEDED"


class AllowanceExceeded(grpc.RpcError):  # type: ignore[misc]
    """The engine refused a request because the organization is over its
    allowance.

    Final: nothing in the SDK sends the request again, because the engine gives
    the same answer until the allowance resets. A step that raises it is not
    recorded as failed, so calling the step again once the allowance resets, or
    running the workflow again, runs it.

    Still a `grpc.RpcError`, so code that catches those catches it too: `code()`
    is RESOURCE_EXHAUSTED, `details()` the engine's message, and
    `trailing_metadata()` what the engine sent. The error the call failed with
    is its `__cause__`.

    Attributes
    ----------
    subject:
        What the allowance is for, such as ``cellaflow.steps``.
    resets_at:
        When the allowance resets, as a timezone-aware datetime in UTC, or
        ``None`` when the engine did not say.
    limit:
        What is blocked until then, such as ``new_sessions_blocked`` or
        ``writes_blocked``, or ``None`` when the engine did not say.
    """

    def __init__(
        self,
        details: str = "",
        *,
        subject: str = "",
        resets_at: Optional[datetime] = None,
        limit: Optional[str] = None,
        trailing_metadata: Any = (),
    ) -> None:
        super().__init__(details)
        self._details = details
        self.subject = subject
        self.resets_at = resets_at
        self.limit = limit
        self._trailing_metadata = tuple(trailing_metadata or ())

    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.RESOURCE_EXHAUSTED

    def details(self) -> str:
        return self._details

    def trailing_metadata(self) -> Tuple[Any, ...]:
        return self._trailing_metadata

    def debug_error_string(self) -> str:
        return f"RESOURCE_EXHAUSTED: {self._details}"

    def __str__(self) -> str:
        # Worded as the TypeScript SDK's is.
        what = f" for {self.subject}" if self.subject else ""
        text = (
            f"The Cellaflow org is over its allowance{what}, so the engine "
            f"refused this call."
        )
        if self.resets_at is not None:
            text += f" The allowance resets at {_rfc3339(self.resets_at)}."
        if self._details:
            text += f" The engine said: {self._details}"
        return text

    def __reduce__(self) -> Any:
        # Pickled without the call it came from, which pickle leaves out with
        # the rest of `__cause__`, and with the trailer as plain pairs: gRPC's
        # own metadata type cannot be pickled.
        state = dict(self.__dict__)
        state["_trailing_metadata"] = tuple(
            (key, value) for key, value in self._trailing_metadata
        )
        return (type(self), self.args, state)


def _refusal_from(error: BaseException) -> Optional[AllowanceExceeded]:
    """`error` as `AllowanceExceeded`, if it is the engine refusing a request
    because the organization is over its allowance, or None.

    Never raises. Anything it cannot read (no trailer, bytes that do not decode,
    no violation about a Cellaflow allowance) is not an allowance refusal, and
    the error stays what it was. An `ErrorInfo` it cannot use only leaves
    `resets_at` and `limit` unknown.
    """
    if isinstance(error, AllowanceExceeded):
        return error
    if not isinstance(error, grpc.RpcError):
        return None
    try:
        if error.code() != grpc.StatusCode.RESOURCE_EXHAUSTED:
            return None
        trailer = tuple(error.trailing_metadata() or ())
        found = _read_status(_status_bytes(trailer))
        if found is None:
            return None
        subject, resets_at, limit = found
        details = error.details()
        refusal = AllowanceExceeded(
            details if isinstance(details, str) else "",
            subject=subject,
            resets_at=resets_at,
            limit=limit,
            trailing_metadata=trailer,
        )
    except Exception:
        return None
    refusal.__cause__ = error
    return refusal


def _is_allowance_refusal(error: BaseException) -> bool:
    """Whether `error` is the engine refusing a request because the org is over
    its allowance: `AllowanceExceeded`, or the gRPC error it is made from, as a
    client other than `CellaflowClient` raises it (a stub of your own, or a test
    double). Every decision about a refusal asks this, so it holds however the
    refusal arrived."""
    return _refusal_from(error) is not None


#: How many explicit causes, or levels of exception groups, `_carries_refusal`
#: follows looking for a refusal.
_MAX_REFUSAL_DEPTH = 8
#: How many errors it looks at, at most, and at most how many members of any one
#: group, so a wide group costs a bounded amount.
_MAX_REFUSAL_ERRORS = 64

#: Python 3.11's exception groups, or None before them.
_BaseExceptionGroup: Any = getattr(builtins, "BaseExceptionGroup", None)


def _carries_refusal(error: BaseException) -> bool:
    """Whether `error` is an allowance refusal, or carries one: in its chain of
    explicit causes (`raise MyError(...) from refusal`), up to
    `_MAX_REFUSAL_DEPTH` links, or among the members of an exception group, at
    any level within that depth. Searched breadth first, looking at no more
    than `_MAX_REFUSAL_ERRORS` errors, with the same bounds as the TypeScript
    SDK's `carriesAllowanceRefusal`.

    Only `__cause__` is followed, not `__context__`: an exception merely raised
    while a refusal was being handled is not made from it. A cycle ends the
    search, and an attribute that raises hides what is behind it. Never
    raises."""
    seen: "set[int]" = set()
    queue: "collections.deque[Tuple[Any, int]]" = collections.deque([(error, 0)])
    try:
        while queue and len(seen) < _MAX_REFUSAL_ERRORS:
            value, depth = queue.popleft()
            # Every error queued is reachable from `error`, so ids stay unique.
            if not isinstance(value, BaseException) or id(value) in seen:
                continue
            seen.add(id(value))
            if _is_allowance_refusal(value):
                return True
            if depth >= _MAX_REFUSAL_DEPTH:
                continue
            try:
                queue.append((value.__cause__, depth + 1))
                if _BaseExceptionGroup is not None and isinstance(
                    value, _BaseExceptionGroup
                ):
                    for member in value.exceptions[:_MAX_REFUSAL_ERRORS]:
                        queue.append((member, depth + 1))
            except Exception:
                continue
    except Exception:
        return False
    return False


def _status_bytes(trailer: Tuple[Any, ...]) -> Optional[bytes]:
    """The serialized `google.rpc.Status` in a call's trailer, if it has one."""
    for key, value in trailer:
        if key == STATUS_DETAILS_KEY and isinstance(value, (bytes, bytearray)):
            return bytes(value)
    return None


def _read_status(
    raw: Optional[bytes],
) -> Optional[Tuple[str, Optional[datetime], Optional[str]]]:
    """The subject, reset time and limit of an allowance refusal, from its
    serialized `google.rpc.Status`; None if it is not one.

    A detail is matched by the type name after the last `/` of its type URL, as
    protobuf's own `Any.Is` does, so any host prefix matches. The first
    violation about a Cellaflow allowance counts, and the first `ErrorInfo` from
    Cellaflow about the allowance. A detail that does not decode is skipped.
    """
    if raw is None:
        return None
    status_class, quota_failure_class, error_info_class = _messages()
    status = status_class.FromString(raw)
    subject: Optional[str] = None
    info: Any = None
    for detail in status.details:
        name = detail.type_url.rpartition("/")[2]
        try:
            if name == "google.rpc.QuotaFailure" and subject is None:
                quota = quota_failure_class.FromString(detail.value)
                subject = next(
                    (
                        v.subject
                        for v in quota.violations
                        if v.subject.startswith(_SUBJECT_PREFIX)
                    ),
                    None,
                )
            elif name == "google.rpc.ErrorInfo" and info is None:
                candidate = error_info_class.FromString(detail.value)
                if candidate.domain == _DOMAIN and candidate.reason == _REASON:
                    info = candidate
        except Exception:
            continue
    if subject is None:
        return None
    resets_at: Optional[datetime] = None
    limit: Optional[str] = None
    if info is not None:
        resets_at = _parse_rfc3339(info.metadata.get("resets_at", ""))
        limit = info.metadata.get("limit") or None
    return subject, resets_at, limit


@functools.lru_cache(maxsize=None)
def _messages() -> Tuple[Any, Any, Any]:
    """The `google.rpc` Status, QuotaFailure and ErrorInfo message classes,
    built once, in a pool of this module's own.

    Each declares only the fields read here, by the numbers `google/rpc` gives
    them; a field it does not declare is kept as unknown and ignored, so the
    engine can send the full messages. `google/protobuf/any.proto` is copied in
    from protobuf's own, which every Status detail is.
    """
    pool = descriptor_pool.DescriptorPool()
    any_file = descriptor_pb2.FileDescriptorProto()
    any_pb2.DESCRIPTOR.CopyToProto(any_file)
    for file in (any_file, _status_file(), _error_details_file()):
        pool.AddSerializedFile(file.SerializeToString())
    return (
        message_factory.GetMessageClass(
            pool.FindMessageTypeByName("google.rpc.Status")
        ),
        message_factory.GetMessageClass(
            pool.FindMessageTypeByName("google.rpc.QuotaFailure")
        ),
        message_factory.GetMessageClass(
            pool.FindMessageTypeByName("google.rpc.ErrorInfo")
        ),
    )


_Field = descriptor_pb2.FieldDescriptorProto


def _add_field(
    message: descriptor_pb2.DescriptorProto,
    name: str,
    number: int,
    kind: "descriptor_pb2.FieldDescriptorProto.Type.ValueType" = _Field.TYPE_STRING,
    repeats: str = "",
) -> None:
    """Adds a field holding one value of `kind`, or, when `repeats` names a
    message, a repeated field of that message."""
    field = message.field.add(name=name, number=number)
    if repeats:
        field.label = _Field.LABEL_REPEATED
        field.type = _Field.TYPE_MESSAGE
        field.type_name = repeats
    else:
        field.label = _Field.LABEL_OPTIONAL
        field.type = kind


def _status_file() -> descriptor_pb2.FileDescriptorProto:
    """`google/rpc/status.proto`: Status { code = 1; message = 2; details = 3 }."""
    file = descriptor_pb2.FileDescriptorProto(
        name="google/rpc/status.proto",
        package="google.rpc",
        syntax="proto3",
        dependency=["google/protobuf/any.proto"],
    )
    status = file.message_type.add(name="Status")
    _add_field(status, "code", 1, _Field.TYPE_INT32)
    _add_field(status, "message", 2)
    _add_field(status, "details", 3, repeats=".google.protobuf.Any")
    return file


def _error_details_file() -> descriptor_pb2.FileDescriptorProto:
    """The two messages of `google/rpc/error_details.proto` read here:
    ErrorInfo { reason = 1; domain = 2; map<string, string> metadata = 3 } and
    QuotaFailure { repeated Violation violations = 1 }, where
    Violation { subject = 1; description = 2 }."""
    file = descriptor_pb2.FileDescriptorProto(
        name="google/rpc/error_details.proto", package="google.rpc", syntax="proto3"
    )
    info = file.message_type.add(name="ErrorInfo")
    _add_field(info, "reason", 1)
    _add_field(info, "domain", 2)
    entry = info.nested_type.add(name="MetadataEntry")
    entry.options.map_entry = True
    _add_field(entry, "key", 1)
    _add_field(entry, "value", 2)
    _add_field(info, "metadata", 3, repeats=".google.rpc.ErrorInfo.MetadataEntry")
    quota = file.message_type.add(name="QuotaFailure")
    violation = quota.nested_type.add(name="Violation")
    _add_field(violation, "subject", 1)
    _add_field(violation, "description", 2)
    _add_field(quota, "violations", 1, repeats=".google.rpc.QuotaFailure.Violation")
    return file


#: An RFC 3339 date-time: `2026-12-01T00:00:00Z`, with any fraction of a second
#: and any offset. ASCII digits only.
_RFC3339 = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt]([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]+))?(?:[Zz]|([+-])([0-9]{2}):([0-9]{2}))"
)


def _parse_rfc3339(text: str) -> Optional[datetime]:
    """An RFC 3339 date-time as a timezone-aware datetime in UTC, or None if it
    is not one (a leap second included, which datetime cannot hold). Written
    out rather than `datetime.fromisoformat`, which before Python 3.11 takes
    neither `Z` nor a fraction of any length but 3 or 6 digits."""
    match = _RFC3339.fullmatch(text)
    if match is None:
        return None
    year, month, day, hour, minute, second, fraction, sign, off_h, off_m = (
        match.groups()
    )
    if int(year) < 100:
        # Refused, as the TypeScript SDK refuses it: JavaScript's dates read a
        # year below 100 as 19xx.
        return None
    offset = timedelta(0)
    if sign is not None:
        if int(off_h) > 23 or int(off_m) > 59:
            return None
        offset = timedelta(hours=int(off_h), minutes=int(off_m))
        if sign == "-":
            offset = -offset
    try:
        moment = datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            int(second),
            int((fraction or "0")[:6].ljust(6, "0")),
            tzinfo=timezone(offset),
        )
        return moment.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _rfc3339(moment: datetime) -> str:
    """`moment` in UTC as the TypeScript SDK prints it: `2026-12-01T00:00:00Z`,
    with milliseconds (`.500Z`) unless it falls on a whole second. Finer
    precision is left out of the message, as JavaScript's dates cannot hold it,
    but `resets_at` keeps it."""
    utc = moment.astimezone(timezone.utc)
    millis = utc.microsecond // 1000
    fraction = f".{millis:03d}" if millis else ""
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T"
        f"{utc.hour:02d}:{utc.minute:02d}:{utc.second:02d}{fraction}Z"
    )
