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
    """`moment` as RFC 3339 in UTC, the way the engine sends it."""
    text = moment.astimezone(timezone.utc).isoformat()
    return text[: -len("+00:00")] + "Z" if text.endswith("+00:00") else text
