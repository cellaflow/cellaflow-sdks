"""Records a step's failure, and raises the same failure again on replay.

A workflow that catches a step's exception and carries on takes a branch that
depends on that exception. A resumed run must take the same branch, so it must
raise the same exception at the same position instead of running the step again.
The failure is recorded as data, and rebuilt here into an instance of the same
class, so `except` clauses, `isinstance` checks and attribute reads behave as
they did the first time.

Rebuilding runs no code the recorded data can choose. The class is looked up only
among modules the process has already loaded; nothing is imported. The instance
is made by Python's own exception machinery: the nearest built-in `__new__`, and
the built-in `__init__` only where the class does not define its own, both of
which only store fields. The class's own `__new__` and `__init__` never run.
Recorded attributes are written into the instance's `__dict__`, bypassing any
property or `__setattr__` the class defines. This keeps the MessagePack-only rule
for step payloads: the record is plain data, and reading it back cannot run
arbitrary code the way unpickling can.

What a rebuild needs is the class: defined at module level, in a module the
resumed run has loaded, as it has when the workflow's `except` clause names it.
Values that cannot be recorded, such as an HTTP response object, read as an
error naming them on replay rather than quietly as something else.

An engine call's gRPC error is recorded as its status code and details, and comes
back as `ReplayedRpcError`, a `grpc.RpcError` whose `code()` and `details()`
return them: the transport's own error class holds live channel state that cannot
be recorded.
"""

import builtins
import enum
import inspect
import sys
import traceback
import types
from typing import Any, Dict, List, Optional, Tuple

import grpc

from cellaflow.serialization import serialize

#: Format of the recorded failure, so a later SDK can tell what it is reading.
FAILURE_FORMAT = 1

#: Marks a recorded tuple, which MessagePack would otherwise turn into a list.
_TUPLE = "__cellaflow_tuple__"


class UnavailableOnReplay:
    """Stands in for an exception argument that could not be recorded."""

    def __init__(self, what: str) -> None:
        self._what = what

    def __repr__(self) -> str:
        return f"<{self._what}: not recorded, so not available on replay>"

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(
            f"{self._what} was not recorded, so it is not available on replay"
        )


class ReplayedFailureError(RuntimeError):
    """A recorded failure that cannot be raised again as the same exception."""


class ReplayedRpcError(grpc.RpcError):  # type: ignore[misc]
    """An engine call's gRPC error, as recorded and raised again on replay."""

    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        super().__init__(f"{code.name}: {details}")
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def trailing_metadata(self) -> Tuple[Any, ...]:
        return ()

    def debug_error_string(self) -> str:
        return f"replayed {self._code.name}: {self._details}"


# --- Encoding values ----------------------------------------------------------


def _encode(value: Any) -> Tuple[bool, Any]:
    """`value` as plain data the serializer accepts, if it can be.

    Enums and subclasses of the plain types are stored as their plain value, so
    `e.status == 404` holds on replay for an `HTTPStatus.NOT_FOUND`, though the
    value is no longer the enum member. Tuples keep a marker so they come back
    as tuples.
    """
    if value is None or type(value) in (bool, int, float, str, bytes):
        return True, value
    if isinstance(value, enum.Enum):
        return _encode(value.value)
    if isinstance(value, bool):
        return True, bool(value)
    if isinstance(value, int):
        return True, int(value)
    if isinstance(value, float):
        return True, float(value)
    if isinstance(value, str):
        return True, str(value)
    if isinstance(value, bytes):
        return True, bytes(value)
    if isinstance(value, (list, tuple)):
        items = []
        for item in value:
            ok, encoded = _encode(item)
            if not ok:
                return False, None
            items.append(encoded)
        return True, ({_TUPLE: items} if isinstance(value, tuple) else items)
    if isinstance(value, dict):
        out: Dict[Any, Any] = {}
        for key, item in value.items():
            ok_key, encoded_key = _encode(key)
            ok, encoded = _encode(item)
            if not (ok_key and ok) or not isinstance(encoded_key, (str, int)):
                return False, None
            out[encoded_key] = encoded
        return True, out
    return False, None


def _decode(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode(item) for item in value]
    if isinstance(value, dict):
        if set(value) == {_TUPLE}:
            return tuple(_decode(item) for item in value[_TUPLE])
        return {key: _decode(item) for key, item in value.items()}
    return value


# --- Capturing ----------------------------------------------------------------


def type_location(cls: type) -> str:
    return f"{cls.__module__}:{cls.__qualname__}"


def _resolve(location: str) -> Any:
    """The object at `location`, among modules already loaded. Imports nothing."""
    module_name, _, qualname = location.partition(":")
    obj: Any = sys.modules.get(module_name)
    if obj is None or not qualname:
        return None
    for part in qualname.split("."):
        obj = inspect.getattr_static(obj, part, None)
        if obj is None:
            return None
    return obj


def findable(cls: type) -> bool:
    """Whether `cls` can be found again by its recorded location."""
    return _resolve(type_location(cls)) is cls


#: Bounds on a recorded failure, so a record always fits in a commit. A value over
#: `_MAX_VALUE_BYTES` is not recorded (reading it on replay raises), the traceback
#: keeps its last `_MAX_TRACEBACK_CHARS`, and a record still over
#: `_MAX_RECORD_BYTES` keeps only its type and message.
_MAX_VALUE_BYTES = 64 * 1024
_MAX_TRACEBACK_CHARS = 16 * 1024
_MAX_MESSAGE_CHARS = 4 * 1024
_MAX_RECORD_BYTES = 512 * 1024
#: How deep and how wide an exception group is recorded.
_MAX_GROUP_DEPTH = 5
_MAX_GROUP_MEMBERS = 50

_BaseExceptionGroup: Any = getattr(builtins, "BaseExceptionGroup", None)


def _safe_encode(value: Any) -> Tuple[bool, Any]:
    """`_encode`, treating a value it cannot walk (too deep, or self-referencing)
    as one that cannot be recorded."""
    try:
        return _encode(value)
    except RecursionError:
        return False, None


def _small_enough(encoded: Any) -> bool:
    try:
        return len(serialize({"v": encoded})) <= _MAX_VALUE_BYTES
    except Exception:
        return False


def _builtin_fields(exc: BaseException) -> Dict[str, Any]:
    """Fields Python's own exception classes keep outside `args` and `__dict__`.

    `OSError.errno`, `ImportError.name`, `StopIteration.value` and the like. An
    exception whose class defines its own `__init__` sets them there, and the
    rebuild never runs that `__init__`, so they are recorded and set directly.
    """
    fields: Dict[str, Any] = {}
    for klass in type(exc).__mro__:
        if klass.__module__ != "builtins":
            continue
        for name, descriptor in vars(klass).items():
            if name.startswith("__") or name == "args":
                continue
            builtin_field = (types.GetSetDescriptorType, types.MemberDescriptorType)
            if not isinstance(descriptor, builtin_field):
                continue
            try:
                value = getattr(exc, name)
            except Exception:
                continue
            if value is not None:
                fields.setdefault(name, value)
    return fields


def capture(exc: BaseException, _depth: int = 0) -> Dict[str, Any]:
    """The recorded form of `exc`. `unavailable_args` and `unavailable_attrs`
    name what could not be recorded.

    Never raises: a failure is recorded while the step's own exception is on its
    way to the workflow, and nothing here may replace it. Whatever cannot be read
    is recorded as unavailable.
    """
    try:
        return _capture(exc, _depth)
    except Exception as error:  # an exception that cannot even be inspected
        return {
            "format": FAILURE_FORMAT,
            "sdk": "python",
            "type": type_location(type(exc)),
            "message": f"<not recorded: {type(error).__name__}>",
            "args": [],
            "unavailable_args": [],
            "attrs": {},
            "unavailable_attrs": [],
            "traceback": "",
        }


def _capture(exc: BaseException, depth: int) -> Dict[str, Any]:
    try:
        message = str(exc)[:_MAX_MESSAGE_CHARS]
    except Exception:
        message = f"<{type(exc).__name__}: str() raised>"
    try:
        trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    except Exception:
        trace = ""
    recorded: Dict[str, Any] = {
        "format": FAILURE_FORMAT,
        "sdk": "python",
        "type": type_location(type(exc)),
        "message": message,
        "traceback": trace[-_MAX_TRACEBACK_CHARS:],
    }

    code = getattr(exc, "code", None)
    if isinstance(exc, grpc.RpcError) and callable(code):
        try:
            status = code()
            details = exc.details()
        except Exception:
            status = None
        if isinstance(status, grpc.StatusCode):
            recorded["type"] = type_location(ReplayedRpcError)
            recorded["grpc"] = {"code": status.name, "details": str(details or "")}
            return recorded

    if _BaseExceptionGroup is not None and isinstance(exc, _BaseExceptionGroup):
        members = list(exc.exceptions)
        if depth < _MAX_GROUP_DEPTH and len(members) <= _MAX_GROUP_MEMBERS:
            recorded["group"] = {
                "message": exc.message,
                "exceptions": [_capture_member(m, depth + 1) for m in members],
            }

    args: List[Any] = []
    unavailable_args: List[int] = []
    if "group" not in recorded:
        for i, arg in enumerate(exc.args):
            ok, encoded = _safe_encode(arg)
            ok = ok and _small_enough(encoded)
            args.append(encoded if ok else None)
            if not ok:
                unavailable_args.append(i)

    fields = {**_builtin_fields(exc), **dict(vars(exc))}
    if "group" in recorded:
        for name in ("message", "exceptions"):
            fields.pop(name, None)
    attrs: Dict[str, Any] = {}
    unavailable_attrs: List[str] = []
    for name, value in fields.items():
        if name.startswith("__"):
            continue
        ok, encoded = _safe_encode(value)
        if ok and _small_enough(encoded):
            attrs[name] = encoded
        else:
            unavailable_attrs.append(name)

    recorded.update(
        args=args,
        unavailable_args=unavailable_args,
        attrs=attrs,
        unavailable_attrs=unavailable_attrs,
    )
    try:
        too_big = len(serialize({"e": recorded})) > _MAX_RECORD_BYTES
    except Exception:
        too_big = True
    if too_big:
        # Still recorded, so the position is filled: the type and the message.
        recorded.pop("group", None)
        recorded.update(
            args=[None] * len(exc.args),
            unavailable_args=list(range(len(exc.args))),
            attrs={},
            unavailable_attrs=sorted(n for n in fields if not n.startswith("__")),
        )
    return recorded


def _capture_member(exc: BaseException, depth: int) -> Dict[str, Any]:
    member = capture(exc, depth)
    member.pop("traceback", None)  # the group's own traceback covers it
    return member


# --- Rebuilding ---------------------------------------------------------------


def _builtin_new(cls: type) -> Any:
    """The nearest `__new__` in `cls`'s hierarchy that Python itself defines."""
    for klass in cls.__mro__:
        new = klass.__dict__.get("__new__")
        if new is None:
            continue
        if isinstance(new, staticmethod):
            new = new.__func__
        if isinstance(new, types.FunctionType):
            continue  # defined in Python by the class: never run on replay
        return new
    return BaseException.__new__


_missing_cache: Dict[Tuple[type, Tuple[str, ...]], type] = {}


def _with_missing(cls: type, names: List[str]) -> type:
    """`cls` with a property per name that raises when read.

    Created once per class and set of names, so a class's subclass hooks run once
    rather than on every replay.
    """
    key = (cls, tuple(sorted(names)))
    cached = _missing_cache.get(key)
    if cached is not None:
        return cached

    def _missing(name: str) -> property:
        def _get(self: Any) -> Any:
            raise AttributeError(
                f"{cls.__name__}.{name} was not recorded, so it is not "
                f"available on replay"
            )

        return property(_get)

    subclass = type(
        cls.__name__,
        (cls,),
        {
            "__module__": cls.__module__,
            "__qualname__": cls.__qualname__,
            **{name: _missing(name) for name in names},
        },
    )
    _missing_cache[key] = subclass
    return subclass


def rebuild(recorded: Any, step_name: str, sequence: int) -> BaseException:
    """An instance of the recorded exception's class, carrying its recorded state.

    Raises `ReplayedFailureError` when it cannot, so the caller can refuse the
    resume instead of running the step again.
    """
    where = f"the failure of {step_name!r} at position {sequence}"
    try:
        return _rebuild(recorded, where)
    except ReplayedFailureError:
        raise
    except Exception as error:
        # A record this SDK could not have written, or a class hook that refused.
        raise ReplayedFailureError(
            f"Cannot replay {where}: the recorded failure could not be recreated "
            f"({type(error).__name__}: {error})."
        ) from error


def _rebuild(recorded: Any, where: str) -> BaseException:
    if not isinstance(recorded, dict) or not recorded.get("type"):
        raise ReplayedFailureError(
            f"Cannot replay {where}: the step is recorded as failed, but not in a "
            f"form this SDK writes."
        )
    sdk = recorded.get("sdk", "python")
    if sdk != "python":
        raise ReplayedFailureError(
            f"Cannot replay {where}: it was recorded by the {sdk} SDK, and cannot "
            f"be raised as a Python exception."
        )

    grpc_error = recorded.get("grpc")
    if grpc_error is not None:
        status = getattr(grpc.StatusCode, str(grpc_error.get("code", "")), None)
        if not isinstance(status, grpc.StatusCode):
            raise ReplayedFailureError(
                f"Cannot replay {where}: unknown gRPC status {grpc_error!r}."
            )
        replayed = ReplayedRpcError(status, str(grpc_error.get("details", "")))
        _note(replayed, recorded, where)
        return replayed

    location = recorded["type"]
    cls = _resolve(location)
    if not (isinstance(cls, type) and issubclass(cls, BaseException)):
        raise ReplayedFailureError(
            f"Cannot replay {where}: its exception type {location} is not defined "
            f"in any module this process has loaded. Define the exceptions a step "
            f"raises at module level, in a module the workflow imports, and keep "
            f"them there while runs that raised them may still resume."
        )

    group = recorded.get("group")
    if group is not None:
        # An exception group: its members are rebuilt the same way, and the group
        # is made from them, as `except*` needs.
        members = [_rebuild(member, where) for member in group.get("exceptions", [])]
        args: List[Any] = [group.get("message", ""), members]
    else:
        args = [_decode(arg) for arg in recorded.get("args", [])]
        for i in recorded.get("unavailable_args", []):
            args[i] = UnavailableOnReplay(f"argument {i} of {cls.__name__}")
    unavailable = list(recorded.get("unavailable_attrs", []))
    if unavailable:
        cls = _with_missing(cls, unavailable)

    try:
        exc: BaseException = _builtin_new(cls)(cls, *args)
        init = inspect.getattr_static(cls, "__init__")
        if isinstance(init, types.WrapperDescriptorType):
            # Python's own __init__, which only stores fields from the arguments,
            # such as a UnicodeError's positions. The class defines none of its
            # own, so this is all its construction ever did.
            cls.__init__(exc, *args)
    except Exception as error:
        raise ReplayedFailureError(
            f"Cannot replay {where}: {location} could not be recreated ({error})."
        ) from error
    if group is None:
        exc.args = tuple(args)

    state = vars(exc)
    for name, value in recorded.get("attrs", {}).items():
        value = _decode(value)
        descriptor = inspect.getattr_static(type(exc), name, None)
        builtin_field = (types.GetSetDescriptorType, types.MemberDescriptorType)
        if isinstance(descriptor, builtin_field):
            # A field Python keeps outside __dict__, such as OSError.errno.
            try:
                descriptor.__set__(exc, value)
            except (AttributeError, TypeError):
                pass  # read-only on this class: its constructor already set it
        else:
            state[name] = value

    _note(exc, recorded, where)
    return exc


def _note(exc: BaseException, recorded: Dict[str, Any], where: str) -> None:
    note = f"Replayed {where}. Original traceback:\n{recorded.get('traceback', '')}"
    add_note: Optional[Any] = getattr(exc, "add_note", None)
    if add_note is not None:  # Python 3.11 and later
        add_note(note)


def unrecorded(recorded: Dict[str, Any]) -> List[str]:
    """What `capture` could not record, for a warning when the failure happens."""
    names = [f"argument {i}" for i in recorded.get("unavailable_args", [])]
    return names + list(recorded.get("unavailable_attrs", []))
