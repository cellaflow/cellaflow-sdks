"""Records a step's failure, and raises the same failure again on replay.

A workflow that catches a step's exception and carries on takes a branch that
depends on that exception. A resumed run must take the same branch, so it must
raise the same exception at the same position instead of running the step again.
The failure is recorded as data, and rebuilt here into an instance of the same
class, so `except` clauses, `isinstance` checks and attribute reads behave as
they did the first time.

The rebuild does not call the class's constructor. It creates a bare instance and
restores the recorded `args` and attributes, so an exception whose constructor
takes different arguments from the ones it passes to `Exception.__init__` still
comes back intact. What it needs is the class itself: defined at module level,
and still importable when the run resumes. Values that cannot be serialized,
such as an HTTP response object, are not recorded; reading one on replay raises
an error naming it, rather than quietly giving a different value.
"""

import importlib
import traceback
import types
from typing import Any, Dict, List

from cellaflow.serialization import serialize

#: Format of the recorded failure, so a later SDK can tell what it is reading.
FAILURE_FORMAT = 1


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


def _plain(value: Any) -> bool:
    """Whether `value` survives the SDK's serializer unchanged."""
    try:
        serialize({"v": value})
        return True
    except Exception:
        return False


def type_location(cls: type) -> str:
    return f"{cls.__module__}:{cls.__qualname__}"


def importable(cls: type) -> bool:
    """Whether `cls` can be found again by its recorded location."""
    try:
        return _resolve(type_location(cls)) is cls
    except Exception:
        return False


def capture(exc: BaseException) -> Dict[str, Any]:
    """The recorded form of `exc`."""
    args: List[Any] = []
    unavailable_args: List[int] = []
    for i, arg in enumerate(exc.args):
        if _plain(arg):
            args.append(arg)
        else:
            args.append(None)
            unavailable_args.append(i)

    attrs: Dict[str, Any] = {}
    unavailable_attrs: List[str] = []
    fields = dict(vars(exc))
    if isinstance(exc, OSError):
        # Kept outside `args` and `__dict__`, by OSError itself.
        for name in ("filename", "filename2"):
            if getattr(exc, name, None) is not None:
                fields[name] = getattr(exc, name)
    for name, value in fields.items():
        if name.startswith("__"):
            continue
        if _plain(value):
            attrs[name] = value
        else:
            unavailable_attrs.append(name)

    return {
        "format": FAILURE_FORMAT,
        "type": type_location(type(exc)),
        "message": str(exc),
        "args": args,
        "unavailable_args": unavailable_args,
        "attrs": attrs,
        "unavailable_attrs": unavailable_attrs,
        "traceback": "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ),
    }


def _resolve(location: str) -> Any:
    module_name, _, qualname = location.partition(":")
    obj: Any = importlib.import_module(module_name)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return obj


def rebuild(recorded: Dict[str, Any], step_name: str, sequence: int) -> BaseException:
    """An instance of the recorded exception's class, carrying its recorded state.

    Raises `ReplayedFailureError` when the class cannot be found, so the caller
    can refuse the resume instead of running the step again.
    """
    location = recorded.get("type", "")
    try:
        cls = _resolve(location)
    except Exception:
        cls = None
    if not (isinstance(cls, type) and issubclass(cls, BaseException)):
        raise ReplayedFailureError(
            f"Cannot replay the failure of {step_name!r} at position {sequence}: "
            f"its exception type {location} cannot be imported. Define the "
            f"exceptions a step raises at module level, and keep them where they "
            f"are while runs that raised them may still resume."
        )

    args: List[Any] = list(recorded.get("args", []))
    for i in recorded.get("unavailable_args", []):
        args[i] = UnavailableOnReplay(f"argument {i} of {cls.__name__}")
    unavailable = list(recorded.get("unavailable_attrs", []))

    if unavailable:
        # Reading an attribute that was not recorded must fail loudly. A subclass
        # with a property per missing name does that, and keeps `except` and
        # `isinstance` matching the original class.
        def _missing(name: str) -> property:
            def _get(self: Any) -> Any:
                raise AttributeError(
                    f"{cls.__name__}.{name} was not recorded, so it is not "
                    f"available on replay"
                )

            return property(_get)

        cls = type(
            cls.__name__,
            (cls,),
            {
                "__module__": cls.__module__,
                "__qualname__": cls.__qualname__,
                **{name: _missing(name) for name in unavailable},
            },
        )

    exc: BaseException
    try:
        # BaseException.__new__ takes the arguments without running __init__, and
        # lets built-ins such as OSError restore their C-level fields from them.
        exc = cls.__new__(cls, *args)
    except Exception:
        # A class whose own __new__ takes different arguments.
        try:
            exc = cls.__new__(cls)
        except Exception as error:
            raise ReplayedFailureError(
                f"Cannot replay the failure of {step_name!r} at position "
                f"{sequence}: {location} could not be recreated ({error})."
            ) from error
    if isinstance(cls.__init__, types.WrapperDescriptorType):
        # Python's own __init__, which only stores fields from the arguments (a
        # UnicodeError's positions, say). Safe to run, and the only way to set
        # them. A class's own __init__ is never run on replay.
        type(exc).__init__(exc, *args)
    exc.args = tuple(args)
    state = vars(exc)
    for name, value in recorded.get("attrs", {}).items():
        descriptor = getattr(type(exc), name, None)
        builtin_field = (types.GetSetDescriptorType, types.MemberDescriptorType)
        if isinstance(descriptor, builtin_field):
            # A field the built-in keeps outside __dict__, such as OSError.filename.
            setattr(exc, name, value)
        else:
            state[name] = value

    note = (
        f"Replayed from the failure of step {step_name!r} recorded at position "
        f"{sequence}. Original traceback:\n{recorded.get('traceback', '')}"
    )
    add_note = getattr(exc, "add_note", None)
    if add_note is not None:  # Python 3.11 and later
        add_note(note)
    return exc
