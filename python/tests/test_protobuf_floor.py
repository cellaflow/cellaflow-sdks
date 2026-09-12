"""The declared protobuf floor must be one the generated stubs can actually run.

`cellaflow==0.5.0` declared `protobuf>=4.25.0` while its stubs asserted a
runtime of at least 7.35.1, so pip resolved installs that crashed on
`import cellaflow`. Nothing caught it: the suite only ever ran against whatever
protobuf happened to be installed, which was always new enough.

protoc bakes its own version into every generated module as a runtime
assertion, so the floor is set by whichever `grpcio-tools` produced the stubs
rather than by anything anyone wrote down. These tests tie the two numbers
together so they cannot drift apart again silently.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_GENERATED = Path(__file__).resolve().parents[1] / "src" / "cellaflow" / "v1"
_PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"

#: `ValidateProtobufRuntimeVersion(Domain.PUBLIC, major, minor, patch, ...)`
_GENCODE = re.compile(
    r"ValidateProtobufRuntimeVersion\(\s*"
    r"_runtime_version\.Domain\.\w+,\s*(\d+),\s*(\d+),\s*(\d+)",
    re.MULTILINE,
)


def _generated_modules() -> list[Path]:
    mods = sorted(_GENERATED.glob("*_pb2.py"))
    assert mods, f"no generated modules under {_GENERATED}"
    return mods


def _declared_floor() -> tuple[int, int, int]:
    match = re.search(r'"protobuf>=(\d+)\.(\d+)\.(\d+)"', _PYPROJECT.read_text())
    assert match, "pyproject declares no protobuf floor"
    return tuple(int(g) for g in match.groups())  # type: ignore[return-value]


@pytest.mark.parametrize("module", _generated_modules(), ids=lambda p: p.name)
def test_declared_floor_covers_every_generated_module(module: Path) -> None:
    """Each stub asserts a runtime at import; the floor must satisfy all of them.

    Parametrised per module rather than checking the maximum, so a failure names
    the file whose regeneration moved the requirement.
    """
    found = _GENCODE.search(module.read_text())
    if found is None:
        pytest.skip(f"{module.name} carries no runtime assertion")

    gencode = tuple(int(g) for g in found.groups())
    assert gencode <= _declared_floor(), (
        f"{module.name} requires protobuf {'.'.join(map(str, gencode))} at import, "
        f"but pyproject declares >={'.'.join(map(str, _declared_floor()))}. "
        "An install at the declared floor would resolve and then fail on "
        "`import cellaflow`. Either lower the gencode by pinning an older "
        "grpcio-tools, or raise the declared floor to match."
    )


def test_the_installed_runtime_can_import_the_package() -> None:
    """The end the user actually experiences.

    Passes trivially when the environment is new enough, which is why it is not
    sufficient on its own -- the parametrised test above is what covers the
    floor CI does not install. This one catches the case where the stubs and
    the *installed* runtime disagree.
    """
    from google.protobuf import __version__ as runtime

    import cellaflow  # noqa: F401
    from cellaflow.v1 import service_pb2  # noqa: F401

    installed = tuple(int(p) for p in runtime.split(".")[:3])
    assert installed >= _declared_floor(), (
        f"the test environment runs protobuf {runtime}, below the declared "
        "floor -- the suite is not exercising a supported configuration"
    )
