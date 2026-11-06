"""Fixtures every test gets."""

from typing import Any

import pytest

from cellaflow.client import API_KEY_ENV_VAR


@pytest.fixture(autouse=True)
def no_key_in_environment(monkeypatch: Any) -> None:
    """A key exported in the developer's shell would otherwise reach every
    client a test builds, and change what the test sees."""
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
