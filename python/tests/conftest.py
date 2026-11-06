"""Fixtures every test gets."""

from typing import Any

import pytest

from cellaflow.client import ALLOW_PLAINTEXT_KEY_ENV_VAR, API_KEY_ENV_VAR


@pytest.fixture(autouse=True)
def no_key_in_environment(monkeypatch: Any) -> None:
    """A key, or the plaintext opt-in, exported in the developer's shell would
    otherwise reach every client a test builds, and change what it sees."""
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    monkeypatch.delenv(ALLOW_PLAINTEXT_KEY_ENV_VAR, raising=False)
