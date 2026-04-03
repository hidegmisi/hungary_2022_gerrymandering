"""County pipeline tqdm helpers."""

from __future__ import annotations

import sys

import pytest

from hungary_ge.pipeline.progress import county_progress_disabled


def test_county_progress_disabled_explicit_off() -> None:
    assert county_progress_disabled(no_progress=True) is True


def test_county_progress_disabled_env_tqdm_disable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TQDM_DISABLE", "1")
    monkeypatch.setattr(sys.stderr, "isatty", lambda: True)
    assert county_progress_disabled(no_progress=False) is True
