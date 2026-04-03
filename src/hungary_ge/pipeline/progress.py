"""tqdm helpers for county-mode pipeline loops."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterable

from tqdm import tqdm


def county_progress_disabled(*, no_progress: bool) -> bool:
    """Whether to disable tqdm (non-interactive, explicit opt-out, or TQDM_DISABLE)."""
    if no_progress:
        return True
    if not sys.stderr.isatty():
        return True
    v = os.environ.get("TQDM_DISABLE", "").strip().lower()
    return v in ("1", "true", "yes", "on")


def county_tqdm[T](
    iterable: Iterable[T],
    *,
    desc: str,
    no_progress: bool,
) -> tqdm:
    return tqdm(
        iterable,
        desc=desc,
        unit="county",
        file=sys.stderr,
        disable=county_progress_disabled(no_progress=no_progress),
    )
