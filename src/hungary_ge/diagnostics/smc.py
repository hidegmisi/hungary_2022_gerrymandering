"""Best-effort redist SMC log inspection (no R dependency in unit tests)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

from hungary_ge.diagnostics.report import SmcLogBlock

_ESS_PAT = re.compile(
    r"(effective\s+sample\s+size|(^|\s)ESS\s*[:=])", re.IGNORECASE | re.MULTILINE
)

_STDERR_KEY = "redist_stderr_path"
_STDOUT_KEY = "redist_stdout_path"
_EXCERPT_MAX = 4000


def scrape_redist_logs_from_metadata(
    metadata: Mapping[str, object],
) -> SmcLogBlock:
    """Read stderr/stdout paths from ``ensemble.metadata`` when present."""

    def _s(key: str) -> str | None:
        v = metadata.get(key)
        if v is None:
            return None
        return str(v)

    stderr_p = _s(_STDERR_KEY)
    stdout_p = _s(_STDOUT_KEY)
    if not stderr_p and not stdout_p:
        return SmcLogBlock(
            parse_status="no_log_paths",
            redist_stderr_path=stderr_p,
            redist_stdout_path=stdout_p,
            log_excerpt_chars=0,
            excerpt_suffix=None,
        )

    combined = ""
    for label, p in ("stderr", stderr_p), ("stdout", stdout_p):
        if not p:
            continue
        path = Path(p)
        if not path.is_file():
            combined += f"[{label}: missing file {p}]\n"
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            combined += f"[{label}: read error {e}]\n"
            continue
        combined += f"--- {label} ---\n{text}\n"

    if not combined.strip():
        return SmcLogBlock(
            parse_status="empty_or_unreadable",
            redist_stderr_path=stderr_p,
            redist_stdout_path=stdout_p,
            log_excerpt_chars=0,
            excerpt_suffix=None,
        )

    hits = len(_ESS_PAT.findall(combined))
    excerpt = combined[-_EXCERPT_MAX:] if len(combined) > _EXCERPT_MAX else combined
    return SmcLogBlock(
        parse_status="scanned",
        redist_stderr_path=stderr_p,
        redist_stdout_path=stdout_p,
        ess_line_hits=hits,
        log_excerpt_chars=len(excerpt),
        excerpt_suffix=excerpt,
    )
