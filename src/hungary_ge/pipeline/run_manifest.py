"""Run-level provenance JSON (argv, git commit, output fingerprints)."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _sha256_file(path: Path) -> str | None:
    if not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_head(repo_root: Path) -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0 or not out.stdout.strip():
        return None
    return out.stdout.strip()


def write_pipeline_run_manifest(
    repo_root: Path,
    *,
    argv: list[str],
    stages_run: list[str],
    pq_graph: Path,
    extra_output_paths: list[Path] | None = None,
) -> Path:
    """Write ``data/processed/manifests/run_<utc-stamp>.json`` and return its path."""
    from hungary_ge.config import ProcessedPaths

    paths = ProcessedPaths(repo_root)
    paths.manifests_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_path = paths.manifests_dir / f"run_{stamp}.json"

    outputs: dict[str, Any] = {}
    for p in [pq_graph, *(extra_output_paths or [])]:
        key = p.name
        outputs[key] = {
            "path": str(p.resolve()),
            "sha256": _sha256_file(p.resolve()),
        }

    payload: dict[str, Any] = {
        "kind": "pipeline_run",
        "created_at_utc": stamp,
        "argv": argv,
        "stages": stages_run,
        "git_commit": _git_head(repo_root),
        "outputs": outputs,
    }
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return out_path
