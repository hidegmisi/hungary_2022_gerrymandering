"""Pre-flight validation after parse + profile (before stage execution)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hungary_ge.pipeline.stages.core import DEFAULT_STAGES


def validate_pipeline_args(args: argparse.Namespace, repo_root: Path) -> int | None:
    """Return exit code if validation fails, else ``None``."""
    stages = list(dict.fromkeys(args.only or DEFAULT_STAGES))
    if args.mode == "county" and args.run_id is None:
        print("--run-id is required when using --mode county", file=sys.stderr)
        return 2
    if "allocation" in stages and args.run_id is None:
        print(
            "--run-id is required when the allocation stage is included",
            file=sys.stderr,
        )
        return 2

    if args.etl_with_gaps:
        if args.etl_shell is None:
            args.etl_shell = Path("data/raw/admin")
        shell_chk = args.etl_shell
        if not shell_chk.is_absolute():
            shell_chk = (repo_root / shell_chk).resolve()
        if not shell_chk.is_file() and not shell_chk.is_dir():
            print(f"--etl-shell not found: {shell_chk}", file=sys.stderr)
            return 2
    if args.etl_void_hex and not args.etl_with_gaps:
        print("--etl-void-hex requires --etl-with-gaps", file=sys.stderr)
        return 2

    szavkor = args.szavkor_root
    if not szavkor.is_absolute():
        szavkor = (repo_root / szavkor).resolve()
    needs_raw = "etl" in stages or "votes" in stages
    if needs_raw and not szavkor.is_dir():
        print(f"Missing szavkor_topo root: {szavkor}", file=sys.stderr)
        return 1
    return None
