"""Named pipeline profiles (bundled CLI defaults for reproducible analysis)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

PROFILE_CHOICES: tuple[str, ...] = ("plain", "void_hex_fuzzy_latest")


def apply_pipeline_profile(args: Any) -> None:
    """Mutate ``args`` when ``args.pipeline_profile`` is set (explicit bundle)."""
    name = getattr(args, "pipeline_profile", None)
    if name is None:
        return
    if name == "plain":
        args.parquet = Path("data/processed/precincts.parquet")
        args.graph_fuzzy = False
        args.graph_fuzzy_buffering = False
    elif name == "void_hex_fuzzy_latest":
        args.etl_with_gaps = True
        args.etl_shell = Path("data/raw/admin")
        args.etl_void_hex = True
        args.etl_out_parquet = Path("data/processed/precincts_void_hex.parquet")
        args.parquet = Path("data/processed/precincts_void_hex.parquet")
        args.graph_fuzzy = True
        args.graph_fuzzy_buffering = True
    else:
        msg = f"unknown profile: {name!r}"
        raise ValueError(msg)
