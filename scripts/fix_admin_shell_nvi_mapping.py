#!/usr/bin/env python3
"""Align ``data/raw/admin/NN.geojson`` stems with NVI ``maz`` for counties 11–16.

The shipped OSM export used filenames 11–16 that did not match NVI county codes
(Jász-Nagykun-Szolnok is NVI 11 but lived in ``16.geojson``, etc.). Gap ETL keys
shells by file stem, so shells were paired with the wrong precincts.

This script rewrites those six files: each target stem ``NN`` receives the geometry
that previously lived under the mapped source stem, and ``properties.ksh`` is set
to ``NN`` so :func:`read_shell_gdf` validation still passes.

Safe to re-run: if ``13.geojson`` already describes Nógrád (not Pest), the script exits.

Example::

    uv run python scripts/fix_admin_shell_nvi_mapping.py --admin-dir data/raw/admin
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

# Target NVI stem -> source file stem (geometry currently under wrong name).
TARGET_STEM_FROM_SOURCE: dict[str, str] = {
    "11": "16",
    "12": "11",
    "13": "12",
    "14": "13",
    "15": "14",
    "16": "15",
}


def _needs_fix(admin_dir: Path) -> bool:
    """True if 13.geojson is still Pest (misaligned)."""
    p = admin_dir / "13.geojson"
    if not p.is_file():
        return False
    data = json.loads(p.read_text(encoding="utf-8"))
    for feat in data.get("features", []):
        props = feat.get("properties") or {}
        name = str(props.get("name", ""))
        if "Pest" in name and "vármegye" in name:
            return True
    return False


def _already_aligned(admin_dir: Path) -> bool:
    """True if 14.geojson is Pest (NVI-aligned layout)."""
    p = admin_dir / "14.geojson"
    if not p.is_file():
        return False
    data = json.loads(p.read_text(encoding="utf-8"))
    for feat in data.get("features", []):
        props = feat.get("properties") or {}
        name = str(props.get("name", ""))
        ksh = str(props.get("ksh", ""))
        if "Pest" in name and "vármegye" in name and ksh == "14":
            return True
    return False


def _apply_ksh(data: dict, target_stem: str) -> None:
    for feat in data.get("features", []):
        props = feat.setdefault("properties", {})
        props["ksh"] = target_stem


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fix NVI maz alignment for admin county shells 11–16.",
    )
    parser.add_argument(
        "--admin-dir",
        type=Path,
        default=Path("data/raw/admin"),
        help="Directory containing 01.geojson … 20.geojson",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions only; do not write files",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rewrite 11–16 even if Pest is no longer under 13.geojson",
    )
    args = parser.parse_args()
    admin_dir = args.admin_dir.resolve()
    if not admin_dir.is_dir():
        print(f"Not a directory: {admin_dir}", file=sys.stderr)
        return 2

    if not args.force and not _needs_fix(admin_dir):
        print(
            "No change: shells look already aligned "
            "(13.geojson is not Pest), or missing. Use --force only after restoring "
            "the original misaligned OSM export."
        )
        return 0

    if args.force and _already_aligned(admin_dir) and not _needs_fix(admin_dir):
        print(
            "Refusing --force: directory already NVI-aligned (14.geojson is Pest). "
            "Re-applying would corrupt shells. Restore misaligned files first if needed.",
            file=sys.stderr,
        )
        return 2

    staged: list[tuple[str, Path]] = []
    with tempfile.TemporaryDirectory(prefix="admin_shell_fix_") as tmp:
        tmp_path = Path(tmp)
        for target, source in TARGET_STEM_FROM_SOURCE.items():
            src = admin_dir / f"{source}.geojson"
            if not src.is_file():
                print(f"Missing source shell: {src}", file=sys.stderr)
                return 1
            data = json.loads(src.read_text(encoding="utf-8"))
            _apply_ksh(data, target)
            out = tmp_path / f"{target}.geojson"
            out.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            staged.append((target, out))

        if args.dry_run:
            for target, _out in staged:
                src_stem = TARGET_STEM_FROM_SOURCE[target]
                print(f"would write {admin_dir / (target + '.geojson')} <- old {src_stem}.geojson")
            return 0

        for target, staged_path in staged:
            dest = admin_dir / f"{target}.geojson"
            shutil.copy2(staged_path, dest)
            print(f"Wrote {dest.name} (from former {TARGET_STEM_FROM_SOURCE[target]}.geojson, ksh={target})")

    print("Done. Re-run precinct ETL with --with-gaps to refresh void geometry and manifests.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
