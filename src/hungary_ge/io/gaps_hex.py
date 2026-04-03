"""Hexagonal subdivision of void (gap) polygons in metric CRS."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from shapely.geometry import Polygon
from shapely.geometry.base import BaseGeometry


@dataclass(frozen=True)
class HexVoidOptions:
    """Tessellate large gap polygons into flat-top hex cells."""

    enabled: bool = False
    auto_size: bool = True
    hex_area_factor: float = 1.5
    """If set, use this cell area (m²) and skip auto sizing from mean szvk area."""
    hex_cell_area_m2: float | None = None
    """Minimum gap area (m²) to subdivide; if None, use ``mean_szvk_area * subdivide_min_void_factor``."""
    subdivide_min_void_m2: float | None = None
    subdivide_min_void_factor: float = 4.0
    hex_min_cell_area_m2: float = 10_000.0
    hex_max_cell_area_m2: float = 5_000_000.0
    max_cells_per_gap: int = 200_000
    """Minimum metric thickness (m) for a clipped fragment; ``buffer(-w/2)`` must remain non-empty.
    ``None`` or ``<= 0`` disables. Default **30** m (~2× typical carriageway + margin)."""
    min_hex_fragment_width_m: float | None = 30.0
    """Require ``fragment.area >= fraction * resolved_hex_cell_area_m2``. ``None`` or ``<= 0`` disables."""
    min_hex_fragment_area_fraction: float | None = None


def hex_area_from_circumradius(R: float) -> float:
    """Regular hexagon area with circumradius R (center to vertex; equals side length)."""
    return 3.0 * math.sqrt(3) / 2.0 * R * R


def circumradius_from_hex_area(A: float) -> float:
    """Circumradius R from target area A for a regular hexagon."""
    if A <= 0:
        msg = f"hex cell area must be positive, got {A}"
        raise ValueError(msg)
    return math.sqrt(2.0 * A / (3.0 * math.sqrt(3)))


def flat_top_hex_polygon(cx: float, cy: float, R: float) -> Polygon:
    """Flat-top regular hexagon centered at (cx, cy) with circumradius R."""
    coords: list[tuple[float, float]] = []
    for k in range(6):
        ang = math.pi / 6.0 + k * math.pi / 3.0
        coords.append((cx + R * math.cos(ang), cy + R * math.sin(ang)))
    return Polygon(coords)


def _explode_polygons(geom: BaseGeometry) -> list[Polygon]:
    if geom.is_empty:
        return []
    gt = geom.geom_type
    if gt == "Polygon":
        return [geom]  # type: ignore[return-value]
    if gt == "MultiPolygon":
        return list(geom.geoms)  # type: ignore[union-attr]
    if gt == "GeometryCollection":
        out: list[Polygon] = []
        for part in geom.geoms:  # type: ignore[union-attr]
            out.extend(_explode_polygons(part))
        return out
    return []


def _hex_void_quality_filter_active(hex_opts: HexVoidOptions) -> bool:
    w = hex_opts.min_hex_fragment_width_m
    frac = hex_opts.min_hex_fragment_area_fraction
    return (w is not None and w > 0) or (frac is not None and frac > 0)


def _fragment_meets_hex_quality(
    frag: Polygon,
    cell_area_m2: float,
    hex_opts: HexVoidOptions,
) -> bool:
    """Apply optional width (erosion) and area-fraction gates for clipped hex fragments."""
    w = hex_opts.min_hex_fragment_width_m
    if w is not None and w > 0:
        g = frag.buffer(0)
        if g.is_empty:
            return False
        eroded = g.buffer(-0.5 * w)
        if eroded.is_empty:
            return False

    frac = hex_opts.min_hex_fragment_area_fraction
    if frac is not None and frac > 0:
        if frag.area < frac * cell_area_m2:
            return False
    return True


def resolve_hex_cell_area_m2(
    mean_szvk_area_m2: float,
    opts: HexVoidOptions,
) -> tuple[float | None, list[str]]:
    """Pick target hex cell area from manual override or mean-based auto sizing."""
    warns: list[str] = []
    if opts.hex_cell_area_m2 is not None:
        a = opts.hex_cell_area_m2
        if a <= 0:
            return None, ["hex_cell_area_m2 must be positive"]
        a = max(opts.hex_min_cell_area_m2, min(a, opts.hex_max_cell_area_m2))
        return a, warns
    if not opts.auto_size:
        return None, ["hex_void: auto_size is False but hex_cell_area_m2 is unset"]
    if mean_szvk_area_m2 <= 0 or math.isnan(mean_szvk_area_m2):
        return None, [
            "mean szvk area is invalid; set hex_cell_area_m2 or disable hex_void"
        ]
    a = mean_szvk_area_m2 * opts.hex_area_factor
    a = max(opts.hex_min_cell_area_m2, min(a, opts.hex_max_cell_area_m2))
    return a, warns


def resolve_subdivide_min_void_m2(
    mean_szvk_area_m2: float, opts: HexVoidOptions
) -> float:
    if opts.subdivide_min_void_m2 is not None:
        return opts.subdivide_min_void_m2
    base = mean_szvk_area_m2
    if base <= 0 or math.isnan(base):
        return opts.hex_min_cell_area_m2
    return max(opts.hex_min_cell_area_m2, base * opts.subdivide_min_void_factor)


def _hex_centers_covering_bbox(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    R: float,
) -> list[tuple[float, float]]:
    h = math.sqrt(3) * R
    v = 1.5 * R
    pad = 2 * R
    y = miny - pad
    centers: list[tuple[float, float]] = []
    r = 0
    while y <= maxy + pad:
        x0 = minx - pad + (r % 2) * (h / 2.0)
        x = x0
        while x <= maxx + pad:
            centers.append((x, y))
            x += h
        r += 1
        y += v
    return centers


def subdivide_one_gap_polygon(
    gap: Polygon,
    cell_area_m2: float,
    min_fragment_m2: float,
    max_cells: int,
    hex_opts: HexVoidOptions,
) -> tuple[list[Polygon], int]:
    """Clip a hex grid to ``gap``; drop fragments below ``min_fragment_m2`` and optional quality rules.

    Returns:
        ``(polygons, n_truncated)`` — ``n_truncated`` estimates remaining hex centers
        not processed after ``max_cells`` output polygons were collected.
    """
    R = circumradius_from_hex_area(cell_area_m2)
    minx, miny, maxx, maxy = gap.bounds
    centers = _hex_centers_covering_bbox(minx, miny, maxx, maxy, R)
    out: list[Polygon] = []
    truncated = 0
    for idx, (cx, cy) in enumerate(centers):
        if len(out) >= max_cells:
            truncated = len(centers) - idx
            break
        hx = flat_top_hex_polygon(cx, cy, R)
        if not hx.intersects(gap):
            continue
        inter = hx.intersection(gap)
        for frag in _explode_polygons(inter):
            if frag.area < min_fragment_m2:
                continue
            if not _fragment_meets_hex_quality(frag, cell_area_m2, hex_opts):
                continue
            out.append(frag)
            if len(out) >= max_cells:
                truncated = len(centers) - idx - 1
                return out, max(0, truncated)
    return out, truncated


def subdivide_gap_polygons_hex(
    gap_polygons: list[Any],
    *,
    mean_szvk_area_m2: float,
    hex_opts: HexVoidOptions,
    min_fragment_m2: float,
) -> tuple[list[Polygon], dict[str, Any]]:
    """Subdivide each large gap polygon into hex cells; keep small gaps as one piece.

    Returns:
        ``(final_polygons, meta)`` where ``meta`` includes ``hex_cell_area_m2_used``,
        ``n_truncated_cells``, and ``skipped_hex`` (bool).
    """
    meta: dict[str, Any] = {
        "hex_cell_area_m2_used": None,
        "n_truncated_cells": 0,
        "skipped_hex": False,
    }
    cell_a, warns = resolve_hex_cell_area_m2(mean_szvk_area_m2, hex_opts)
    meta["resolve_warnings"] = warns
    if cell_a is None:
        meta["skipped_hex"] = True
        meta["n_void_polygons_dropped_post_quality"] = 0
        return [p for p in gap_polygons if not p.is_empty], meta

    meta["hex_cell_area_m2_used"] = cell_a
    sub_min = resolve_subdivide_min_void_m2(mean_szvk_area_m2, hex_opts)

    final: list[Polygon] = []
    total_trunc = 0
    for gap in gap_polygons:
        if gap.is_empty:
            continue
        a = float(gap.area)
        if a < sub_min:
            if a >= min_fragment_m2:
                final.append(gap)
            continue
        cells, trunc = subdivide_one_gap_polygon(
            gap,
            cell_a,
            min_fragment_m2,
            hex_opts.max_cells_per_gap,
            hex_opts,
        )
        total_trunc += trunc
        if not cells:
            if a >= min_fragment_m2:
                final.append(gap)
        else:
            final.extend(cells)

    meta["n_truncated_cells"] = total_trunc

    # Gaps with area below ``sub_min`` (and whole-gap fallbacks) skip per-fragment
    # checks inside ``subdivide_one_gap_polygon``; apply the same quality rules here
    # so miniature undivided voids cannot bypass ``min_hex_fragment_area_fraction`` /
    # width when those options are active.
    if _hex_void_quality_filter_active(hex_opts):
        before = len(final)
        final = [
            p
            for p in final
            if not p.is_empty
            and float(p.area) >= min_fragment_m2
            and _fragment_meets_hex_quality(p, cell_a, hex_opts)
        ]
        meta["n_void_polygons_dropped_post_quality"] = before - len(final)
    else:
        meta["n_void_polygons_dropped_post_quality"] = 0

    return final, meta
