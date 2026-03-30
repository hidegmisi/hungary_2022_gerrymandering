"""Parse ``szavkor_topo`` settlement JSON fields into Shapely geometries.

Coordinate order in source strings is **latitude then longitude** (WGS84).
:class:`shapely.geometry.Polygon` uses ``(x, y) = (lon, lat)`` (GeoJSON order).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from shapely import make_valid
from shapely.geometry import MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union


@dataclass(frozen=True)
class SzavkorRecord:
    """One precinct row from a settlement ``list`` (before geometry build)."""

    maz: str
    taz: str
    szk: str
    centrum: str
    poligon: str


def normalize_id_component(s: str, width: int) -> str:
    """Zero-pad numeric ID strings (e.g. ``maz`` / ``taz`` / ``szk``)."""
    s = s.strip()
    if s.isdigit():
        return s.zfill(width)
    return s


def composite_precinct_id(maz: str, taz: str, szk: str) -> str:
    """Stable key ``maz-taz-szk`` with normalized components."""
    return (
        f"{normalize_id_component(maz, 2)}-"
        f"{normalize_id_component(taz, 3)}-"
        f"{normalize_id_component(szk, 3)}"
    )


_WS = re.compile(r"\s+")


def parse_centrum(centrum: str) -> tuple[float, float] | None:
    """Parse ``centrum`` into ``(lon, lat)`` or return ``None`` if invalid."""
    centrum = centrum.strip()
    if not centrum:
        return None
    parts = _WS.split(centrum)
    if len(parts) < 2:
        return None
    try:
        lat = float(parts[0])
        lon = float(parts[1])
    except ValueError:
        return None
    return (lon, lat)


def parse_poligon_vertices_lonlat(poligon: str) -> list[tuple[float, float]]:
    """Split ``poligon`` string into ``(lon, lat)`` vertices (closed ring not required)."""
    vertices: list[tuple[float, float]] = []
    for part in poligon.split(","):
        part = part.strip()
        if not part:
            continue
        nums = _WS.split(part)
        if len(nums) < 2:
            continue
        try:
            lat = float(nums[0])
            lon = float(nums[1])
        except ValueError:
            continue
        vertices.append((lon, lat))
    return vertices


def parse_poligon(poligon: str) -> Polygon:
    """Build a :class:`~shapely.geometry.Polygon` shell from ``poligon`` text.

    Raises:
        ValueError: Fewer than three distinct vertices after parsing.
    """
    coords = parse_poligon_vertices_lonlat(poligon)
    if len(coords) < 3:
        msg = "poligon must yield at least three vertices"
        raise ValueError(msg)
    if coords[0] != coords[-1]:
        coords = [*coords, coords[0]]
    return Polygon(coords)


def _polygonal_part(geom: BaseGeometry) -> Polygon | MultiPolygon | None:
    """Reduce a geometry to polygonal part(s), or ``None``."""
    if geom.is_empty:
        return None
    t = geom.geom_type
    if t == "Polygon":
        return geom  # type: ignore[return-value]
    if t == "MultiPolygon":
        return geom  # type: ignore[return-value]
    if t == "GeometryCollection":
        polys: list[Polygon | MultiPolygon] = []
        for g in geom.geoms:  # type: ignore[attr-defined]
            if isinstance(g, Polygon):
                polys.append(g)
            elif isinstance(g, MultiPolygon):
                polys.append(g)
        if not polys:
            return None
        u = unary_union(polys)
        if isinstance(u, Polygon):
            return u
        if isinstance(u, MultiPolygon):
            return u
        return None
    return None


def repair_polygonal_geometry(geom: BaseGeometry) -> Polygon | MultiPolygon | None:
    """Repair invalid rings using documented order: ``make_valid`` then ``buffer(0)``.

    Self-intersections and duplicate vertices are handled by Shapely repair.
    If the result is not a polygon or multipolygon (e.g. only slivers), returns
    ``None`` so the caller can drop the row.
    """
    if geom.is_empty:
        return None
    g: BaseGeometry = geom
    if not g.is_valid:
        g = make_valid(g)
    out = _polygonal_part(g)
    if out is None:
        return None
    if not out.is_valid:
        out = make_valid(out)
        out = _polygonal_part(out)
    if out is not None and not out.is_valid:
        b = out.buffer(0)
        out = _polygonal_part(b)
    if out is None or out.is_empty:
        return None
    if out.geom_type not in ("Polygon", "MultiPolygon"):
        return None
    return out  # type: ignore[return-value]


def record_to_geometry(
    record: SzavkorRecord,
) -> tuple[Polygon | MultiPolygon | None, Point | None]:
    """Return ``(polygonal_geometry, centroid_point)`` for one record.

    If ``poligon`` cannot be repaired to a polygon, returns ``(None, None)`` or
    ``(None, point)`` if ``centrum`` parses (for diagnostics only).
    """
    centroid: Point | None = None
    c = parse_centrum(record.centrum)
    if c is not None:
        centroid = Point(c[0], c[1])
    try:
        raw = parse_poligon(record.poligon)
    except ValueError:
        return None, centroid
    fixed = repair_polygonal_geometry(raw)
    return fixed, centroid
