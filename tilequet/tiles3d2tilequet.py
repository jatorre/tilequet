"""Convert OGC 3D Tiles to TileQuet format.

Reads a 3D Tiles tileset (tileset.json) and converts the tile content
(glTF, GLB, b3dm, pnts) to TileQuet format.

The 3D Tiles specification uses a hierarchical spatial structure.
This converter flattens tiles into QUADBIN-indexed rows based on
the geographic bounds of each tile. The original tileset.json tree
is preserved in the row-0 metadata with content URIs rewritten to
QUADBIN cell IDs, so viewers can reconstruct the hierarchy.

Requires the `httpx` package: pip install httpx
"""

from __future__ import annotations

import copy
import logging
import math
from typing import Any
from urllib.parse import urljoin

import quadbin

from .metadata import build_tilejson, create_metadata, write_tilequet

logger = logging.getLogger(__name__)

TILE_3D_FORMATS = {
    b"glTF": "gltf",
    b"b3dm": "b3dm",
    b"pnts": "pnts",
    b"i3dm": "i3dm",
    b"cmpt": "cmpt",
}


def _get_http_client():
    """Get HTTP client for making requests."""
    try:
        import httpx
        return httpx.Client(timeout=60.0, follow_redirects=True)
    except ImportError:
        raise ImportError(
            "The 'httpx' package is required for 3D Tiles conversion. "
            "Install with: pip install 'tilequet-io[tiles3d]'"
        )


def _detect_3d_format(data: bytes) -> str:
    """Detect 3D tile format from magic bytes."""
    if len(data) < 4:
        return "unknown"

    # GLB magic: 0x46546C67 ("glTF" in ASCII)
    if data[:4] == b"glTF":
        return "glb"

    # b3dm magic
    if data[:4] == b"b3dm":
        return "b3dm"

    # pnts magic
    if data[:4] == b"pnts":
        return "pnts"

    # i3dm magic
    if data[:4] == b"i3dm":
        return "i3dm"

    # cmpt (composite)
    if data[:4] == b"cmpt":
        return "cmpt"

    # Try JSON (tileset.json)
    if data[:1] == b"{":
        return "json"

    return "unknown"


def _region_to_bounds(region: list[float]) -> list[float]:
    """Convert 3D Tiles region [west, south, east, north, minH, maxH] (radians) to WGS84 bounds."""
    return [
        math.degrees(region[0]),
        math.degrees(region[1]),
        math.degrees(region[2]),
        math.degrees(region[3]),
    ]


def _bounds_to_quadbin(bounds: list[float], zoom: int) -> int:
    """Convert WGS84 bounds to a QUADBIN cell at the center."""
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2

    # Convert to tile coordinates
    n = 1 << zoom
    x = int((center_lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(center_lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)

    x = max(0, min(n - 1, x))
    y = max(0, min(n - 1, y))

    return quadbin.tile_to_cell((x, y, zoom))


def _estimate_zoom_from_bounds(bounds: list[float]) -> int:
    """Estimate appropriate zoom level from geographic bounds."""
    width = abs(bounds[2] - bounds[0])
    height = abs(bounds[3] - bounds[1])
    extent = max(width, height)

    if extent <= 0:
        return 14

    zoom = int(math.log2(360.0 / extent))
    return max(0, min(22, zoom))


def _geometric_error_to_zoom(geometric_error: float) -> int:
    """Estimate zoom level from 3D Tiles geometric error (meters).

    Higher geometric error = coarser detail = lower zoom level.
    At zoom 0, a tile covers ~40,000 km; at zoom 20, ~38 m.
    """
    if geometric_error <= 0:
        return 20
    zoom = int(math.log2(40_000_000 / geometric_error))
    return max(0, min(22, zoom))


def _ecef_to_wgs84(x: float, y: float, z: float) -> tuple[float, float]:
    """Convert ECEF (Earth-Centered, Earth-Fixed) coordinates to WGS84 (lon, lat) degrees."""
    # WGS84 ellipsoid constants
    a = 6378137.0  # semi-major axis (meters)
    e2 = 0.00669437999014  # first eccentricity squared

    lon = math.atan2(y, x)
    p = math.sqrt(x * x + y * y)
    lat = math.atan2(z, p * (1 - e2))

    # Iterative refinement (converges in 2-3 iterations)
    for _ in range(5):
        N = a / math.sqrt(1 - e2 * math.sin(lat) ** 2)
        lat = math.atan2(z + e2 * N * math.sin(lat), p)

    return math.degrees(lon), math.degrees(lat)


def _multiply_transforms(a: list[float], b: list[float]) -> list[float]:
    """Multiply two 4x4 column-major matrices: result = a * b."""
    result = [0.0] * 16
    for row in range(4):
        for col in range(4):
            val = 0.0
            for k in range(4):
                val += a[row + k * 4] * b[k + col * 4]
            result[row + col * 4] = val
    return result


def _transform_point(transform: list[float], point: list[float]) -> list[float]:
    """Transform a 3D point using a 4x4 column-major matrix."""
    x = transform[0] * point[0] + transform[4] * point[1] + transform[8] * point[2] + transform[12]
    y = transform[1] * point[0] + transform[5] * point[1] + transform[9] * point[2] + transform[13]
    z = transform[2] * point[0] + transform[6] * point[1] + transform[10] * point[2] + transform[14]
    return [x, y, z]


def _fetch_tileset(url: str, client) -> dict:
    """Fetch and parse a tileset.json."""

    response = client.get(url)
    response.raise_for_status()
    return response.json()


def _collect_tile_refs(
    tileset: dict,
    base_url: str,
    *,
    max_depth: int = 50,
) -> list[dict]:
    """Recursively collect all tile content references from a tileset.

    Propagates 3D Tiles transform matrices down the tree and converts
    box/sphere bounding volumes from local coordinates to WGS84 bounds
    using ECEF→WGS84 conversion.
    """
    identity = [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1]
    refs = []

    def _process_tile(tile: dict, parent_transform: list[float], depth: int = 0):
        if depth > max_depth:
            return

        # Accumulate transform: current = parent * local
        local_transform = tile.get("transform")
        if local_transform:
            current_transform = _multiply_transforms(parent_transform, local_transform)
        else:
            current_transform = parent_transform

        content = tile.get("content", {})
        content_uri = content.get("uri") or content.get("url")

        if content_uri:
            full_url = urljoin(base_url, content_uri)

            bounds = None
            bv = tile.get("boundingVolume", {})

            if "region" in bv:
                bounds = _region_to_bounds(bv["region"])
            elif "box" in bv:
                box = bv["box"]
                # Box: [cx, cy, cz, xx, xy, xz, yx, yy, yz, zx, zy, zz]
                local_center = [box[0], box[1], box[2]]
                ecef = _transform_point(current_transform, local_center)
                lon, lat = _ecef_to_wgs84(ecef[0], ecef[1], ecef[2])
                # Approximate extent from half-axis magnitudes (meters → degrees)
                half_x = math.sqrt(box[3] ** 2 + box[4] ** 2 + box[5] ** 2)
                half_y = math.sqrt(box[6] ** 2 + box[7] ** 2 + box[8] ** 2)
                extent_m = max(half_x, half_y)
                extent_deg = extent_m / 111320.0
                bounds = [
                    lon - extent_deg, lat - extent_deg,
                    lon + extent_deg, lat + extent_deg,
                ]
            elif "sphere" in bv:
                sphere = bv["sphere"]
                local_center = [sphere[0], sphere[1], sphere[2]]
                ecef = _transform_point(current_transform, local_center)
                lon, lat = _ecef_to_wgs84(ecef[0], ecef[1], ecef[2])
                radius_deg = sphere[3] / 111320.0
                bounds = [
                    lon - radius_deg, lat - radius_deg,
                    lon + radius_deg, lat + radius_deg,
                ]

            refs.append({
                "url": full_url,
                "original_uri": content_uri,
                "bounds": bounds,
                "geometric_error": tile.get("geometricError", 0),
            })

        for child in tile.get("children", []):
            _process_tile(child, current_transform, depth + 1)

    root = tileset.get("root", tileset)
    _process_tile(root, identity)

    return refs


def _rewrite_tileset_uris(tileset: dict, uri_to_quadbin: dict[str, int]) -> dict:
    """Deep-copy tileset.json and rewrite content URIs to QUADBIN cell IDs.

    This preserves the tree structure while replacing file paths with
    QUADBIN identifiers so the viewer can map tiles back to Parquet rows.
    """
    rewritten = copy.deepcopy(tileset)

    def _rewrite_node(node: dict):
        content = node.get("content", {})
        content_uri = content.get("uri") or content.get("url")

        if content_uri and content_uri in uri_to_quadbin:
            cell_id = uri_to_quadbin[content_uri]
            # Use "uri" key (3D Tiles 1.0+)
            content["uri"] = str(cell_id)
            content.pop("url", None)

        for child in node.get("children", []):
            _rewrite_node(child)

    root = rewritten.get("root", rewritten)
    _rewrite_node(root)

    return rewritten


def convert(
    tileset_url: str,
    output_path: str,
    *,
    max_tiles: int | None = None,
    row_group_size: int = 200,
    verbose: bool = False,
) -> dict[str, Any]:
    """Convert a 3D Tiles tileset to TileQuet format.

    The original tileset.json tree is stored in the row-0 metadata
    with content URIs rewritten to QUADBIN cell IDs. This allows
    viewers to reconstruct the 3D Tiles hierarchy while fetching
    tile data from the Parquet file.

    Args:
        tileset_url: URL to tileset.json.
        output_path: Path to output .parquet file.
        max_tiles: Maximum number of tiles to fetch (for testing).
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    client = _get_http_client()

    try:
        # Fetch tileset.json
        logger.info("Fetching tileset from %s...", tileset_url)
        tileset = _fetch_tileset(tileset_url, client)

        if verbose:
            asset = tileset.get("asset", {})
            logger.info("3D Tiles version: %s", asset.get("version", "unknown"))

        # Base URL for resolving relative content URIs
        base_url = tileset_url.rsplit("/", 1)[0] + "/"

        # Collect all tile references
        tile_refs = _collect_tile_refs(tileset, base_url)
        if verbose:
            logger.info("Found %d tile content references", len(tile_refs))

        if max_tiles:
            tile_refs = tile_refs[:max_tiles]

        # Fetch tile content and build URI → QUADBIN mapping
        tiles = []
        uri_to_quadbin: dict[str, int] = {}
        used_cells: set[int] = set()
        tile_format = None
        all_bounds = []
        min_zoom = 99
        max_zoom = 0

        for i, ref in enumerate(tile_refs):
            try:
                response = client.get(ref["url"])
                response.raise_for_status()
                data = response.content
            except Exception as e:
                if verbose:
                    logger.warning("Failed to fetch tile %s: %s", ref["url"], e)
                continue

            if not data:
                continue

            # Detect format from first tile
            if tile_format is None:
                tile_format = _detect_3d_format(data)
                if verbose:
                    logger.info("Detected 3D tile format: %s", tile_format)

            # Calculate QUADBIN cell from bounds
            bounds = ref.get("bounds")
            if bounds:
                geo_error = ref.get("geometric_error", 0)
                if geo_error > 0:
                    zoom = _geometric_error_to_zoom(geo_error)
                else:
                    zoom = _estimate_zoom_from_bounds(bounds)
                cell = _bounds_to_quadbin(bounds, zoom)

                # Resolve collisions by trying lower zoom levels
                while cell in used_cells and zoom > 0:
                    zoom -= 1
                    cell = _bounds_to_quadbin(bounds, zoom)

                all_bounds.append(bounds)
                min_zoom = min(min_zoom, zoom)
                max_zoom = max(max_zoom, zoom)
            else:
                cell = quadbin.tile_to_cell((0, 0, 0))
                while cell in used_cells:
                    # Shift to a different cell
                    cell = quadbin.tile_to_cell((1, 0, 0))
                min_zoom = 0
                max_zoom = max(max_zoom, 0)

            used_cells.add(cell)
            tiles.append({"tile": cell, "data": data})

            # Map original URI → QUADBIN cell for tileset.json rewriting
            uri_to_quadbin[ref["original_uri"]] = cell

            if verbose and (i + 1) % 50 == 0:
                logger.info("Fetched %d/%d tiles...", i + 1, len(tile_refs))

    finally:
        client.close()

    if not tiles:
        raise ValueError("No tiles were fetched from the 3D Tiles tileset")

    if tile_format is None:
        tile_format = "glb"

    # Compute overall bounds
    if all_bounds:
        overall_bounds = [
            min(b[0] for b in all_bounds),
            min(b[1] for b in all_bounds),
            max(b[2] for b in all_bounds),
            max(b[3] for b in all_bounds),
        ]
    else:
        overall_bounds = [-180, -85.051129, 180, 85.051129]

    if verbose:
        logger.info("Fetched %d 3D tiles", len(tiles))

    # Rewrite tileset.json URIs to QUADBIN cell IDs
    rewritten_tileset = _rewrite_tileset_uris(tileset, uri_to_quadbin)

    center = [
        (overall_bounds[0] + overall_bounds[2]) / 2,
        (overall_bounds[1] + overall_bounds[3]) / 2,
        min_zoom,
    ]

    tilejson = build_tilejson(
        bounds=overall_bounds,
        center=center,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
    )

    metadata = create_metadata(
        tile_type="3d",
        tile_format=tile_format,
        bounds=overall_bounds,
        center=center,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        num_tiles=len(tiles),
        source_format="3dtiles",
        tileset_json=rewritten_tileset,
        tilejson=tilejson,
    )

    write_tilequet(output_path, tiles, metadata, row_group_size=row_group_size)

    if verbose:
        logger.info("Written %d tiles to %s", len(tiles), output_path)

    return {
        "num_tiles": len(tiles),
        "tile_type": "3d",
        "tile_format": tile_format,
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
    }
