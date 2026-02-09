"""Convert OGC 3D Tiles to TileQuet format.

Reads a 3D Tiles tileset (tileset.json) and converts the tile content
(glTF, GLB, b3dm, pnts) to TileQuet format.

The 3D Tiles specification uses a hierarchical spatial structure.
This converter flattens tiles into QUADBIN-indexed rows based on
the geographic bounds of each tile.

Requires the `httpx` package: pip install httpx
"""

from __future__ import annotations

import json
import logging
import math
import time
from typing import Any
from urllib.parse import urljoin

import quadbin

from .metadata import create_metadata, write_tilequet

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


def _fetch_tileset(url: str, client) -> dict:
    """Fetch and parse a tileset.json."""
    import httpx

    response = client.get(url)
    response.raise_for_status()
    return response.json()


def _collect_tiles(
    tileset: dict,
    base_url: str,
    client,
    *,
    max_depth: int = 50,
    verbose: bool = False,
) -> list[dict]:
    """Recursively collect all tiles from a tileset."""
    tiles = []

    def _process_tile(tile: dict, depth: int = 0):
        if depth > max_depth:
            return

        content = tile.get("content", {})
        content_uri = content.get("uri") or content.get("url")

        if content_uri:
            # Resolve relative URL
            full_url = urljoin(base_url, content_uri)

            # Determine bounds from bounding volume
            bounds = None
            bv = tile.get("boundingVolume", {})

            if "region" in bv:
                bounds = _region_to_bounds(bv["region"])
            elif "box" in bv:
                # box: [cx, cy, cz, xx, xy, xz, yx, yy, yz, zx, zy, zz]
                # Approximate bounds from center
                box = bv["box"]
                cx, cy = box[0], box[1]
                half_x = abs(box[3])
                half_y = abs(box[7])
                bounds = [cx - half_x, cy - half_y, cx + half_x, cy + half_y]

            tiles.append({
                "url": full_url,
                "bounds": bounds,
                "geometric_error": tile.get("geometricError", 0),
            })

        # Process children
        for child in tile.get("children", []):
            _process_tile(child, depth + 1)

    root = tileset.get("root", tileset)
    _process_tile(root)

    return tiles


def convert(
    tileset_url: str,
    output_path: str,
    *,
    max_tiles: int | None = None,
    row_group_size: int = 200,
    verbose: bool = False,
) -> dict[str, Any]:
    """Convert a 3D Tiles tileset to TileQuet format.

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
        tile_refs = _collect_tiles(tileset, base_url, client, verbose=verbose)
        if verbose:
            logger.info("Found %d tile content references", len(tile_refs))

        if max_tiles:
            tile_refs = tile_refs[:max_tiles]

        # Fetch tile content
        tiles = []
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
                zoom = _estimate_zoom_from_bounds(bounds)
                cell = _bounds_to_quadbin(bounds, zoom)
                all_bounds.append(bounds)
                min_zoom = min(min_zoom, zoom)
                max_zoom = max(max_zoom, zoom)
            else:
                cell = quadbin.tile_to_cell((0, 0, 0))
                min_zoom = 0
                max_zoom = max(max_zoom, 0)

            tiles.append({"tile": cell, "data": data})

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

    metadata = create_metadata(
        tile_type="3d",
        tile_format=tile_format,
        bounds=overall_bounds,
        center=[
            (overall_bounds[0] + overall_bounds[2]) / 2,
            (overall_bounds[1] + overall_bounds[3]) / 2,
            min_zoom,
        ],
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        num_tiles=len(tiles),
        source_format="3dtiles",
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
