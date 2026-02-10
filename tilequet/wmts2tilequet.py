"""Convert OGC WMTS (Web Map Tile Service) to TileQuet format.

Fetches pre-rendered tiles from a WMTS endpoint using KVP (Key-Value Pair)
GetTile requests. WMTS serves tiles in a grid structure similar to XYZ/TMS
but with OGC-specific parameters (TileMatrix, TileMatrixSet, etc.).

Requires the `httpx` package: pip install httpx
"""

from __future__ import annotations

import logging
import math
import time
from typing import Any

import quadbin

from .metadata import build_tilejson, create_metadata, TileQuetWriter
from .mbtiles2tilequet import detect_tile_format, tile_type_from_format

logger = logging.getLogger(__name__)


def _get_http_client():
    """Get HTTP client for making requests."""
    try:
        import httpx
        return httpx.Client(timeout=60.0, follow_redirects=True)
    except ImportError:
        raise ImportError(
            "The 'httpx' package is required for WMTS conversion. "
            "Install with: pip install 'tilequet-io[wmts]'"
        )


def _tiles_for_bbox(
    bbox: tuple[float, float, float, float],
    zoom: int,
) -> list[tuple[int, int, int]]:
    """Generate tile coordinates covering a bounding box at a given zoom level."""
    west, south, east, north = bbox

    def _lng_to_tile_x(lng: float, z: int) -> int:
        return int((lng + 180.0) / 360.0 * (1 << z))

    def _lat_to_tile_y(lat: float, z: int) -> int:
        lat_rad = math.radians(lat)
        n = 1 << z
        return int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)

    x_min = max(0, _lng_to_tile_x(west, zoom))
    x_max = min((1 << zoom) - 1, _lng_to_tile_x(east, zoom))
    y_min = max(0, _lat_to_tile_y(north, zoom))
    y_max = min((1 << zoom) - 1, _lat_to_tile_y(south, zoom))

    return [(zoom, x, y) for x in range(x_min, x_max + 1) for y in range(y_min, y_max + 1)]


def _fetch_wmts_tile(
    client,
    service_url: str,
    params: dict,
    *,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> bytes | None:
    """Fetch a single WMTS tile with retry logic."""
    import httpx

    for attempt in range(max_retries):
        try:
            response = client.get(service_url, params=params)
            if response.status_code == 404:
                return None
            if response.status_code == 204:
                return None
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if "xml" in content_type and "image" not in content_type:
                logger.warning("WMTS returned non-image response: %s", response.text[:200])
                return None

            return response.content
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or (500 <= status < 600):
                if attempt < max_retries - 1:
                    retry_after = e.response.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after and retry_after.isdigit() else retry_delay * (attempt + 1)
                    time.sleep(delay)
                    continue
            raise
        except (httpx.TimeoutException, httpx.NetworkError):
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            raise

    return None


def convert(
    service_url: str,
    output_path: str,
    *,
    layer: str,
    tile_matrix_set: str = "GoogleMapsCompatible",
    bbox: tuple[float, float, float, float] | None = None,
    min_zoom: int = 0,
    max_zoom: int = 5,
    image_format: str = "image/png",
    style: str = "default",
    row_group_size: int = 1,
    verbose: bool = False,
) -> dict[str, Any]:
    """Convert a WMTS service to TileQuet format.

    Uses KVP (Key-Value Pair) GetTile requests. The TileMatrix levels
    map directly to zoom levels for standard Web Mercator tile matrix sets
    (GoogleMapsCompatible, WebMercatorQuad, EPSG:3857).

    Args:
        service_url: WMTS service base URL.
        output_path: Path to output .parquet file.
        layer: WMTS layer name.
        tile_matrix_set: Tile matrix set identifier.
        bbox: Bounding box filter (west, south, east, north) in WGS84.
        min_zoom: Minimum zoom level (maps to TileMatrix).
        max_zoom: Maximum zoom level (maps to TileMatrix).
        image_format: Image format (e.g., image/png, image/jpeg).
        style: WMTS style name.
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    if bbox is None:
        bbox = (-180, -85.051129, 180, 85.051129)

    service_url = service_url.rstrip("/")

    client = _get_http_client()

    writer = TileQuetWriter(output_path, row_group_size=row_group_size)
    tile_fmt = None
    tile_type = None
    tiles_fetched = 0
    tiles_skipped = 0

    try:
        for z in range(min_zoom, max_zoom + 1):
            tile_coords = _tiles_for_bbox(bbox, z)

            if verbose:
                logger.info("Zoom %d: %d tiles to fetch", z, len(tile_coords))

            for z, x, y in tile_coords:
                params = {
                    "SERVICE": "WMTS",
                    "VERSION": "1.0.0",
                    "REQUEST": "GetTile",
                    "LAYER": layer,
                    "STYLE": style,
                    "TILEMATRIXSET": tile_matrix_set,
                    "TILEMATRIX": str(z),
                    "TILEROW": str(y),
                    "TILECOL": str(x),
                    "FORMAT": image_format,
                }

                try:
                    data = _fetch_wmts_tile(client, service_url, params)
                except Exception as e:
                    if verbose:
                        logger.warning("Failed to fetch tile z%d/%d/%d: %s", z, x, y, e)
                    tiles_skipped += 1
                    continue

                if not data or len(data) < 100:
                    tiles_skipped += 1
                    continue

                # Detect format from first tile
                if tile_fmt is None:
                    tile_fmt = detect_tile_format(data)
                    tile_type = tile_type_from_format(tile_fmt)
                    if verbose:
                        logger.info("Detected format: %s (%s)", tile_fmt, tile_type)

                cell = quadbin.tile_to_cell((x, y, z))
                writer.add_tile(cell, data)
                tiles_fetched += 1

                if verbose and tiles_fetched % 100 == 0:
                    logger.info("Fetched %d tiles...", tiles_fetched)

    finally:
        client.close()

    if writer.tile_count == 0:
        raise ValueError("No tiles were fetched from the WMTS service")

    if tile_fmt is None:
        tile_fmt = "png"
        tile_type = "raster"

    if verbose:
        logger.info("Fetched %d tiles (%d skipped)", tiles_fetched, tiles_skipped)

    center = [
        (bbox[0] + bbox[2]) / 2,
        (bbox[1] + bbox[3]) / 2,
        min_zoom,
    ]

    tilejson = build_tilejson(
        bounds=list(bbox),
        center=center,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
    )

    metadata = create_metadata(
        tile_type=tile_type,
        tile_format=tile_fmt,
        bounds=list(bbox),
        center=center,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        num_tiles=writer.tile_count,
        source_format="wmts",
        tilejson=tilejson,
    )

    writer.close(metadata)

    if verbose:
        logger.info("Written %d tiles to %s", writer.tile_count, output_path)

    return {
        "num_tiles": writer.tile_count,
        "tile_type": tile_type,
        "tile_format": tile_fmt,
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
        "tiles_skipped": tiles_skipped,
    }
