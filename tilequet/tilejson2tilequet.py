"""Convert a TileJSON endpoint to TileQuet format.

Fetches a TileJSON metadata file, extracts the tile URL templates,
bounds, and zoom range, then fetches all tiles and stores them in
TileQuet format.

TileJSON spec: https://github.com/mapbox/tilejson-spec

Requires the `httpx` package: pip install httpx
"""

from __future__ import annotations

import logging
import math
import time
from typing import Any

import quadbin

from .metadata import build_tilejson, create_metadata, write_tilequet
from .mbtiles2tilequet import detect_tile_format, tile_type_from_format

logger = logging.getLogger(__name__)


def _get_http_client():
    """Get HTTP client for making requests."""
    try:
        import httpx
        return httpx.Client(timeout=60.0, follow_redirects=True)
    except ImportError:
        raise ImportError(
            "The 'httpx' package is required for TileJSON conversion. "
            "Install with: pip install 'tilequet-io[tilejson]'"
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


def _fetch_tile(
    client,
    url: str,
    *,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> bytes | None:
    """Fetch a single tile with retry logic."""
    import httpx

    for attempt in range(max_retries):
        try:
            response = client.get(url)
            if response.status_code == 404:
                return None
            if response.status_code == 204:
                return None
            response.raise_for_status()
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
    tilejson_url: str,
    output_path: str,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    row_group_size: int = 200,
    verbose: bool = False,
) -> dict[str, Any]:
    """Convert a TileJSON endpoint to TileQuet format.

    Fetches the TileJSON metadata, extracts tile URL templates and
    parameters, then fetches all tiles within the specified bounds
    and zoom range.

    Args:
        tilejson_url: URL to a TileJSON file.
        output_path: Path to output .parquet file.
        bbox: Override bounding box (west, south, east, north) in WGS84.
        min_zoom: Override minimum zoom level from TileJSON.
        max_zoom: Override maximum zoom level from TileJSON.
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    client = _get_http_client()

    try:
        # Fetch TileJSON
        logger.info("Fetching TileJSON from %s...", tilejson_url)
        response = client.get(tilejson_url)
        response.raise_for_status()
        tj = response.json()

        if verbose:
            logger.info("TileJSON version: %s", tj.get("tilejson", "unknown"))
            if tj.get("name"):
                logger.info("Name: %s", tj["name"])

        # Extract tile URL template
        tile_templates = tj.get("tiles", [])
        if not tile_templates:
            raise ValueError("TileJSON has no 'tiles' URL templates")

        url_template = tile_templates[0]  # Use first template
        if verbose:
            logger.info("Tile URL template: %s", url_template)

        # Extract bounds and zoom range from TileJSON (with CLI overrides)
        tj_bounds = tj.get("bounds", [-180, -85.051129, 180, 85.051129])
        effective_bbox = bbox if bbox else tuple(tj_bounds)

        tj_min_zoom = tj.get("minzoom", 0)
        tj_max_zoom = tj.get("maxzoom", 22)
        effective_min_zoom = min_zoom if min_zoom is not None else tj_min_zoom
        effective_max_zoom = max_zoom if max_zoom is not None else min(tj_max_zoom, 5)

        if verbose:
            logger.info("Bounds: %s", effective_bbox)
            logger.info("Zoom: %d-%d", effective_min_zoom, effective_max_zoom)

        # Fetch tiles
        tiles = []
        tile_format = None
        tile_type = None
        tiles_fetched = 0
        tiles_skipped = 0

        for z in range(effective_min_zoom, effective_max_zoom + 1):
            tile_coords = _tiles_for_bbox(effective_bbox, z)

            if verbose:
                logger.info("Zoom %d: %d tiles to fetch", z, len(tile_coords))

            for z, x, y in tile_coords:
                url = url_template.replace("{z}", str(z)).replace("{x}", str(x)).replace("{y}", str(y))

                try:
                    data = _fetch_tile(client, url)
                except Exception as e:
                    if verbose:
                        logger.warning("Failed to fetch tile z%d/%d/%d: %s", z, x, y, e)
                    tiles_skipped += 1
                    continue

                if data is None or len(data) < 50:
                    tiles_skipped += 1
                    continue

                if tile_format is None:
                    tile_format = detect_tile_format(data)
                    tile_type = tile_type_from_format(tile_format)
                    if verbose:
                        logger.info("Detected format: %s (%s)", tile_format, tile_type)

                cell = quadbin.tile_to_cell((x, y, z))
                tiles.append({"tile": cell, "data": data})
                tiles_fetched += 1

                if verbose and tiles_fetched % 100 == 0:
                    logger.info("Fetched %d tiles...", tiles_fetched)

    finally:
        client.close()

    if not tiles:
        raise ValueError("No tiles were fetched from the TileJSON source")

    if tile_format is None:
        tile_format = "png"
        tile_type = "raster"

    if verbose:
        logger.info("Fetched %d tiles (%d skipped)", tiles_fetched, tiles_skipped)

    center = list(tj.get("center", [
        (effective_bbox[0] + effective_bbox[2]) / 2,
        (effective_bbox[1] + effective_bbox[3]) / 2,
        effective_min_zoom,
    ]))

    # Build TileJSON from original + overrides
    tilejson = build_tilejson(
        bounds=list(effective_bbox),
        center=center,
        min_zoom=effective_min_zoom,
        max_zoom=effective_max_zoom,
        name=tj.get("name"),
        description=tj.get("description"),
        attribution=tj.get("attribution"),
        vector_layers=tj.get("vector_layers"),
    )

    metadata = create_metadata(
        tile_type=tile_type,
        tile_format=tile_format,
        bounds=list(effective_bbox),
        center=center,
        min_zoom=effective_min_zoom,
        max_zoom=effective_max_zoom,
        num_tiles=len(tiles),
        name=tj.get("name"),
        description=tj.get("description"),
        attribution=tj.get("attribution"),
        layers=tj.get("vector_layers"),
        source_format="tilejson",
        tilejson=tilejson,
    )

    write_tilequet(output_path, tiles, metadata, row_group_size=row_group_size)

    if verbose:
        logger.info("Written %d tiles to %s", len(tiles), output_path)

    return {
        "num_tiles": len(tiles),
        "tile_type": tile_type,
        "tile_format": tile_format,
        "min_zoom": effective_min_zoom,
        "max_zoom": effective_max_zoom,
        "tiles_skipped": tiles_skipped,
    }
