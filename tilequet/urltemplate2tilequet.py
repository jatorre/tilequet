"""Convert XYZ/TMS URL template tile servers to TileQuet format.

Fetches tiles from a URL template like:
    https://tile.openstreetmap.org/{z}/{x}/{y}.png

Supports both XYZ (default, top-left origin) and TMS (bottom-left origin) schemes.

Requires the `httpx` package: pip install httpx
"""

import logging
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
        return httpx.Client(timeout=30.0, follow_redirects=True)
    except ImportError:
        raise ImportError(
            "The 'httpx' package is required for URL template conversion. "
            "Install with: pip install 'tilequet-io[url]'"
        )


def _tms_y_to_xyz(zoom: int, tms_y: int) -> int:
    """Convert TMS Y coordinate to XYZ Y coordinate."""
    return (1 << zoom) - 1 - tms_y


def _fetch_tile(
    client,
    url_template: str,
    z: int,
    x: int,
    y: int,
    *,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> bytes | None:
    """Fetch a single tile with retry logic."""
    import httpx

    url = url_template.replace("{z}", str(z)).replace("{x}", str(x)).replace("{y}", str(y))

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


def _tiles_for_bbox(
    bbox: tuple[float, float, float, float],
    zoom: int,
) -> list[tuple[int, int, int]]:
    """Generate tile coordinates covering a bounding box at a given zoom level."""
    import math

    west, south, east, north = bbox

    def _lng_to_tile_x(lng: float, z: int) -> int:
        return int((lng + 180.0) / 360.0 * (1 << z))

    def _lat_to_tile_y(lat: float, z: int) -> int:
        lat_rad = math.radians(lat)
        n = 1 << z
        return int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)

    x_min = max(0, _lng_to_tile_x(west, zoom))
    x_max = min((1 << zoom) - 1, _lng_to_tile_x(east, zoom))
    y_min = max(0, _lat_to_tile_y(north, zoom))  # north has smaller Y
    y_max = min((1 << zoom) - 1, _lat_to_tile_y(south, zoom))

    tiles = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            tiles.append((zoom, x, y))

    return tiles


def convert(
    url_template: str,
    output_path: str,
    *,
    zooms: list[int] | None = None,
    min_zoom: int = 0,
    max_zoom: int = 5,
    bbox: tuple[float, float, float, float] | None = None,
    tms: bool = False,
    row_group_size: int = 1,
    verbose: bool = False,
) -> dict[str, Any]:
    """Convert tiles from a URL template to TileQuet format.

    Args:
        url_template: URL with {z}, {x}, {y} placeholders.
        output_path: Path to output .parquet file.
        zooms: Explicit list of zoom levels to fetch.
        min_zoom: Minimum zoom level (if zooms not provided).
        max_zoom: Maximum zoom level (if zooms not provided).
        bbox: Bounding box filter [west, south, east, north] in WGS84.
        tms: Use TMS Y convention (flipped Y axis).
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    if zooms is None:
        zooms = list(range(min_zoom, max_zoom + 1))

    actual_min_zoom = min(zooms)
    actual_max_zoom = max(zooms)

    if bbox is None:
        bbox = (-180, -85.051129, 180, 85.051129)

    client = _get_http_client()

    writer = TileQuetWriter(output_path, row_group_size=row_group_size)
    tile_format = None
    tile_type = None
    tiles_fetched = 0
    tiles_skipped = 0

    try:
        for z in zooms:
            tile_coords = _tiles_for_bbox(bbox, z)

            if verbose:
                logger.info("Zoom %d: %d tiles to fetch", z, len(tile_coords))

            for z, x, y in tile_coords:
                # For TMS, flip Y for the URL but store as XYZ
                url_y = _tms_y_to_xyz(z, y) if tms else y
                fetch_y = url_y if tms else y

                data = _fetch_tile(client, url_template, z, x, fetch_y)

                if data is None:
                    tiles_skipped += 1
                    continue

                # Detect format from first successful tile
                if tile_format is None:
                    tile_format = detect_tile_format(data)
                    tile_type = tile_type_from_format(tile_format)
                    if verbose:
                        logger.info("Detected format: %s (%s)", tile_format, tile_type)

                # Store with XYZ convention
                cell = quadbin.tile_to_cell((x, y, z))
                writer.add_tile(cell, data)
                tiles_fetched += 1

                if verbose and tiles_fetched % 100 == 0:
                    logger.info("Fetched %d tiles...", tiles_fetched)

    finally:
        client.close()

    if writer.tile_count == 0:
        raise ValueError("No tiles were fetched from the URL template")

    if tile_format is None:
        tile_format = "png"
        tile_type = "raster"

    if verbose:
        logger.info(
            "Fetched %d tiles (%d skipped/empty)", tiles_fetched, tiles_skipped
        )

    # Build TileJSON 3.0.0
    center = [
        (bbox[0] + bbox[2]) / 2,
        (bbox[1] + bbox[3]) / 2,
        actual_min_zoom,
    ]
    tilejson = build_tilejson(
        bounds=list(bbox),
        center=center,
        min_zoom=actual_min_zoom,
        max_zoom=actual_max_zoom,
    )

    # Build metadata
    metadata = create_metadata(
        tile_type=tile_type,
        tile_format=tile_format,
        bounds=list(bbox),
        center=center,
        min_zoom=actual_min_zoom,
        max_zoom=actual_max_zoom,
        num_tiles=writer.tile_count,
        source_format="url_template",
        tilejson=tilejson,
    )

    # Write TileQuet file
    writer.close(metadata)

    if verbose:
        logger.info("Written %d tiles to %s", writer.tile_count, output_path)

    return {
        "num_tiles": writer.tile_count,
        "tile_type": tile_type,
        "tile_format": tile_format,
        "min_zoom": actual_min_zoom,
        "max_zoom": actual_max_zoom,
        "tiles_skipped": tiles_skipped,
    }
