"""Convert OGC WMS (Web Map Service) to TileQuet format.

Fetches map images from a WMS endpoint by computing XYZ tile bounding boxes
and issuing GetMap requests. The resulting raster tiles are stored in TileQuet
format with QUADBIN spatial indexing.

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
            "The 'httpx' package is required for WMS conversion. "
            "Install with: pip install 'tilequet-io[wms]'"
        )


def _tile_to_web_mercator_bbox(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Convert XYZ tile coordinates to Web Mercator BBOX (xmin, ymin, xmax, ymax)."""
    n = 1 << z
    ORIGIN = 20037508.342789244
    tile_size = 2 * ORIGIN / n
    xmin = -ORIGIN + x * tile_size
    xmax = xmin + tile_size
    ymax = ORIGIN - y * tile_size
    ymin = ymax - tile_size
    return (xmin, ymin, xmax, ymax)


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


def _fetch_wms_tile(
    client,
    service_url: str,
    params: dict,
    *,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> bytes | None:
    """Fetch a single WMS tile with retry logic."""
    import httpx

    for attempt in range(max_retries):
        try:
            response = client.get(service_url, params=params)
            if response.status_code == 404:
                return None
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if "xml" in content_type or "text" in content_type:
                logger.warning("WMS returned non-image response: %s", response.text[:200])
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
    layers: str,
    bbox: tuple[float, float, float, float] | None = None,
    min_zoom: int = 0,
    max_zoom: int = 5,
    tile_size: int = 256,
    image_format: str = "image/png",
    version: str = "1.3.0",
    styles: str = "",
    crs: str = "EPSG:3857",
    transparent: bool = True,
    row_group_size: int = 1,
    verbose: bool = False,
) -> dict[str, Any]:
    """Convert a WMS service to TileQuet format.

    Args:
        service_url: WMS service base URL.
        output_path: Path to output .parquet file.
        layers: Comma-separated WMS layer names.
        bbox: Bounding box filter (west, south, east, north) in WGS84.
        min_zoom: Minimum zoom level.
        max_zoom: Maximum zoom level.
        tile_size: Tile width/height in pixels.
        image_format: WMS image format (e.g., image/png, image/jpeg).
        version: WMS version (1.1.1 or 1.3.0).
        styles: WMS styles parameter.
        crs: Coordinate reference system for requests.
        transparent: Request transparent background.
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    if bbox is None:
        bbox = (-180, -85.051129, 180, 85.051129)

    service_url = service_url.rstrip("/")

    # Determine SRS/CRS parameter name based on WMS version
    crs_key = "CRS" if version >= "1.3.0" else "SRS"

    client = _get_http_client()

    writer = TileQuetWriter(output_path, row_group_size=row_group_size)
    tile_format = None
    tile_type = None
    tiles_fetched = 0
    tiles_skipped = 0

    try:
        for z in range(min_zoom, max_zoom + 1):
            tile_coords = _tiles_for_bbox(bbox, z)

            if verbose:
                logger.info("Zoom %d: %d tiles to fetch", z, len(tile_coords))

            for z, x, y in tile_coords:
                # Compute bounding box in the target CRS
                mx_min, my_min, mx_max, my_max = _tile_to_web_mercator_bbox(z, x, y)

                params = {
                    "SERVICE": "WMS",
                    "VERSION": version,
                    "REQUEST": "GetMap",
                    "LAYERS": layers,
                    "STYLES": styles,
                    crs_key: crs,
                    "BBOX": f"{mx_min},{my_min},{mx_max},{my_max}",
                    "WIDTH": str(tile_size),
                    "HEIGHT": str(tile_size),
                    "FORMAT": image_format,
                    "TRANSPARENT": "TRUE" if transparent else "FALSE",
                }

                try:
                    data = _fetch_wms_tile(client, service_url, params)
                except Exception as e:
                    if verbose:
                        logger.warning("Failed to fetch tile z%d/%d/%d: %s", z, x, y, e)
                    tiles_skipped += 1
                    continue

                if not data or len(data) < 100:
                    tiles_skipped += 1
                    continue

                # Detect format from first tile
                if tile_format is None:
                    tile_format = detect_tile_format(data)
                    tile_type = tile_type_from_format(tile_format)
                    if verbose:
                        logger.info("Detected format: %s (%s)", tile_format, tile_type)

                cell = quadbin.tile_to_cell((x, y, z))
                writer.add_tile(cell, data)
                tiles_fetched += 1

                if verbose and tiles_fetched % 100 == 0:
                    logger.info("Fetched %d tiles...", tiles_fetched)

    finally:
        client.close()

    if writer.tile_count == 0:
        raise ValueError("No tiles were fetched from the WMS service")

    if tile_format is None:
        tile_format = "png"
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
        tile_format=tile_format,
        bounds=list(bbox),
        center=center,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        num_tiles=writer.tile_count,
        source_format="wms",
        tilejson=tilejson,
    )

    writer.close(metadata)

    if verbose:
        logger.info("Written %d tiles to %s", writer.tile_count, output_path)

    return {
        "num_tiles": writer.tile_count,
        "tile_type": tile_type,
        "tile_format": tile_format,
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
        "tiles_skipped": tiles_skipped,
    }
