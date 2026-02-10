"""Convert OGC API - Maps endpoints to TileQuet format.

Fetches rendered map images from OGC API - Maps RESTful endpoints.
This is the modern successor to WMS, using clean REST URLs:

    {baseUrl}/collections/{collectionId}/map?bbox=...&width=...&height=...

For each XYZ tile, computes the WGS84 bounding box and requests a
rendered image from the map endpoint.

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
            "The 'httpx' package is required for OGC API - Maps conversion. "
            "Install with: pip install 'tilequet-io[ogc]'"
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


def _tile_to_wgs84_bbox(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Convert XYZ tile to WGS84 bounding box (west, south, east, north)."""
    n = 1 << z

    west = x / n * 360.0 - 180.0
    east = (x + 1) / n * 360.0 - 180.0

    north = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    south = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))

    return (west, south, east, north)


def _fetch_map_tile(
    client,
    url: str,
    params: dict,
    *,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> bytes | None:
    """Fetch a single map image with retry logic."""
    import httpx

    for attempt in range(max_retries):
        try:
            response = client.get(url, params=params)
            if response.status_code == 404:
                return None
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if "json" in content_type or ("text" in content_type and "image" not in content_type):
                logger.warning("OGC API Maps returned non-image response: %s", response.text[:200])
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
    base_url: str,
    output_path: str,
    *,
    collection: str,
    bbox: tuple[float, float, float, float] | None = None,
    min_zoom: int = 0,
    max_zoom: int = 5,
    tile_size: int = 256,
    image_format: str = "image/png",
    transparent: bool = True,
    crs: str = "CRS84",
    row_group_size: int = 1,
    verbose: bool = False,
) -> dict[str, Any]:
    """Convert an OGC API - Maps endpoint to TileQuet format.

    For each XYZ tile, computes the WGS84 bounding box and requests
    a rendered image from:
    {base_url}/collections/{collection}/map?bbox=...&width=...&height=...

    Args:
        base_url: OGC API base URL.
        output_path: Path to output .parquet file.
        collection: Collection identifier.
        bbox: Bounding box filter (west, south, east, north) in WGS84.
        min_zoom: Minimum zoom level.
        max_zoom: Maximum zoom level.
        tile_size: Tile width/height in pixels.
        image_format: Image format MIME type.
        transparent: Request transparent background.
        crs: Coordinate reference system (default: CRS84 for lon/lat).
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    if bbox is None:
        bbox = (-180, -85.051129, 180, 85.051129)

    base_url = base_url.rstrip("/")
    map_url = f"{base_url}/collections/{collection}/map"

    # Try to fetch collection metadata
    client = _get_http_client()
    name = collection
    description = None

    if verbose:
        try:
            col_url = f"{base_url}/collections/{collection}"
            response = client.get(col_url, params={"f": "json"})
            if response.status_code == 200:
                col_meta = response.json()
                name = col_meta.get("title", collection)
                description = col_meta.get("description")
                logger.info("Collection: %s", name)
        except Exception:
            pass

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
                west, south, east, north = _tile_to_wgs84_bbox(z, x, y)

                params = {
                    "bbox": f"{west},{south},{east},{north}",
                    "width": str(tile_size),
                    "height": str(tile_size),
                    "f": image_format,
                    "crs": crs,
                }
                if transparent:
                    params["transparent"] = "true"

                try:
                    data = _fetch_map_tile(client, map_url, params)
                except Exception as e:
                    if verbose:
                        logger.warning("Failed to fetch tile z%d/%d/%d: %s", z, x, y, e)
                    tiles_skipped += 1
                    continue

                if not data or len(data) < 100:
                    tiles_skipped += 1
                    continue

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
        raise ValueError("No tiles were fetched from the OGC API - Maps endpoint")

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
        name=name,
        description=description,
    )

    metadata = create_metadata(
        tile_type=tile_type,
        tile_format=tile_format,
        bounds=list(bbox),
        center=center,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        num_tiles=writer.tile_count,
        name=name,
        description=description,
        source_format="ogc_api_maps",
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
