"""Convert ArcGIS MapServer tile services to TileQuet format.

Fetches pre-rendered map tiles from ArcGIS MapServer REST endpoints.
These are typically raster tiles (PNG/JPEG) served as XYZ-style tiles.

Requires the `httpx` package: pip install httpx
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from typing import Any

import quadbin

from .metadata import create_metadata, write_tilequet
from .mbtiles2tilequet import detect_tile_format, tile_type_from_format

logger = logging.getLogger(__name__)


@dataclass
class MapServerMetadata:
    """Metadata about an ArcGIS MapServer service."""

    name: str
    description: str
    tile_format: str
    min_zoom: int
    max_zoom: int
    bounds: list[float] | None
    spatial_reference: dict


def _get_http_client():
    """Get HTTP client for making requests."""
    try:
        import httpx
        return httpx.Client(timeout=60.0, follow_redirects=True)
    except ImportError:
        raise ImportError(
            "The 'httpx' package is required for MapServer conversion. "
            "Install with: pip install 'tilequet-io[mapserver]'"
        )


def _make_request(
    url: str,
    params: dict | None = None,
    token: str | None = None,
    *,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    return_bytes: bool = False,
) -> dict | bytes:
    """Make HTTP request with retry logic."""
    import httpx

    if params is None:
        params = {}
    if token:
        params["token"] = token

    for attempt in range(max_retries):
        try:
            with _get_http_client() as client:
                response = client.get(url, params=params)
                response.raise_for_status()
                if return_bytes:
                    return response.content
                return response.json()
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or (500 <= status < 600):
                if attempt < max_retries - 1:
                    retry_after = e.response.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after and retry_after.isdigit() else retry_delay * (attempt + 1)
                    time.sleep(delay)
                    continue
            elif status == 401:
                raise ValueError("Authentication required. Use --token option.") from None
            elif status == 403:
                raise ValueError("Access denied. Check your credentials.") from None
            elif status == 404:
                raise ValueError(f"Service not found (404). Check the URL: {url}") from None
            raise ValueError(f"HTTP error {status}: {e}") from e
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            raise ValueError(f"Request failed after {max_retries} attempts: {e}") from e

    raise ValueError(f"Request failed after {max_retries} attempts")


def get_mapserver_metadata(
    service_url: str,
    token: str | None = None,
) -> MapServerMetadata:
    """Fetch metadata from an ArcGIS MapServer service."""
    service_url = service_url.rstrip("/")

    data = _make_request(service_url, {"f": "json"}, token)

    if "error" in data:
        error = data["error"]
        raise ValueError(f"MapServer error: {error.get('message', 'Unknown error')}")

    # Get tile info
    tile_info = data.get("tileInfo", {})
    lods = tile_info.get("lods", [])

    min_zoom = min(lod["level"] for lod in lods) if lods else 0
    max_zoom = max(lod["level"] for lod in lods) if lods else 18

    # Determine tile format
    tile_format_str = tile_info.get("format", "PNG")
    format_map = {
        "PNG": "png", "PNG8": "png", "PNG24": "png", "PNG32": "png",
        "JPEG": "jpeg", "JPG": "jpeg",
        "MIXED": "png",
    }
    tile_format = format_map.get(tile_format_str.upper(), "png")

    # Get bounds from fullExtent
    full_extent = data.get("fullExtent", {})
    spatial_ref = full_extent.get("spatialReference", {})
    wkid = spatial_ref.get("latestWkid") or spatial_ref.get("wkid")

    bounds = None
    if all(k in full_extent for k in ("xmin", "ymin", "xmax", "ymax")):
        if wkid == 4326:
            bounds = [full_extent["xmin"], full_extent["ymin"], full_extent["xmax"], full_extent["ymax"]]
        elif wkid in (3857, 102100, 102113):
            # Convert from Web Mercator to WGS84
            bounds = _web_mercator_to_wgs84(
                full_extent["xmin"], full_extent["ymin"],
                full_extent["xmax"], full_extent["ymax"],
            )
        else:
            logger.warning("Unknown WKID %s, bounds may be inaccurate", wkid)

    return MapServerMetadata(
        name=data.get("mapName", data.get("documentInfo", {}).get("Title", "Unknown")),
        description=data.get("description", data.get("serviceDescription", "")),
        tile_format=tile_format,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        bounds=bounds,
        spatial_reference=spatial_ref,
    )


def _web_mercator_to_wgs84(xmin: float, ymin: float, xmax: float, ymax: float) -> list[float]:
    """Convert Web Mercator bounds to WGS84."""
    def _to_lng(x: float) -> float:
        return x * 180 / 20037508.342789244

    def _to_lat(y: float) -> float:
        return (2 * math.atan(math.exp(y * math.pi / 20037508.342789244)) - math.pi / 2) * 180 / math.pi

    return [
        max(-180, _to_lng(xmin)),
        max(-85.051129, _to_lat(ymin)),
        min(180, _to_lng(xmax)),
        min(85.051129, _to_lat(ymax)),
    ]


def _tiles_for_bbox(bbox: list[float], zoom: int) -> list[tuple[int, int, int]]:
    """Generate tile coordinates covering a bounding box."""
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


def convert(
    service_url: str,
    output_path: str,
    *,
    token: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    row_group_size: int = 200,
    verbose: bool = False,
) -> dict[str, Any]:
    """Convert an ArcGIS MapServer to TileQuet format.

    Args:
        service_url: MapServer REST URL (e.g., .../MapServer).
        output_path: Path to output .parquet file.
        token: Optional ArcGIS authentication token.
        bbox: Bounding box filter (west, south, east, north) in WGS84.
        min_zoom: Minimum zoom level (overrides service default).
        max_zoom: Maximum zoom level (overrides service default).
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    service_url = service_url.rstrip("/")

    logger.info("Fetching metadata from %s...", service_url)
    meta = get_mapserver_metadata(service_url, token)

    if verbose:
        logger.info("MapServer: %s, format: %s, zoom: %d-%d",
                     meta.name, meta.tile_format, meta.min_zoom, meta.max_zoom)

    effective_min_zoom = min_zoom if min_zoom is not None else meta.min_zoom
    effective_max_zoom = max_zoom if max_zoom is not None else min(meta.max_zoom, 14)

    effective_bounds = list(bbox) if bbox else (meta.bounds or [-180, -85.051129, 180, 85.051129])

    # Build tile URL template
    tile_url_base = f"{service_url}/tile"

    tiles = []
    tiles_fetched = 0
    tiles_skipped = 0

    client = _get_http_client()
    try:
        for z in range(effective_min_zoom, effective_max_zoom + 1):
            tile_coords = _tiles_for_bbox(effective_bounds, z)
            if verbose:
                logger.info("Zoom %d: %d tiles to fetch", z, len(tile_coords))

            for z, x, y in tile_coords:
                tile_url = f"{tile_url_base}/{z}/{y}/{x}"

                try:
                    data = _make_request(tile_url, token=token, return_bytes=True)
                except ValueError:
                    tiles_skipped += 1
                    continue

                if not data or len(data) < 100:
                    tiles_skipped += 1
                    continue

                cell = quadbin.tile_to_cell((x, y, z))
                tiles.append({"tile": cell, "data": data})
                tiles_fetched += 1

                if verbose and tiles_fetched % 100 == 0:
                    logger.info("Fetched %d tiles...", tiles_fetched)

    finally:
        client.close()

    if not tiles:
        raise ValueError("No tiles were fetched from the MapServer")

    tile_type = tile_type_from_format(meta.tile_format)

    if verbose:
        logger.info("Fetched %d tiles (%d skipped)", tiles_fetched, tiles_skipped)

    metadata = create_metadata(
        tile_type=tile_type,
        tile_format=meta.tile_format,
        bounds=effective_bounds,
        center=[
            (effective_bounds[0] + effective_bounds[2]) / 2,
            (effective_bounds[1] + effective_bounds[3]) / 2,
            effective_min_zoom,
        ],
        min_zoom=effective_min_zoom,
        max_zoom=effective_max_zoom,
        num_tiles=len(tiles),
        name=meta.name,
        description=meta.description,
        source_format="arcgis_mapserver",
    )

    write_tilequet(output_path, tiles, metadata, row_group_size=row_group_size)

    if verbose:
        logger.info("Written %d tiles to %s", len(tiles), output_path)

    return {
        "num_tiles": len(tiles),
        "tile_type": tile_type,
        "tile_format": meta.tile_format,
        "min_zoom": effective_min_zoom,
        "max_zoom": effective_max_zoom,
        "tiles_skipped": tiles_skipped,
    }
