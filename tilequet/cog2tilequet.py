"""Convert Cloud Optimized GeoTIFF (COG) to TileQuet format.

Only imports COGs that are perfectly aligned with the Web Mercator tile grid:
- CRS must be EPSG:3857
- Internal tiles must be 256x256 or 512x512
- Pixel resolution must match a standard zoom level
- Origin must align with tile boundaries
- Overviews must exist with powers-of-2 factors

If the COG doesn't meet these criteria, the converter will suggest using
RaQuet (https://github.com/jatorre/raquet) for raw band-level storage.

Requires: pip install 'tilequet-io[cog]'  (rasterio + Pillow)
"""

from __future__ import annotations

import io
import logging
import math
from typing import Any

import quadbin

from .metadata import build_tilejson, create_metadata, write_tilequet

logger = logging.getLogger(__name__)

WEB_MERCATOR_ORIGIN = 20037508.342789244
WEB_MERCATOR_WORLD = 2 * WEB_MERCATOR_ORIGIN


def _get_deps():
    """Import and return (rasterio, Image, np)."""
    try:
        import rasterio
    except ImportError:
        raise ImportError(
            "The 'rasterio' package is required for COG conversion. "
            "Install with: pip install 'tilequet-io[cog]'"
        )
    try:
        from PIL import Image
    except ImportError:
        raise ImportError(
            "The 'Pillow' package is required for COG conversion. "
            "Install with: pip install 'tilequet-io[cog]'"
        )
    import numpy as np
    return rasterio, Image, np


def _validate_cog_alignment(src) -> tuple[bool, list[str], dict]:
    """Validate that a COG is perfectly aligned with the Web Mercator tile grid.

    Returns:
        (is_valid, errors, info_dict)
    """
    errors = []
    info = {}

    # 1. Check CRS is EPSG:3857
    if src.crs is None:
        errors.append("No CRS defined.")
    else:
        epsg = src.crs.to_epsg()
        if epsg != 3857:
            errors.append(
                f"CRS is EPSG:{epsg} (need EPSG:3857 Web Mercator)."
            )
        info["crs"] = f"EPSG:{epsg}" if epsg else str(src.crs)

    # 2. Check block size
    block_shapes = src.block_shapes
    bh, bw = block_shapes[0]
    if (bh, bw) not in ((256, 256), (512, 512)):
        errors.append(f"Block size is {bw}x{bh} (need 256x256 or 512x512).")

    if not src.is_tiled:
        errors.append("File is not internally tiled (striped layout).")

    block_size = bw
    info["block_size"] = block_size

    # 3. Check pixel resolution matches a zoom level
    pixel_size = abs(src.transform.a)
    if pixel_size <= 0:
        errors.append("Invalid pixel size.")
    else:
        zoom_float = math.log2(WEB_MERCATOR_WORLD / (pixel_size * block_size))
        zoom = round(zoom_float)
        if abs(zoom_float - zoom) > 0.01:
            errors.append(
                f"Pixel resolution {pixel_size:.6f}m doesn't align with any zoom level "
                f"(closest: z{zoom}, off by {abs(zoom_float - zoom):.4f})."
            )
        info["native_zoom"] = max(0, min(30, zoom))
        info["pixel_size_meters"] = pixel_size

    # 4. Check origin alignment with tile grid
    if "native_zoom" in info:
        zoom = info["native_zoom"]
        tile_size_meters = WEB_MERCATOR_WORLD / (1 << zoom)

        x_origin = src.transform.c
        y_origin = src.transform.f

        x_offset = (x_origin - (-WEB_MERCATOR_ORIGIN)) / tile_size_meters
        y_offset = (WEB_MERCATOR_ORIGIN - y_origin) / tile_size_meters

        if abs(x_offset - round(x_offset)) > 0.001:
            errors.append(
                f"X origin ({x_origin:.2f}m) not aligned with tile grid at z{zoom}."
            )
        if abs(y_offset - round(y_offset)) > 0.001:
            errors.append(
                f"Y origin ({y_origin:.2f}m) not aligned with tile grid at z{zoom}."
            )

        info["tile_x_start"] = round(x_offset)
        info["tile_y_start"] = round(y_offset)

    # 5. Check overviews
    overviews = src.overviews(1)
    info["overviews"] = overviews

    if not overviews:
        errors.append("No overviews found. COGs need overview levels for lower zoom levels.")
    else:
        for ovr in overviews:
            if ovr & (ovr - 1) != 0:
                errors.append(f"Overview factor {ovr} is not a power of 2.")

    info["num_bands"] = src.count
    info["width"] = src.width
    info["height"] = src.height
    info["dtypes"] = list(src.dtypes)
    info["bounds_3857"] = list(src.bounds)
    info["nodata"] = src.nodata
    info["color_interp"] = [ci.name for ci in src.colorinterp]

    return len(errors) == 0, errors, info


def _web_mercator_to_wgs84_bounds(
    xmin: float, ymin: float, xmax: float, ymax: float,
) -> list[float]:
    """Convert Web Mercator bounds to WGS84."""
    def _to_lng(x: float) -> float:
        return x * 180 / WEB_MERCATOR_ORIGIN

    def _to_lat(y: float) -> float:
        return (2 * math.atan(math.exp(y * math.pi / WEB_MERCATOR_ORIGIN)) - math.pi / 2) * 180 / math.pi

    return [
        max(-180, _to_lng(xmin)),
        max(-85.051129, _to_lat(ymin)),
        min(180, _to_lng(xmax)),
        min(85.051129, _to_lat(ymax)),
    ]


def _encode_tile(data, Image, np, *, image_format: str = "png") -> bytes | None:
    """Encode numpy array (bands, h, w) to image bytes. Returns None if tile is empty."""
    bands, h, w = data.shape

    # Skip fully empty/nodata tiles
    if np.all(data == 0):
        return None
    # Skip fully transparent tiles (RGBA with alpha = 0)
    if bands == 4 and np.all(data[3] == 0):
        return None

    if bands == 1:
        img = Image.fromarray(data[0], mode="L")
    elif bands == 3:
        img = Image.fromarray(np.transpose(data, (1, 2, 0)), mode="RGB")
    elif bands == 4:
        img = Image.fromarray(np.transpose(data, (1, 2, 0)), mode="RGBA")
    elif bands == 2:
        # Grayscale + alpha
        rgba = np.zeros((h, w, 4), dtype=data.dtype)
        rgba[:, :, 0] = data[0]
        rgba[:, :, 1] = data[0]
        rgba[:, :, 2] = data[0]
        rgba[:, :, 3] = data[1]
        img = Image.fromarray(rgba, mode="RGBA")
    else:
        # Take first 3 bands as RGB
        img = Image.fromarray(np.transpose(data[:3], (1, 2, 0)), mode="RGB")

    buf = io.BytesIO()
    fmt = "PNG" if image_format == "png" else "JPEG"
    img.save(buf, format=fmt)
    return buf.getvalue()


def convert(
    cog_path: str,
    output_path: str,
    *,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    image_format: str = "png",
    row_group_size: int = 200,
    verbose: bool = False,
) -> dict[str, Any]:
    """Convert a tile-aligned COG to TileQuet format.

    Only works with COGs that are perfectly aligned with the Web Mercator
    tile grid (EPSG:3857, 256x256 blocks, aligned origin, power-of-2
    overviews). For non-aligned rasters, use RaQuet instead.

    Args:
        cog_path: Path or URL to the COG file.
        output_path: Path to output .parquet file.
        min_zoom: Minimum zoom level (default: lowest available).
        max_zoom: Maximum zoom level (default: native resolution).
        image_format: Output tile format ('png' or 'jpeg').
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    rasterio, Image, np = _get_deps()
    from rasterio.windows import Window

    logger.info("Opening COG: %s", cog_path)

    with rasterio.open(cog_path) as src:
        # Strict validation
        is_valid, errors, info = _validate_cog_alignment(src)

        if not is_valid:
            msg = "COG is not perfectly aligned with the Web Mercator tile grid:\n"
            msg += "\n".join(f"  - {e}" for e in errors)
            msg += (
                "\n\nThis COG cannot be imported into TileQuet without resampling."
                "\nConsider using RaQuet (https://github.com/jatorre/raquet) for"
                "\nraw band-level storage of non-aligned rasters."
            )
            raise ValueError(msg)

        native_zoom = info["native_zoom"]
        block_size = info["block_size"]
        overviews = info["overviews"]
        tile_x_start = info["tile_x_start"]
        tile_y_start = info["tile_y_start"]

        if verbose:
            logger.info("COG validated: %dx%d, %d bands (%s), block %dx%d",
                        info["width"], info["height"], info["num_bands"],
                        ", ".join(info["dtypes"]), block_size, block_size)
            logger.info("Native zoom: %d, overviews: %s", native_zoom, overviews)
            logger.info("Tile grid start: (%d, %d)", tile_x_start, tile_y_start)
            logger.info("Color interpretation: %s", ", ".join(info["color_interp"]))

        # Determine available zoom levels: native zoom + one per overview
        available_zooms = {native_zoom: 1}  # zoom -> overview_factor
        for ovr in overviews:
            z = native_zoom - int(math.log2(ovr))
            if z >= 0:
                available_zooms[z] = ovr

        effective_min_zoom = min_zoom if min_zoom is not None else min(available_zooms)
        effective_max_zoom = max_zoom if max_zoom is not None else native_zoom

        if verbose:
            logger.info("Available zooms: %s", sorted(available_zooms.keys()))
            logger.info("Extracting zooms: %d-%d", effective_min_zoom, effective_max_zoom)

        tiles_per_row = info["width"] // block_size
        tiles_per_col = info["height"] // block_size

        tiles = []
        tiles_skipped = 0

        for z in sorted(available_zooms.keys()):
            if z < effective_min_zoom or z > effective_max_zoom:
                continue

            ovr_factor = available_zooms[z]

            # At this zoom, how many tiles does the COG cover?
            z_tiles_x = max(1, tiles_per_row // ovr_factor)
            z_tiles_y = max(1, tiles_per_col // ovr_factor)

            # Tile coordinates in the global grid at this zoom
            z_tile_x_start = tile_x_start // ovr_factor
            z_tile_y_start = tile_y_start // ovr_factor

            if verbose:
                logger.info("Zoom %d: %dx%d tiles (overview factor %d)",
                            z, z_tiles_x, z_tiles_y, ovr_factor)

            for ty in range(z_tiles_y):
                for tx in range(z_tiles_x):
                    # Read window in native resolution pixel coordinates
                    px_x = tx * block_size * ovr_factor
                    px_y = ty * block_size * ovr_factor
                    px_w = block_size * ovr_factor
                    px_h = block_size * ovr_factor

                    # Clamp to image bounds
                    if px_x + px_w > info["width"]:
                        px_w = info["width"] - px_x
                    if px_y + px_h > info["height"]:
                        px_h = info["height"] - px_y

                    if px_w <= 0 or px_h <= 0:
                        continue

                    window = Window(px_x, px_y, px_w, px_h)

                    try:
                        # GDAL automatically uses the right overview level
                        # when out_shape differs from the window size
                        data = src.read(
                            window=window,
                            out_shape=(info["num_bands"], block_size, block_size),
                        )
                    except Exception as e:
                        if verbose:
                            logger.warning("Failed to read tile z%d/%d/%d: %s",
                                           z, z_tile_x_start + tx,
                                           z_tile_y_start + ty, e)
                        tiles_skipped += 1
                        continue

                    # Replace nodata with 0/transparent
                    if info["nodata"] is not None:
                        data = np.where(data == info["nodata"], 0, data)

                    # Ensure uint8 for image encoding
                    if data.dtype != np.uint8:
                        if np.issubdtype(data.dtype, np.floating):
                            data = np.clip(data * 255, 0, 255).astype(np.uint8)
                        elif np.issubdtype(data.dtype, np.integer):
                            data = np.clip(data, 0, 255).astype(np.uint8)

                    encoded = _encode_tile(data, Image, np,
                                           image_format=image_format)
                    if encoded is None:
                        tiles_skipped += 1
                        continue

                    tile_x = z_tile_x_start + tx
                    tile_y = z_tile_y_start + ty
                    cell = quadbin.tile_to_cell((tile_x, tile_y, z))
                    tiles.append({"tile": cell, "data": encoded})

                    if verbose and len(tiles) % 100 == 0:
                        logger.info("Extracted %d tiles...", len(tiles))

    if not tiles:
        raise ValueError("No non-empty tiles were extracted from the COG")

    tile_format = image_format
    tile_type = "raster"

    if verbose:
        logger.info("Extracted %d tiles (%d empty skipped)",
                     len(tiles), tiles_skipped)

    # Convert bounds to WGS84
    bounds = _web_mercator_to_wgs84_bounds(*info["bounds_3857"])

    center = [
        (bounds[0] + bounds[2]) / 2,
        (bounds[1] + bounds[3]) / 2,
        effective_min_zoom,
    ]

    tilejson = build_tilejson(
        bounds=bounds,
        center=center,
        min_zoom=effective_min_zoom,
        max_zoom=effective_max_zoom,
    )

    # COG-specific metadata so consumers know exactly what was imported
    cog_info = {
        "native_zoom": native_zoom,
        "block_size": block_size,
        "num_bands": info["num_bands"],
        "color_interpretation": info["color_interp"],
        "original_dtypes": info["dtypes"],
        "overview_factors": overviews,
        "source_crs": info.get("crs", "EPSG:3857"),
        "pixel_size_meters": info.get("pixel_size_meters"),
        "image_dimensions": [info["width"], info["height"]],
        "encoding": image_format,
    }

    metadata = create_metadata(
        tile_type=tile_type,
        tile_format=tile_format,
        bounds=bounds,
        center=center,
        min_zoom=effective_min_zoom,
        max_zoom=effective_max_zoom,
        num_tiles=len(tiles),
        source_format="cog",
        tilejson=tilejson,
        cog=cog_info,
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
