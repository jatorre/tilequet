"""Convert PMTiles files to TileQuet format.

PMTiles is a single-file archive format for pyramids of map tiles.
This module reads tiles from a PMTiles file and writes them
to a TileQuet Parquet file.

Requires the `pmtiles` package: pip install pmtiles
"""

import json
import logging
from pathlib import Path

import quadbin

from .metadata import create_metadata, write_tilequet

logger = logging.getLogger(__name__)

# PMTiles tile type constants
PMTILES_TILE_TYPE_MVT = 1
PMTILES_TILE_TYPE_PNG = 2
PMTILES_TILE_TYPE_JPEG = 3
PMTILES_TILE_TYPE_WEBP = 4
PMTILES_TILE_TYPE_AVIF = 5

PMTILES_FORMAT_MAP = {
    PMTILES_TILE_TYPE_MVT: ("vector", "pbf"),
    PMTILES_TILE_TYPE_PNG: ("raster", "png"),
    PMTILES_TILE_TYPE_JPEG: ("raster", "jpeg"),
    PMTILES_TILE_TYPE_WEBP: ("raster", "webp"),
    PMTILES_TILE_TYPE_AVIF: ("raster", "avif"),
}


def _check_pmtiles():
    """Check that pmtiles package is available."""
    try:
        import pmtiles  # noqa: F401

        return True
    except ImportError:
        raise ImportError(
            "The 'pmtiles' package is required for PMTiles conversion. "
            "Install it with: pip install 'tilequet-io[pmtiles]'"
        )


def convert(
    input_path: str,
    output_path: str,
    row_group_size: int = 200,
    verbose: bool = False,
) -> dict:
    """Convert a PMTiles file to TileQuet format.

    Args:
        input_path: Path to input .pmtiles file.
        output_path: Path to output .parquet file.
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    _check_pmtiles()

    from pmtiles.reader import MmapSource, Reader as PMTilesReader, all_tiles

    input_path = str(input_path)

    with open(input_path, "rb") as f:
        source = MmapSource(f)
        reader = PMTilesReader(source)

        # Read PMTiles header/metadata
        pm_header = reader.header()
        pm_metadata = reader.metadata()

        if verbose:
            logger.info("PMTiles header: %s", pm_header)
            logger.info("PMTiles metadata keys: %s", list(pm_metadata.keys()) if pm_metadata else "none")

        # Determine tile type and format
        # tile_type may be an enum (TileType.MVT) or int
        raw_tile_type = pm_header.get("tile_type", PMTILES_TILE_TYPE_MVT)
        tile_type_id = raw_tile_type.value if hasattr(raw_tile_type, "value") else raw_tile_type
        tile_type, tile_format = PMTILES_FORMAT_MAP.get(
            tile_type_id, ("raster", "png")
        )

        # Extract bounds
        bounds = [
            pm_header.get("min_lon_e7", -180_0000000) / 1e7,
            pm_header.get("min_lat_e7", -85_0000000) / 1e7,
            pm_header.get("max_lon_e7", 180_0000000) / 1e7,
            pm_header.get("max_lat_e7", 85_0000000) / 1e7,
        ]

        # Extract zoom range
        min_zoom = pm_header.get("min_zoom", 0)
        max_zoom = pm_header.get("max_zoom", 14)

        # Center
        center_lon = pm_header.get("center_lon_e7", 0) / 1e7
        center_lat = pm_header.get("center_lat_e7", 0) / 1e7
        center_zoom = pm_header.get("center_zoom", min_zoom)
        center = [center_lon, center_lat, center_zoom]

        # Read all tiles using the all_tiles iterator
        tiles = []
        tile_count = 0

        for (z, x, y), tile_data in all_tiles(source):
            cell = quadbin.tile_to_cell((x, y, z))
            tiles.append({"tile": cell, "data": tile_data})
            tile_count += 1

            if verbose and tile_count % 10000 == 0:
                logger.info("Read %d tiles...", tile_count)

        if verbose:
            logger.info("Read %d tiles total from PMTiles", tile_count)

        # Extract layer information for vector tiles
        layers = None
        if tile_type == "vector" and pm_metadata:
            vector_layers = pm_metadata.get("vector_layers", [])
            if vector_layers:
                layers = []
                for vl in vector_layers:
                    layer = {"id": vl["id"]}
                    if "description" in vl:
                        layer["description"] = vl["description"]
                    if "minzoom" in vl:
                        layer["minzoom"] = vl["minzoom"]
                    if "maxzoom" in vl:
                        layer["maxzoom"] = vl["maxzoom"]
                    if "fields" in vl:
                        layer["fields"] = vl["fields"]
                    layers.append(layer)

        # Build metadata
        name = None
        description = None
        attribution = None
        if pm_metadata:
            name = pm_metadata.get("name")
            description = pm_metadata.get("description")
            attribution = pm_metadata.get("attribution")

        metadata = create_metadata(
            tile_type=tile_type,
            tile_format=tile_format,
            bounds=bounds,
            center=center,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            num_tiles=len(tiles),
            name=name,
            description=description,
            attribution=attribution,
            layers=layers,
            source_format="pmtiles",
        )

        # Write TileQuet file
        write_tilequet(output_path, tiles, metadata, row_group_size=row_group_size)

        if verbose:
            logger.info("Written %d tiles to %s", len(tiles), output_path)

        return {
            "num_tiles": len(tiles),
            "tile_type": tile_type,
            "tile_format": tile_format,
            "min_zoom": min_zoom,
            "max_zoom": max_zoom,
        }
