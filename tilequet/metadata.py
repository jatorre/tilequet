"""Shared metadata utilities for TileQuet files.

Provides functions for creating, reading, and writing TileQuet metadata.
The metadata is stored as a JSON string in the tile=0 row.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.parquet import SortingColumn

from . import __version__

logger = logging.getLogger(__name__)

TILEQUET_VERSION = "0.1.0"
METADATA_TILE_ID = 0

TILEQUET_SCHEMA = pa.schema([
    ("tile", pa.uint64()),
    ("metadata", pa.string()),
    ("data", pa.binary()),
])


def build_tilejson(
    *,
    bounds: list[float] | None = None,
    center: list[float] | None = None,
    min_zoom: int = 0,
    max_zoom: int = 22,
    name: str | None = None,
    description: str | None = None,
    attribution: str | None = None,
    vector_layers: list[dict] | None = None,
) -> dict[str, Any]:
    """Build a TileJSON 3.0.0 compliant object.

    This is stored in the row-0 metadata so downstream tools can
    reconstruct a standards-compliant TileJSON for serving.

    Args:
        bounds: [west, south, east, north] in WGS84.
        center: [lon, lat, zoom].
        min_zoom: Minimum zoom level.
        max_zoom: Maximum zoom level.
        name: Human-readable name.
        description: Human-readable description.
        attribution: Attribution string.
        vector_layers: TileJSON vector_layers array.

    Returns:
        TileJSON 3.0.0 dictionary.
    """
    tj: dict[str, Any] = {
        "tilejson": "3.0.0",
        "tiles": [],
        "bounds": bounds or [-180, -85.051129, 180, 85.051129],
        "minzoom": min_zoom,
        "maxzoom": max_zoom,
    }
    if center is not None:
        tj["center"] = center
    if name is not None:
        tj["name"] = name
    if description is not None:
        tj["description"] = description
    if attribution is not None:
        tj["attribution"] = attribution
    if vector_layers is not None:
        tj["vector_layers"] = vector_layers
    return tj


def create_metadata(
    *,
    tile_type: str,
    tile_format: str,
    bounds: list[float] | None = None,
    center: list[float] | None = None,
    min_zoom: int = 0,
    max_zoom: int = 14,
    num_tiles: int = 0,
    name: str | None = None,
    description: str | None = None,
    attribution: str | None = None,
    layers: list[dict] | None = None,
    source_format: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Create a TileQuet metadata dictionary.

    Args:
        tile_type: Type of tiles: "vector", "raster", or "3d".
        tile_format: Format of tile data: "pbf", "png", "jpeg", "webp", "gltf", "glb", "b3dm", "pnts".
        bounds: [west, south, east, north] in EPSG:4326.
        center: [lon, lat, zoom].
        min_zoom: Minimum zoom level.
        max_zoom: Maximum zoom level.
        num_tiles: Number of tile rows (excluding metadata row).
        name: Human-readable name.
        description: Human-readable description.
        attribution: Attribution string (may contain HTML).
        layers: List of layer dicts for vector tiles.
        source_format: Source format identifier (e.g., "pmtiles", "mbtiles").

    Returns:
        Metadata dictionary ready to be serialized as JSON.
    """
    metadata = {
        "file_format": "tilequet",
        "version": TILEQUET_VERSION,
        "tile_type": tile_type,
        "tile_format": tile_format,
        "bounds": bounds or [-180, -85.051129, 180, 85.051129],
        "bounds_crs": "EPSG:4326",
        "center": center,
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
        "num_tiles": num_tiles,
        "tiling": {
            "scheme": "quadbin",
        },
    }

    if name is not None:
        metadata["name"] = name
    if description is not None:
        metadata["description"] = description
    if attribution is not None:
        metadata["attribution"] = attribution
    if layers is not None:
        metadata["layers"] = layers

    metadata["processing"] = {
        "source_format": source_format,
        "created_by": f"tilequet-io {__version__}",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Merge any extra fields (e.g., tileset_json for 3D Tiles)
    metadata.update(extra)

    return metadata


def read_metadata(filepath: str) -> dict[str, Any]:
    """Read metadata from a TileQuet file.

    Args:
        filepath: Path to a TileQuet Parquet file.

    Returns:
        Parsed metadata dict.

    Raises:
        ValueError: If no metadata row found.
    """
    table = pq.read_table(
        filepath,
        filters=[("tile", "=", 0)],
        columns=["tile", "metadata"],
    )

    if len(table) == 0:
        raise ValueError(f"No metadata row (tile=0) found in {filepath}")

    metadata_str = table.column("metadata")[0].as_py()
    if metadata_str is None:
        raise ValueError(f"Metadata column is NULL in tile=0 row of {filepath}")

    return json.loads(metadata_str)


def write_tilequet(
    output_path: str,
    tiles: list[dict],
    metadata: dict,
    *,
    row_group_size: int = 200,
) -> None:
    """Write a TileQuet Parquet file.

    Args:
        output_path: Path for the output Parquet file.
        tiles: List of dicts with 'tile' (uint64) and 'data' (bytes) keys.
        metadata: Metadata dictionary to store in tile=0 row.
        row_group_size: Rows per Parquet row group.
    """
    # Sort tiles by tile ID for optimal row group pruning
    tiles.sort(key=lambda t: t["tile"])

    logger.info("Sorting %d tiles by tile ID for optimized row group pruning...", len(tiles))

    # Build column arrays
    tile_ids = [METADATA_TILE_ID] + [t["tile"] for t in tiles]
    metadata_col = [json.dumps(metadata)] + [None] * len(tiles)
    data_col = [None] + [t["data"] for t in tiles]

    table = pa.table(
        {"tile": tile_ids, "metadata": metadata_col, "data": data_col},
        schema=TILEQUET_SCHEMA,
    )

    # Add file-level Parquet key-value metadata for fast identification
    existing_meta = table.schema.metadata or {}
    existing_meta[b"tilequet:version"] = TILEQUET_VERSION.encode()
    table = table.replace_schema_metadata(existing_meta)

    pq.write_table(
        table,
        output_path,
        compression="none",
        row_group_size=row_group_size,
        write_page_index=True,
        write_statistics=True,
        sorting_columns=[SortingColumn(0)],
    )
