"""Convert MBTiles files to TileQuet format.

MBTiles is a SQLite-based format for storing map tile sets.
This module reads tiles from an MBTiles file and writes them
to a TileQuet Parquet file.
"""

import logging
import sqlite3

import quadbin

from .metadata import build_tilejson, create_metadata, write_tilequet

logger = logging.getLogger(__name__)

# MBTiles format detection from tile data magic bytes
FORMAT_SIGNATURES = {
    b"\x89PNG": "png",
    b"\xff\xd8\xff": "jpeg",
    b"RIFF": "webp",
    b"\x1a\x45\xdf\xa3": "pbf",  # gzip-compressed PBF starts differently
}


def detect_tile_format(tile_data: bytes) -> str:
    """Detect tile format from magic bytes."""
    if tile_data[:4] == b"\x89PNG":
        return "png"
    if tile_data[:3] == b"\xff\xd8\xff":
        return "jpeg"
    if tile_data[:4] == b"RIFF":
        return "webp"
    # gzip-compressed data (common for PBF vector tiles)
    if tile_data[:2] == b"\x1f\x8b":
        return "pbf"
    # Uncompressed PBF
    if len(tile_data) > 0 and tile_data[0] in (0x0A, 0x12, 0x1A, 0x22):
        return "pbf"
    return "unknown"


def tile_type_from_format(fmt: str) -> str:
    """Determine tile_type from tile_format."""
    if fmt == "pbf":
        return "vector"
    if fmt in ("png", "jpeg", "webp"):
        return "raster"
    return "raster"  # default assumption


def read_mbtiles_metadata(conn: sqlite3.Connection) -> dict:
    """Read metadata from MBTiles metadata table."""
    cursor = conn.execute("SELECT name, value FROM metadata")
    return dict(cursor.fetchall())


def tms_to_xyz_y(zoom: int, tms_y: int) -> int:
    """Convert TMS Y coordinate to XYZ Y coordinate."""
    return (1 << zoom) - 1 - tms_y


def convert(
    input_path: str,
    output_path: str,
    row_group_size: int = 200,
    verbose: bool = False,
) -> dict:
    """Convert an MBTiles file to TileQuet format.

    Args:
        input_path: Path to input .mbtiles file.
        output_path: Path to output .parquet file.
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    conn = sqlite3.connect(f"file:{input_path}?mode=ro", uri=True)

    try:
        # Read MBTiles metadata
        mb_meta = read_mbtiles_metadata(conn)
        if verbose:
            logger.info("MBTiles metadata: %s", mb_meta)

        # Get tile format from metadata or detect from first tile
        tile_format = mb_meta.get("format", None)
        if tile_format == "pbf":
            tile_type = "vector"
        elif tile_format in ("png", "jpg", "jpeg", "webp"):
            if tile_format == "jpg":
                tile_format = "jpeg"
            tile_type = "raster"
        else:
            tile_format = None
            tile_type = None

        # Count tiles and get zoom range
        cursor = conn.execute(
            "SELECT MIN(zoom_level), MAX(zoom_level), COUNT(*) FROM tiles"
        )
        min_zoom, max_zoom, total_tiles = cursor.fetchone()

        if total_tiles == 0:
            raise ValueError("MBTiles file contains no tiles")

        if verbose:
            logger.info(
                "Found %d tiles, zoom %d-%d", total_tiles, min_zoom, max_zoom
            )

        # Detect format from first tile if not in metadata
        if tile_format is None:
            cursor = conn.execute("SELECT tile_data FROM tiles LIMIT 1")
            sample = cursor.fetchone()[0]
            tile_format = detect_tile_format(sample)
            tile_type = tile_type_from_format(tile_format)

        # Parse bounds
        bounds_str = mb_meta.get("bounds", "-180,-85.05,180,85.05")
        bounds = [float(x.strip()) for x in bounds_str.split(",")]

        # Parse center
        center_str = mb_meta.get("center", "0,0,2")
        center_parts = [x.strip() for x in center_str.split(",")]
        center = [float(center_parts[0]), float(center_parts[1]), int(float(center_parts[2]))]

        # Read all tiles
        tiles = []
        cursor = conn.execute(
            "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles"
        )

        for zoom, x, tms_y, data in cursor:
            # Convert TMS Y to XYZ Y
            y = tms_to_xyz_y(zoom, tms_y)
            # Encode as QUADBIN
            cell = quadbin.tile_to_cell((x, y, zoom))
            tiles.append({"tile": cell, "data": data})

        if verbose:
            logger.info("Read %d tiles from MBTiles", len(tiles))

        # Extract vector_layers from MBTiles json metadata
        vector_layers = None
        layers = None
        if tile_type == "vector" and "json" in mb_meta:
            import json

            try:
                mb_json = json.loads(mb_meta["json"])
                vector_layers = mb_json.get("vector_layers")
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
            except (json.JSONDecodeError, KeyError):
                pass

        # Build TileJSON 3.0.0
        tilejson = build_tilejson(
            bounds=bounds,
            center=center,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            name=mb_meta.get("name"),
            description=mb_meta.get("description"),
            attribution=mb_meta.get("attribution"),
            vector_layers=vector_layers,
        )

        # Build metadata
        metadata = create_metadata(
            tile_type=tile_type,
            tile_format=tile_format,
            bounds=bounds,
            center=center,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            num_tiles=len(tiles),
            name=mb_meta.get("name"),
            description=mb_meta.get("description"),
            attribution=mb_meta.get("attribution"),
            layers=layers,
            source_format="mbtiles",
            tilejson=tilejson,
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

    finally:
        conn.close()
