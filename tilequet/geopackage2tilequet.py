"""Convert GeoPackage tile tables to TileQuet format.

GeoPackage is an OGC standard based on SQLite for storing geospatial data.
This module reads tiles from a GeoPackage file and writes them
to a TileQuet Parquet file.

Uses Python's built-in sqlite3 module — no extra dependencies required.
"""

import logging
import sqlite3

import quadbin

from .metadata import create_metadata, write_tilequet
from .mbtiles2tilequet import detect_tile_format, tile_type_from_format

logger = logging.getLogger(__name__)


def _get_tile_tables(conn: sqlite3.Connection) -> list[str]:
    """Get list of tile pyramid user data tables from GeoPackage."""
    cursor = conn.execute(
        "SELECT table_name FROM gpkg_contents WHERE data_type = 'tiles'"
    )
    return [row[0] for row in cursor.fetchall()]


def _get_tile_matrix(conn: sqlite3.Connection, table_name: str) -> list[dict]:
    """Get tile matrix metadata for a tile table."""
    cursor = conn.execute(
        "SELECT zoom_level, matrix_width, matrix_height, tile_width, tile_height "
        "FROM gpkg_tile_matrix WHERE table_name = ? ORDER BY zoom_level",
        (table_name,),
    )
    return [
        {
            "zoom_level": row[0],
            "matrix_width": row[1],
            "matrix_height": row[2],
            "tile_width": row[3],
            "tile_height": row[4],
        }
        for row in cursor.fetchall()
    ]


def convert(
    input_path: str,
    output_path: str,
    *,
    table_name: str | None = None,
    row_group_size: int = 200,
    verbose: bool = False,
) -> dict:
    """Convert a GeoPackage tile table to TileQuet format.

    Args:
        input_path: Path to input .gpkg file.
        output_path: Path to output .parquet file.
        table_name: Name of the tile table to convert. If None, uses the first one found.
        row_group_size: Parquet row group size.
        verbose: Enable verbose logging.

    Returns:
        Dict with conversion statistics.
    """
    conn = sqlite3.connect(f"file:{input_path}?mode=ro", uri=True)

    try:
        # Find tile tables
        tile_tables = _get_tile_tables(conn)
        if not tile_tables:
            raise ValueError("No tile tables found in GeoPackage")

        if table_name is None:
            table_name = tile_tables[0]
            if verbose:
                logger.info("Using tile table: %s", table_name)
        elif table_name not in tile_tables:
            raise ValueError(
                f"Table '{table_name}' not found. Available: {tile_tables}"
            )

        # Get tile matrix info
        tile_matrix = _get_tile_matrix(conn, table_name)
        if verbose:
            logger.info("Tile matrix: %d zoom levels", len(tile_matrix))

        # Get contents metadata
        cursor = conn.execute(
            "SELECT identifier, description, min_x, min_y, max_x, max_y, srs_id "
            "FROM gpkg_contents WHERE table_name = ?",
            (table_name,),
        )
        contents = cursor.fetchone()
        gpkg_name = contents[0] if contents else table_name
        gpkg_desc = contents[1] if contents else None
        bounds = [contents[2], contents[3], contents[4], contents[5]] if contents else None
        # Count tiles and get zoom range
        cursor = conn.execute(
            f"SELECT MIN(zoom_level), MAX(zoom_level), COUNT(*) FROM \"{table_name}\""
        )
        min_zoom, max_zoom, total_tiles = cursor.fetchone()

        if total_tiles == 0:
            raise ValueError("GeoPackage tile table contains no tiles")

        if verbose:
            logger.info("Found %d tiles, zoom %d-%d", total_tiles, min_zoom, max_zoom)

        # Detect format from first tile
        cursor = conn.execute(
            f"SELECT tile_data FROM \"{table_name}\" LIMIT 1"
        )
        sample = cursor.fetchone()[0]
        tile_format = detect_tile_format(sample)
        tile_type = tile_type_from_format(tile_format)

        # Read all tiles
        # GeoPackage uses the same Y convention as XYZ (top-left origin)
        tiles = []
        cursor = conn.execute(
            f"SELECT zoom_level, tile_column, tile_row, tile_data FROM \"{table_name}\""
        )

        for zoom, x, y, data in cursor:
            cell = quadbin.tile_to_cell((x, y, zoom))
            tiles.append({"tile": cell, "data": data})

        if verbose:
            logger.info("Read %d tiles from GeoPackage", len(tiles))

        # Build center from bounds
        center = None
        if bounds:
            center = [
                (bounds[0] + bounds[2]) / 2,
                (bounds[1] + bounds[3]) / 2,
                min_zoom,
            ]

        # Build metadata
        metadata = create_metadata(
            tile_type=tile_type,
            tile_format=tile_format,
            bounds=bounds,
            center=center,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            num_tiles=len(tiles),
            name=gpkg_name,
            description=gpkg_desc,
            source_format="geopackage",
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
