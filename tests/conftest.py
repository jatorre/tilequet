"""Pytest configuration and fixtures for TileQuet tests."""

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import quadbin


@pytest.fixture
def tmp_dir():
    """Provide a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def test_data_dir():
    """Return the test data directory."""
    return Path(__file__).parent


@pytest.fixture
def examples_dir():
    """Return the examples directory."""
    return Path(__file__).parent.parent / "examples"


@pytest.fixture
def sample_tilequet_file(tmp_dir):
    """Create a minimal valid TileQuet file for testing."""
    from tilequet.metadata import create_metadata, write_tilequet

    filepath = tmp_dir / "sample.parquet"

    tiles = []
    for z in range(3):
        for x in range(1 << z):
            for y in range(1 << z):
                cell = quadbin.tile_to_cell((x, y, z))
                tiles.append({
                    "tile": cell,
                    "data": b"\x89PNG\r\n\x1a\n" + b"\x00" * 100,
                })

    metadata = create_metadata(
        tile_type="raster",
        tile_format="png",
        bounds=[-180.0, -85.05, 180.0, 85.05],
        center=[0.0, 0.0, 1],
        min_zoom=0,
        max_zoom=2,
        num_tiles=len(tiles),
        name="Test Tile Set",
        source_format="test",
    )

    write_tilequet(str(filepath), tiles, metadata)
    return filepath


@pytest.fixture
def sample_vector_tilequet_file(tmp_dir):
    """Create a minimal valid TileQuet file with vector (PBF) tiles."""
    from tilequet.metadata import create_metadata, write_tilequet

    filepath = tmp_dir / "vector_sample.parquet"

    tiles = []
    for z in range(2):
        for x in range(1 << z):
            for y in range(1 << z):
                cell = quadbin.tile_to_cell((x, y, z))
                # gzip magic bytes simulate compressed PBF
                tiles.append({
                    "tile": cell,
                    "data": b"\x1f\x8b" + b"\x00" * 50,
                })

    metadata = create_metadata(
        tile_type="vector",
        tile_format="pbf",
        bounds=[-180.0, -85.05, 180.0, 85.05],
        center=[0.0, 0.0, 0],
        min_zoom=0,
        max_zoom=1,
        num_tiles=len(tiles),
        name="Test Vector Set",
        layers=[
            {"id": "buildings", "minzoom": 0, "maxzoom": 1, "fields": {"name": "String"}},
        ],
        source_format="test",
    )

    write_tilequet(str(filepath), tiles, metadata)
    return filepath


@pytest.fixture
def sample_mbtiles_file(tmp_dir):
    """Create a minimal MBTiles file for testing."""
    import sqlite3

    filepath = tmp_dir / "sample.mbtiles"
    conn = sqlite3.connect(str(filepath))

    conn.execute("CREATE TABLE metadata (name TEXT, value TEXT)")
    conn.execute(
        "CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, "
        "tile_row INTEGER, tile_data BLOB)"
    )

    metadata_entries = [
        ("name", "Test MBTiles"),
        ("format", "png"),
        ("bounds", "-180,-85,180,85"),
        ("center", "0,0,2"),
        ("minzoom", "0"),
        ("maxzoom", "2"),
        ("type", "baselayer"),
    ]
    conn.executemany("INSERT INTO metadata VALUES (?, ?)", metadata_entries)

    tile_data = b"\x89PNG\r\n\x1a\n" + b"\x00" * 50
    for z in range(3):
        max_y = (1 << z) - 1
        for x in range(1 << z):
            for y in range(1 << z):
                tms_y = max_y - y
                conn.execute(
                    "INSERT INTO tiles VALUES (?, ?, ?, ?)",
                    (z, x, tms_y, tile_data),
                )

    conn.commit()
    conn.close()
    return filepath


@pytest.fixture
def sample_geopackage_file(tmp_dir):
    """Create a minimal GeoPackage file with tiles for testing."""
    import sqlite3

    filepath = tmp_dir / "sample.gpkg"
    conn = sqlite3.connect(str(filepath))

    # GeoPackage required tables
    conn.execute("""
        CREATE TABLE gpkg_contents (
            table_name TEXT NOT NULL PRIMARY KEY,
            data_type TEXT NOT NULL,
            identifier TEXT,
            description TEXT DEFAULT '',
            last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            min_x DOUBLE,
            min_y DOUBLE,
            max_x DOUBLE,
            max_y DOUBLE,
            srs_id INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE gpkg_tile_matrix (
            table_name TEXT NOT NULL,
            zoom_level INTEGER NOT NULL,
            matrix_width INTEGER NOT NULL,
            matrix_height INTEGER NOT NULL,
            tile_width INTEGER NOT NULL,
            tile_height INTEGER NOT NULL,
            pixel_x_size DOUBLE NOT NULL,
            pixel_y_size DOUBLE NOT NULL,
            CONSTRAINT pk_ttm PRIMARY KEY (table_name, zoom_level)
        )
    """)
    conn.execute("""
        CREATE TABLE test_tiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            zoom_level INTEGER NOT NULL,
            tile_column INTEGER NOT NULL,
            tile_row INTEGER NOT NULL,
            tile_data BLOB NOT NULL
        )
    """)

    conn.execute(
        "INSERT INTO gpkg_contents VALUES (?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?)",
        ("test_tiles", "tiles", "Test GPKG", "Test GeoPackage tiles",
         -180.0, -85.05, 180.0, 85.05, 4326),
    )

    for z in range(3):
        n = 1 << z
        conn.execute(
            "INSERT INTO gpkg_tile_matrix VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("test_tiles", z, n, n, 256, 256, 360.0 / (256 * n), 180.0 / (256 * n)),
        )

    tile_data = b"\x89PNG\r\n\x1a\n" + b"\x00" * 50
    for z in range(3):
        for x in range(1 << z):
            for y in range(1 << z):
                conn.execute(
                    "INSERT INTO test_tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                    (z, x, y, tile_data),
                )

    conn.commit()
    conn.close()
    return filepath


@pytest.fixture
def invalid_parquet_file(tmp_dir):
    """Create a Parquet file that is NOT a valid TileQuet file (wrong schema)."""
    filepath = tmp_dir / "invalid.parquet"

    table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "value": pa.array(["a", "b", "c"], type=pa.string()),
    })
    pq.write_table(table, str(filepath))
    return filepath
