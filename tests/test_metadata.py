"""Tests for TileQuet metadata utilities."""


from tilequet.metadata import create_metadata, read_metadata, write_tilequet


def test_create_metadata():
    """Test metadata creation."""
    metadata = create_metadata(
        tile_type="vector",
        tile_format="pbf",
        bounds=[-180, -85, 180, 85],
        center=[0, 0, 5],
        min_zoom=0,
        max_zoom=14,
        num_tiles=1000,
        name="Test",
        source_format="pmtiles",
    )

    assert metadata["file_format"] == "tilequet"
    assert metadata["version"] == "0.1.0"
    assert metadata["tile_type"] == "vector"
    assert metadata["tile_format"] == "pbf"
    assert metadata["bounds"] == [-180, -85, 180, 85]
    assert metadata["min_zoom"] == 0
    assert metadata["max_zoom"] == 14
    assert metadata["num_tiles"] == 1000
    assert metadata["name"] == "Test"
    assert metadata["tiling"]["scheme"] == "quadbin"
    assert metadata["processing"]["source_format"] == "pmtiles"
    assert "created_at" in metadata["processing"]
    assert "created_by" in metadata["processing"]


def test_create_metadata_defaults():
    """Test metadata creation with default values."""
    metadata = create_metadata(
        tile_type="raster",
        tile_format="png",
    )

    assert metadata["file_format"] == "tilequet"
    assert metadata["bounds"] == [-180, -85.051129, 180, 85.051129]
    assert metadata["bounds_crs"] == "EPSG:4326"
    assert metadata["min_zoom"] == 0
    assert metadata["max_zoom"] == 14
    assert metadata["num_tiles"] == 0
    assert "name" not in metadata


def test_create_metadata_with_layers():
    """Test metadata creation with vector layers."""
    layers = [
        {"id": "roads", "minzoom": 0, "maxzoom": 14},
        {"id": "buildings", "minzoom": 10, "maxzoom": 14},
    ]
    metadata = create_metadata(
        tile_type="vector",
        tile_format="pbf",
        layers=layers,
    )
    assert metadata["layers"] == layers


def test_write_and_read(tmp_dir):
    """Test writing and reading a TileQuet file."""
    import quadbin

    filepath = str(tmp_dir / "test.parquet")

    tiles = [
        {"tile": quadbin.tile_to_cell((0, 0, 0)), "data": b"tile0"},
        {"tile": quadbin.tile_to_cell((0, 0, 1)), "data": b"tile1"},
        {"tile": quadbin.tile_to_cell((1, 0, 1)), "data": b"tile2"},
    ]

    metadata = create_metadata(
        tile_type="raster",
        tile_format="png",
        bounds=[-180, -85, 180, 85],
        center=[0, 0, 1],
        min_zoom=0,
        max_zoom=1,
        num_tiles=3,
        source_format="test",
    )

    write_tilequet(filepath, tiles, metadata)

    read_meta = read_metadata(filepath)
    assert read_meta["file_format"] == "tilequet"
    assert read_meta["num_tiles"] == 3
    assert read_meta["tile_type"] == "raster"


def test_write_sorts_tiles(tmp_dir):
    """Test that write_tilequet sorts tiles by tile ID."""
    import pyarrow.parquet as pq
    import quadbin

    filepath = str(tmp_dir / "sorted.parquet")

    # Insert tiles in reverse order
    tiles = [
        {"tile": quadbin.tile_to_cell((1, 1, 1)), "data": b"tile3"},
        {"tile": quadbin.tile_to_cell((0, 0, 0)), "data": b"tile1"},
        {"tile": quadbin.tile_to_cell((0, 0, 1)), "data": b"tile2"},
    ]

    metadata = create_metadata(
        tile_type="raster",
        tile_format="png",
        min_zoom=0,
        max_zoom=1,
        num_tiles=3,
    )

    write_tilequet(filepath, tiles, metadata)

    table = pq.read_table(filepath)
    tile_ids = table.column("tile").to_pylist()
    # tile=0 (metadata) should be first, then sorted tile IDs
    assert tile_ids[0] == 0
    assert tile_ids[1:] == sorted(tile_ids[1:])


def test_read_metadata(sample_tilequet_file):
    """Test reading metadata from a sample file."""
    metadata = read_metadata(str(sample_tilequet_file))
    assert metadata["file_format"] == "tilequet"
    assert metadata["tile_type"] == "raster"
    assert metadata["tile_format"] == "png"
    assert metadata["name"] == "Test Tile Set"


def test_read_metadata_nonexistent(tmp_dir):
    """Test reading metadata from a non-existent file raises an error."""
    import pytest
    with pytest.raises(Exception):
        read_metadata(str(tmp_dir / "nonexistent.parquet"))


# --- TileQuetWriter streaming tests ---


def test_tilequet_writer_basic(tmp_dir):
    """Test TileQuetWriter produces a valid file with correct metadata."""
    import quadbin
    from tilequet.metadata import TileQuetWriter

    filepath = str(tmp_dir / "streamed.parquet")
    writer = TileQuetWriter(filepath, row_group_size=2)

    tiles_data = [
        (quadbin.tile_to_cell((0, 0, 0)), b"tile0"),
        (quadbin.tile_to_cell((0, 0, 1)), b"tile1"),
        (quadbin.tile_to_cell((1, 0, 1)), b"tile2"),
    ]
    for tile_id, data in tiles_data:
        writer.add_tile(tile_id, data)

    assert writer.tile_count == 3

    metadata = create_metadata(
        tile_type="raster", tile_format="png",
        min_zoom=0, max_zoom=1, num_tiles=writer.tile_count,
        source_format="test",
    )
    writer.close(metadata)

    read_meta = read_metadata(filepath)
    assert read_meta["num_tiles"] == 3
    assert read_meta["file_format"] == "tilequet"


def test_tilequet_writer_flushes_on_memory(tmp_dir):
    """Test that TileQuetWriter flushes when memory limit is reached."""
    import pyarrow.parquet as pq
    import quadbin
    from tilequet.metadata import TileQuetWriter

    filepath = str(tmp_dir / "flushed.parquet")
    # max_memory_mb=0 forces flush on every add_tile call
    writer = TileQuetWriter(filepath, row_group_size=2, max_memory_mb=0)

    for i in range(10):
        cell = quadbin.tile_to_cell((i % 4, 0, 2))
        writer.add_tile(cell, b"x" * 100)

    metadata = create_metadata(
        tile_type="raster", tile_format="png",
        min_zoom=2, max_zoom=2, num_tiles=writer.tile_count,
    )
    writer.close(metadata)

    pf = pq.ParquetFile(filepath)
    assert pf.metadata.num_row_groups > 1
    assert pf.metadata.num_rows == 11  # 10 tiles + 1 metadata row


def test_tilequet_writer_validates(tmp_dir):
    """Test that validation passes on writer-produced files."""
    import quadbin
    from tilequet.metadata import TileQuetWriter
    from tilequet.validate import validate_tilequet

    filepath = str(tmp_dir / "validated.parquet")
    writer = TileQuetWriter(filepath, row_group_size=10)

    for z in range(3):
        for x in range(1 << z):
            for y in range(1 << z):
                cell = quadbin.tile_to_cell((x, y, z))
                writer.add_tile(cell, b"\x89PNG\r\n\x1a\n" + b"\x00" * 50)

    metadata = create_metadata(
        tile_type="raster", tile_format="png",
        bounds=[-180.0, -85.05, 180.0, 85.05],
        min_zoom=0, max_zoom=2,
        num_tiles=writer.tile_count,
        source_format="test",
    )
    writer.close(metadata)

    result = validate_tilequet(filepath)
    assert result.is_valid
