"""Tests for MBTiles to TileQuet conversion."""

from tilequet.mbtiles2tilequet import (
    convert,
    detect_tile_format,
    tile_type_from_format,
    tms_to_xyz_y,
)
from tilequet.metadata import read_metadata


def test_detect_tile_format_png():
    assert detect_tile_format(b"\x89PNG\r\n\x1a\n" + b"\x00" * 10) == "png"


def test_detect_tile_format_jpeg():
    assert detect_tile_format(b"\xff\xd8\xff" + b"\x00" * 10) == "jpeg"


def test_detect_tile_format_webp():
    assert detect_tile_format(b"RIFF" + b"\x00" * 10) == "webp"


def test_detect_tile_format_gzip_pbf():
    assert detect_tile_format(b"\x1f\x8b" + b"\x00" * 10) == "pbf"


def test_detect_tile_format_unknown():
    assert detect_tile_format(b"\x00\x00\x00\x00") == "unknown"


def test_tile_type_from_format():
    assert tile_type_from_format("pbf") == "vector"
    assert tile_type_from_format("png") == "raster"
    assert tile_type_from_format("jpeg") == "raster"
    assert tile_type_from_format("webp") == "raster"
    assert tile_type_from_format("unknown") == "raster"


def test_tms_to_xyz_y():
    assert tms_to_xyz_y(0, 0) == 0
    assert tms_to_xyz_y(1, 0) == 1
    assert tms_to_xyz_y(1, 1) == 0
    assert tms_to_xyz_y(2, 0) == 3
    assert tms_to_xyz_y(2, 3) == 0


def test_convert_mbtiles(sample_mbtiles_file, tmp_dir):
    """Test full MBTiles conversion."""
    output = str(tmp_dir / "output.parquet")
    result = convert(str(sample_mbtiles_file), output)

    assert result["num_tiles"] == 21  # z0=1 + z1=4 + z2=16
    assert result["tile_format"] == "png"
    assert result["tile_type"] == "raster"
    assert result["min_zoom"] == 0
    assert result["max_zoom"] == 2

    # Verify metadata
    metadata = read_metadata(output)
    assert metadata["file_format"] == "tilequet"
    assert metadata["tile_type"] == "raster"
    assert metadata["name"] == "Test MBTiles"
