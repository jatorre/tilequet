"""Tests for GeoPackage to TileQuet conversion."""

from tilequet.geopackage2tilequet import convert
from tilequet.metadata import read_metadata


def test_convert_geopackage(sample_geopackage_file, tmp_dir):
    """Test full GeoPackage conversion."""
    output = str(tmp_dir / "output.parquet")
    result = convert(str(sample_geopackage_file), output)

    assert result["tile_format"] == "png"
    assert result["tile_type"] == "raster"
    assert result["min_zoom"] == 0
    assert result["max_zoom"] == 2
    assert result["num_tiles"] > 0

    metadata = read_metadata(output)
    assert metadata["file_format"] == "tilequet"
    assert metadata["tile_type"] == "raster"
    assert metadata["name"] == "Test GPKG"


def test_convert_geopackage_with_table_name(sample_geopackage_file, tmp_dir):
    """Test GeoPackage conversion with explicit table name."""
    output = str(tmp_dir / "output.parquet")
    result = convert(str(sample_geopackage_file), output, table_name="test_tiles")

    assert result["num_tiles"] > 0
