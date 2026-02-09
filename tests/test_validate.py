"""Tests for TileQuet validation module."""


import pyarrow as pa
import pyarrow.parquet as pq

from tilequet.validate import ValidationResult, validate_tilequet


def test_validate_valid_file(sample_tilequet_file):
    """Test validation of a valid TileQuet file."""
    result = validate_tilequet(str(sample_tilequet_file))
    assert result.is_valid
    assert result.errors == []
    assert result.metadata is not None
    assert result.metadata["file_format"] == "tilequet"
    assert "row_count" in result.stats


def test_validate_vector_file(sample_vector_tilequet_file):
    """Test validation of a valid vector TileQuet file."""
    result = validate_tilequet(str(sample_vector_tilequet_file))
    assert result.is_valid
    assert result.metadata["tile_type"] == "vector"
    assert result.metadata["tile_format"] == "pbf"


def test_validate_invalid_schema(invalid_parquet_file):
    """Test validation catches wrong schema."""
    result = validate_tilequet(str(invalid_parquet_file))
    assert not result.is_valid
    assert any("tile" in e for e in result.errors)


def test_validate_nonexistent_file():
    """Test validation of a non-existent file."""
    result = validate_tilequet("/tmp/nonexistent_file.parquet")
    assert not result.is_valid
    assert len(result.errors) > 0


def test_validate_no_metadata_row(tmp_dir):
    """Test validation catches missing metadata row."""
    filepath = str(tmp_dir / "no_meta.parquet")

    table = pa.table({
        "tile": pa.array([1, 2, 3], type=pa.uint64()),
        "metadata": pa.array([None, None, None], type=pa.string()),
        "data": pa.array([b"a", b"b", b"c"], type=pa.binary()),
    })
    pq.write_table(table, filepath)

    result = validate_tilequet(filepath)
    assert not result.is_valid
    assert any("metadata" in e.lower() for e in result.errors)


def test_validate_invalid_metadata_json(tmp_dir):
    """Test validation catches invalid JSON in metadata."""
    filepath = str(tmp_dir / "bad_json.parquet")

    table = pa.table({
        "tile": pa.array([0, 1], type=pa.uint64()),
        "metadata": pa.array(["not valid json{{{", None], type=pa.string()),
        "data": pa.array([None, b"tile"], type=pa.binary()),
    })
    pq.write_table(table, filepath)

    result = validate_tilequet(filepath)
    assert not result.is_valid
    assert any("json" in e.lower() for e in result.errors)


def test_validation_result_str():
    """Test ValidationResult string representation."""
    result = ValidationResult(
        is_valid=True,
        errors=[],
        warnings=["Some warning"],
        metadata={"file_format": "tilequet"},
        stats={"row_count": 10},
    )
    text = str(result)
    assert "VALID" in text
    assert "Some warning" in text

    invalid = ValidationResult(
        is_valid=False,
        errors=["Missing column"],
        warnings=[],
        metadata=None,
        stats={},
    )
    text = str(invalid)
    assert "INVALID" in text
    assert "Missing column" in text


def test_validate_stats(sample_tilequet_file):
    """Test validation produces useful stats."""
    result = validate_tilequet(str(sample_tilequet_file))
    assert result.is_valid
    assert result.stats["tile_type"] == "raster"
    assert result.stats["tile_format"] == "png"
    assert "zoom_levels" in result.stats
