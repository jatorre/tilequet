"""Tests for the TileQuet CLI."""

from click.testing import CliRunner

from tilequet.cli import cli


def test_version():
    """Test --version flag."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.output


def test_help():
    """Test --help flag."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "TileQuet" in result.output
    assert "inspect" in result.output
    assert "convert" in result.output
    assert "validate" in result.output


def test_convert_help():
    """Test convert group help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["convert", "--help"])
    assert result.exit_code == 0
    assert "pmtiles" in result.output
    assert "mbtiles" in result.output
    assert "geopackage" in result.output
    assert "url" in result.output
    assert "mapserver" in result.output
    assert "3dtiles" in result.output


def test_inspect(sample_tilequet_file):
    """Test inspect command with a valid file."""
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", str(sample_tilequet_file)])
    assert result.exit_code == 0
    assert "raster" in result.output
    assert "png" in result.output


def test_inspect_verbose(sample_tilequet_file):
    """Test inspect command with verbose flag."""
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", str(sample_tilequet_file), "-v"])
    assert result.exit_code == 0
    assert "Full metadata:" in result.output
    assert '"file_format"' in result.output


def test_inspect_nonexistent():
    """Test inspect with non-existent file."""
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", "nonexistent.parquet"])
    assert result.exit_code != 0


def test_validate_valid(sample_tilequet_file):
    """Test validate command with a valid file."""
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(sample_tilequet_file)])
    assert result.exit_code == 0
    assert "Valid" in result.output or "VALID" in result.output


def test_validate_json(sample_tilequet_file):
    """Test validate command with --json flag."""
    import json

    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(sample_tilequet_file), "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["is_valid"] is True
    assert data["errors"] == []


def test_validate_invalid(invalid_parquet_file):
    """Test validate command with an invalid file."""
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(invalid_parquet_file)])
    assert result.exit_code == 1


def test_validate_invalid_json(invalid_parquet_file):
    """Test validate --json with an invalid file."""
    import json

    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(invalid_parquet_file), "--json"])
    assert result.exit_code == 1
    data = json.loads(result.output)
    assert data["is_valid"] is False
    assert len(data["errors"]) > 0


def test_convert_mbtiles(sample_mbtiles_file, tmp_dir):
    """Test MBTiles conversion."""
    output = tmp_dir / "output.parquet"
    runner = CliRunner()
    result = runner.invoke(
        cli, ["convert", "mbtiles", str(sample_mbtiles_file), str(output)]
    )
    assert result.exit_code == 0
    assert "Done" in result.output
    assert output.exists()

    # Validate the output
    result2 = runner.invoke(cli, ["validate", str(output)])
    assert result2.exit_code == 0


def test_convert_geopackage(sample_geopackage_file, tmp_dir):
    """Test GeoPackage conversion."""
    output = tmp_dir / "output.parquet"
    runner = CliRunner()
    result = runner.invoke(
        cli, ["convert", "geopackage", str(sample_geopackage_file), str(output)]
    )
    assert result.exit_code == 0
    assert "Done" in result.output
    assert output.exists()

    # Validate the output
    result2 = runner.invoke(cli, ["validate", str(output)])
    assert result2.exit_code == 0


def test_split_zoom(sample_tilequet_file, tmp_dir):
    """Test split-zoom command."""
    output_dir = tmp_dir / "split_output"
    runner = CliRunner()
    result = runner.invoke(
        cli, ["split-zoom", str(sample_tilequet_file), str(output_dir)]
    )
    assert result.exit_code == 0
    assert "Split complete" in result.output
    assert output_dir.exists()
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0
