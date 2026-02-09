"""TileQuet file validation module.

Provides comprehensive validation of TileQuet Parquet files including:
- Schema validation (required columns)
- Metadata validation (version, structure)
- Tile data integrity checks

Usage:
    from tilequet.validate import validate_tilequet

    result = validate_tilequet("path/to/file.parquet")
    if result.is_valid:
        print("File is valid!")
    else:
        for error in result.errors:
            print(f"Error: {error}")
"""

import dataclasses
import json
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import quadbin


@dataclasses.dataclass
class ValidationResult:
    """Result of TileQuet file validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    metadata: dict | None
    stats: dict[str, Any]

    def __str__(self) -> str:
        status = "VALID" if self.is_valid else "INVALID"
        lines = [f"TileQuet Validation: {status}"]

        if self.errors:
            lines.append(f"\nErrors ({len(self.errors)}):")
            for error in self.errors:
                lines.append(f"  ✗ {error}")

        if self.warnings:
            lines.append(f"\nWarnings ({len(self.warnings)}):")
            for warning in self.warnings:
                lines.append(f"  ⚠ {warning}")

        if self.stats:
            lines.append("\nStatistics:")
            for key, value in self.stats.items():
                lines.append(f"  {key}: {value}")

        return "\n".join(lines)


def validate_schema(table: pa.Table) -> tuple[list[str], list[str]]:
    """Validate TileQuet table schema."""
    errors = []
    warnings = []

    column_names = table.column_names

    if "tile" not in column_names:
        errors.append("Missing required column: 'tile'")
    elif str(table.schema.field("tile").type) not in ("uint64", "int64"):
        errors.append(f"Column 'tile' should be uint64 or int64, got {table.schema.field('tile').type}")

    if "metadata" not in column_names:
        errors.append("Missing required column: 'metadata'")
    elif str(table.schema.field("metadata").type) not in ("string", "utf8", "large_string", "large_utf8"):
        errors.append(f"Column 'metadata' should be string, got {table.schema.field('metadata').type}")

    if "data" not in column_names:
        errors.append("Missing required column: 'data'")
    elif str(table.schema.field("data").type) not in ("binary", "large_binary"):
        errors.append(f"Column 'data' should be binary, got {table.schema.field('data').type}")

    return errors, warnings


def validate_metadata(table: pa.Table) -> tuple[list[str], list[str], dict | None]:
    """Validate TileQuet metadata in tile 0."""
    errors = []
    warnings = []
    metadata = None

    try:
        tile_zero = table.filter(pc.equal(table.column("tile"), 0))
        if len(tile_zero) == 0:
            errors.append("No metadata row found (tile=0)")
            return errors, warnings, None

        metadata_str = tile_zero.column("metadata")[0].as_py()
        if metadata_str is None:
            errors.append("Metadata column is NULL in tile=0 row")
            return errors, warnings, None

        metadata = json.loads(metadata_str)
    except json.JSONDecodeError as e:
        errors.append(f"Invalid JSON in metadata: {e}")
        return errors, warnings, None
    except Exception as e:
        errors.append(f"Error reading metadata: {e}")
        return errors, warnings, None

    # Validate file_format
    file_format = metadata.get("file_format")
    if file_format != "tilequet":
        errors.append(f"Expected file_format 'tilequet', got '{file_format}'")

    # Validate version
    version = metadata.get("version")
    if version is None:
        errors.append("Missing 'version' in metadata")
    elif version not in ("0.1.0",):
        warnings.append(f"Unknown version '{version}', expected 0.1.0")

    # Validate required fields
    required_fields = ["tile_type", "tile_format", "bounds", "bounds_crs", "min_zoom", "max_zoom", "tiling"]
    for field in required_fields:
        if field not in metadata:
            errors.append(f"Missing required field '{field}' in metadata")

    # Validate tile_type
    tile_type = metadata.get("tile_type")
    if tile_type and tile_type not in ("vector", "raster", "3d"):
        errors.append(f"Invalid tile_type '{tile_type}', expected 'vector', 'raster', or '3d'")

    # Validate tiling
    tiling = metadata.get("tiling", {})
    if isinstance(tiling, dict):
        if tiling.get("scheme") != "quadbin":
            errors.append(f"Tiling scheme must be 'quadbin', got '{tiling.get('scheme')}'")

    # Validate bounds
    bounds = metadata.get("bounds")
    if bounds and isinstance(bounds, list) and len(bounds) != 4:
        errors.append(f"Bounds must have 4 values [west, south, east, north], got {len(bounds)}")

    return errors, warnings, metadata


def validate_tiles(table: pa.Table, metadata: dict) -> tuple[list[str], list[str], dict]:
    """Validate tile data integrity."""
    errors = []
    warnings = []
    stats: dict[str, Any] = {}

    if metadata is None:
        return errors, warnings, stats

    min_zoom = metadata.get("min_zoom", 0)
    max_zoom = metadata.get("max_zoom", 0)

    # Count tiles per zoom level
    data_rows = table.filter(pc.greater(table.column("tile"), 0))
    zoom_counts: dict[int, int] = {}

    for i in range(len(data_rows)):
        tile_id = data_rows.column("tile")[i].as_py()
        try:
            x, y, z = quadbin.cell_to_tile(tile_id)
            zoom_counts[z] = zoom_counts.get(z, 0) + 1
        except Exception:
            continue

    stats["zoom_levels"] = {}
    for z in range(min_zoom, max_zoom + 1):
        count = zoom_counts.get(z, 0)
        stats["zoom_levels"][z] = {"total": count}
        if count == 0:
            warnings.append(f"Zoom {z}: No tiles found")

    # Sample data integrity check
    if len(data_rows) > 0:
        sample_indices = list(set(
            i for i in [0, len(data_rows) // 2, len(data_rows) - 1]
            if i < len(data_rows)
        ))
        for idx in sample_indices:
            data = data_rows.column("data")[idx].as_py()
            if data is not None and len(data) == 0:
                warnings.append(f"Empty data blob at row {idx}")

    return errors, warnings, stats


def validate_tilequet(filepath: str) -> ValidationResult:
    """Validate a TileQuet Parquet file.

    Args:
        filepath: Path to the TileQuet file.

    Returns:
        ValidationResult with validation status, errors, warnings, and stats.
    """
    all_errors: list[str] = []
    all_warnings: list[str] = []
    all_stats: dict[str, Any] = {}
    metadata = None

    try:
        table = pq.read_table(filepath)
        all_stats["row_count"] = len(table)
        all_stats["columns"] = table.column_names
    except Exception as e:
        return ValidationResult(
            is_valid=False,
            errors=[f"Failed to read Parquet file: {e}"],
            warnings=[],
            metadata=None,
            stats={},
        )

    # Schema validation
    errors, warnings = validate_schema(table)
    all_errors.extend(errors)
    all_warnings.extend(warnings)

    # Metadata validation
    errors, warnings, metadata = validate_metadata(table)
    all_errors.extend(errors)
    all_warnings.extend(warnings)

    if metadata:
        all_stats["version"] = metadata.get("version")
        all_stats["tile_type"] = metadata.get("tile_type")
        all_stats["tile_format"] = metadata.get("tile_format")
        all_stats["zoom_range"] = f"{metadata.get('min_zoom')}-{metadata.get('max_zoom')}"
        all_stats["num_tiles"] = metadata.get("num_tiles")

    # Tile data validation
    if metadata:
        errors, warnings, tile_stats = validate_tiles(table, metadata)
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        all_stats.update(tile_stats)

    is_valid = len(all_errors) == 0

    return ValidationResult(
        is_valid=is_valid,
        errors=all_errors,
        warnings=all_warnings,
        metadata=metadata,
        stats=all_stats,
    )
