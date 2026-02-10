# TileQuet

[![Unit Tests](https://github.com/jatorre/tilequet/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/jatorre/tilequet/actions/workflows/unit-tests.yml)

TileQuet is a specification for storing map tile sets (vector tiles, raster tiles, and 3D tiles) in [Apache Parquet](https://parquet.apache.org/) format with [QUADBIN](https://docs.carto.com/data-and-analysis/analytics-toolbox-for-bigquery/key-concepts/spatial-indexes#quadbin) spatial indexing.

**[Documentation](https://jatorre.github.io/tilequet)** | **[Specification](format-specs/tilequet.md)** | **[CLI Reference](https://jatorre.github.io/tilequet/cli)**

## Why?

Tile sets today are stored in formats like PMTiles, MBTiles, or directories of files. These work well for **serving** tiles, but they're opaque to the rest of the data stack:

- You can't query them with SQL
- You can't catalog them in a data warehouse
- You can't JOIN them with other datasets
- You can't govern them alongside the rest of your data

TileQuet stores each tile as a row in a Parquet file. This means:

- **SQL-queryable** — analyze tile sets with DuckDB, BigQuery, Snowflake, or any Parquet-compatible engine
- **Governable** — catalog, audit, and manage tile sets alongside your other data
- **Cloud-native** — QUADBIN spatial indexing enables efficient tile lookups via row group pruning
- **Interoperable** — standard Parquet, no proprietary extensions

## Installation

```bash
# Basic installation (MBTiles and GeoPackage work out of the box)
pip install tilequet-io

# With PMTiles support
pip install "tilequet-io[pmtiles]"

# With all converters
pip install "tilequet-io[all]"
```

## Supported Converters

| Source | Command | Extra Dependency |
|--------|---------|-----------------|
| PMTiles | `convert pmtiles` | `pmtiles` |
| MBTiles | `convert mbtiles` | none (sqlite3) |
| GeoPackage | `convert geopackage` | none (sqlite3) |
| URL Template (XYZ/TMS) | `convert url` | `httpx` |
| TileJSON | `convert tilejson` | `httpx` |
| WMS | `convert wms` | `httpx` |
| WMTS | `convert wmts` | `httpx` |
| OGC API - Tiles | `convert ogc-tiles` | `httpx` |
| OGC API - Maps | `convert ogc-maps` | `httpx` |
| ArcGIS MapServer | `convert mapserver` | `httpx` |
| OGC 3D Tiles | `convert 3dtiles` | `httpx` |
| Cloud Optimized GeoTIFF | `convert cog` | `rasterio`, `Pillow` |

## CLI Usage

```bash
# Convert from various sources
tilequet-io convert pmtiles input.pmtiles output.parquet
tilequet-io convert mbtiles input.mbtiles output.parquet
tilequet-io convert geopackage input.gpkg output.parquet
tilequet-io convert url "https://tile.osm.org/{z}/{x}/{y}.png" output.parquet --max-zoom 3
tilequet-io convert mapserver https://server/.../MapServer output.parquet
tilequet-io convert 3dtiles https://example.com/tileset.json output.parquet
tilequet-io convert wms "https://ows.example.com/wms" output.parquet -l layer_name --max-zoom 4
tilequet-io convert wmts "https://ows.example.com/wmts" output.parquet -l layer_name
tilequet-io convert tilejson "https://example.com/tiles.json" output.parquet
tilequet-io convert ogc-tiles "https://api.example.com" output.parquet -c collection_id
tilequet-io convert ogc-maps "https://api.example.com" output.parquet -c collection_id
tilequet-io convert cog aligned.tif output.parquet

# Inspect a TileQuet file
tilequet-io inspect output.parquet

# Validate
tilequet-io validate output.parquet
tilequet-io validate output.parquet --json

# Split by zoom level
tilequet-io split-zoom output.parquet ./by_zoom/
```

## Querying with DuckDB

```sql
-- Get metadata
SELECT metadata FROM read_parquet('tiles.parquet') WHERE tile = 0;

-- Count tiles per zoom level
SELECT
    quadbin_z(tile) AS zoom,
    COUNT(*) AS num_tiles,
    SUM(LENGTH(data)) AS total_bytes
FROM read_parquet('tiles.parquet')
WHERE tile != 0
GROUP BY quadbin_z(tile)
ORDER BY zoom;
```

## Specification

See [format-specs/tilequet.md](format-specs/tilequet.md) for the full specification.

## Part of the RaQuet Family

TileQuet follows the same design principles as [RaQuet](https://raquet.io):
- Row-zero metadata (JSON in `tile=0` / `block=0` row)
- QUADBIN spatial indexing
- Standard Parquet, no proprietary extensions

## License

See [LICENSE](LICENSE) for the license.
