---
layout: default
title: CLI Reference
page_header: true
page_description: "Command reference for the tilequet-io CLI tool"
---

## Installation

```bash
# Basic installation (MBTiles and GeoPackage work out of the box)
pip install tilequet-io

# With PMTiles support
pip install "tilequet-io[pmtiles]"

# With network converters (URL template, MapServer, 3D Tiles)
pip install "tilequet-io[url]"

# All features
pip install "tilequet-io[all]"
```

Or with uv:

```bash
uv add "tilequet-io[all]"
```

---

## Commands Overview

| Command | Description |
|---------|-------------|
| `convert pmtiles` | Convert PMTiles v3 to TileQuet |
| `convert mbtiles` | Convert MBTiles (SQLite) to TileQuet |
| `convert geopackage` | Convert GeoPackage tiles to TileQuet |
| `convert url` | Convert XYZ/TMS URL template to TileQuet |
| `convert mapserver` | Convert ArcGIS MapServer to TileQuet |
| `convert 3dtiles` | Convert OGC 3D Tiles to TileQuet |
| `inspect` | Display metadata and statistics |
| `validate` | Validate file structure and data integrity |
| `split-zoom` | Split by zoom level for optimized remote access |

---

## convert pmtiles

Convert a PMTiles v3 file to TileQuet format.

```bash
tilequet-io convert pmtiles INPUT OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Example

```bash
tilequet-io convert pmtiles firenze.pmtiles firenze.parquet -v
```

**Requirements:** `pip install "tilequet-io[pmtiles]"`

---

## convert mbtiles

Convert an MBTiles file to TileQuet format. Handles TMS-to-XYZ Y-axis conversion automatically.

```bash
tilequet-io convert mbtiles INPUT OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Example

```bash
tilequet-io convert mbtiles basemap.mbtiles basemap.parquet -v
```

No extra dependencies — uses Python's built-in `sqlite3`.

---

## convert geopackage

Convert tiles from a GeoPackage file to TileQuet format.

```bash
tilequet-io convert geopackage INPUT OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--table` | auto | Tile table name (auto-detects first tile table) |
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Example

```bash
tilequet-io convert geopackage terrain.gpkg terrain.parquet
tilequet-io convert geopackage multi.gpkg output.parquet --table satellite_tiles
```

No extra dependencies — uses Python's built-in `sqlite3`.

---

## convert url

Convert tiles from an XYZ or TMS URL template to TileQuet format.

```bash
tilequet-io convert url TEMPLATE OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--min-zoom` | `0` | Minimum zoom level |
| `--max-zoom` | `5` | Maximum zoom level |
| `--bbox` | world | Bounding box: `west,south,east,north` (WGS84) |
| `--tms` | — | Use TMS Y convention (flipped Y axis) |
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Examples

```bash
# OpenStreetMap tiles, zoom 0-3
tilequet-io convert url "https://tile.openstreetmap.org/{z}/{x}/{y}.png" osm.parquet \
  --max-zoom 3

# With bounding box filter
tilequet-io convert url "https://tiles.example.com/{z}/{x}/{y}.pbf" city.parquet \
  --bbox "-3.8,40.3,-3.6,40.5" --min-zoom 10 --max-zoom 14

# TMS convention
tilequet-io convert url "https://tms.example.com/{z}/{x}/{y}.png" output.parquet --tms
```

**Requirements:** `pip install "tilequet-io[url]"`

---

## convert mapserver

Convert pre-rendered tiles from an ArcGIS MapServer REST endpoint to TileQuet format.

```bash
tilequet-io convert mapserver URL OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--token` | — | ArcGIS authentication token |
| `--bbox` | service extent | Bounding box: `west,south,east,north` (WGS84) |
| `--min-zoom` | service min | Minimum zoom level |
| `--max-zoom` | service max (capped at 14) | Maximum zoom level |
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Examples

```bash
# Basic conversion
tilequet-io convert mapserver https://server/arcgis/rest/services/Basemap/MapServer output.parquet

# With bounding box and zoom range
tilequet-io convert mapserver https://server/.../MapServer output.parquet \
  --bbox "-122.5,37.5,-122.0,38.0" --min-zoom 8 --max-zoom 12

# With authentication
tilequet-io convert mapserver https://server/.../MapServer output.parquet --token YOUR_TOKEN
```

**Requirements:** `pip install "tilequet-io[mapserver]"`

---

## convert 3dtiles

Convert an OGC 3D Tiles tileset to TileQuet format. Fetches tile content (glTF, GLB, b3dm, pnts) from a tileset.json URL.

```bash
tilequet-io convert 3dtiles URL OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--max-tiles` | unlimited | Maximum number of tiles to fetch |
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Examples

```bash
# Convert a 3D Tiles tileset
tilequet-io convert 3dtiles https://example.com/tileset.json buildings.parquet -v

# Limit tile count for testing
tilequet-io convert 3dtiles https://example.com/tileset.json test.parquet --max-tiles 100
```

**Requirements:** `pip install "tilequet-io[tiles3d]"`

---

## inspect

Display metadata and statistics for a TileQuet file. Uses rich formatting when available.

```bash
tilequet-io inspect FILE [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Show full JSON metadata |

### Examples

```bash
tilequet-io inspect firenze.parquet
tilequet-io inspect firenze.parquet -v
```

### Sample Output

```
TileQuet File: firenze.parquet
Path: /data/firenze.parquet

  General Information
┌─────────────┬────────┐
│ File Size   │ 6.3 MB │
│ Total Rows  │ 93     │
│ Row Groups  │ 1      │
│ Tile Type   │ vector │
│ Tile Format │ pbf    │
│ Zoom Range  │ 0-15   │
│ Num Tiles   │ 92     │
└─────────────┴────────┘
   Bounds (WGS84)
┌───────┬───────────┐
│ West  │ 11.221144 │
│ South │ 43.745121 │
│ East  │ 11.287543 │
│ North │ 43.789306 │
└───────┴───────────┘
```

---

## validate

Validate a TileQuet file for correctness and data integrity.

```bash
tilequet-io validate FILE [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--json` | Output results as JSON |

### Validation Checks

- Valid Parquet file
- Required columns present (`tile`, `metadata`, `data`)
- Correct column types (uint64, string, binary)
- Metadata row exists at `tile=0`
- Valid JSON in metadata
- Required metadata fields (`file_format`, `version`, `tile_type`, `tile_format`, `bounds`, zoom range)
- Valid tiling scheme (`quadbin`)
- Tile count per zoom level

### Examples

```bash
# Basic validation
tilequet-io validate output.parquet

# JSON output for automation
tilequet-io validate output.parquet --json
```

### Exit Codes

| Code | Description |
|------|-------------|
| `0` | Valid file |
| `1` | Invalid file or errors found |

---

## split-zoom

Split a TileQuet file by zoom level for optimized remote access.

```bash
tilequet-io split-zoom INPUT OUTPUT_DIR [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Example

```bash
tilequet-io split-zoom tiles.parquet ./by_zoom/
```

### Output Structure

```
by_zoom/
├── zoom_0.parquet
├── zoom_1.parquet
├── zoom_2.parquet
└── ...
```

Each file contains only the tiles for that zoom level, plus the full metadata in the `tile=0` row.

---

## Global Options

| Option | Description |
|--------|-------------|
| `--version` | Show version and exit |
| `--help` | Show help and exit |

---

## Exit Codes

| Code | Description |
|------|-------------|
| `0` | Success |
| `1` | General error |
| `2` | Invalid arguments |
