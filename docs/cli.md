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

# With network converters (URL, WMS, WMTS, MapServer, TileJSON, OGC APIs, 3D Tiles)
pip install "tilequet-io[url]"

# With COG support
pip install "tilequet-io[cog]"

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
| `convert tilejson` | Convert TileJSON endpoint to TileQuet |
| `convert wms` | Convert WMS (Web Map Service) to TileQuet |
| `convert wmts` | Convert WMTS (Web Map Tile Service) to TileQuet |
| `convert ogc-tiles` | Convert OGC API - Tiles endpoint to TileQuet |
| `convert ogc-maps` | Convert OGC API - Maps endpoint to TileQuet |
| `convert mapserver` | Convert ArcGIS MapServer to TileQuet |
| `convert 3dtiles` | Convert OGC 3D Tiles to TileQuet |
| `convert cog` | Convert tile-aligned COG to TileQuet |
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

## convert wms

Convert tiles from a WMS (Web Map Service) endpoint to TileQuet format. Computes Web Mercator bounding boxes per XYZ tile and issues WMS GetMap requests.

```bash
tilequet-io convert wms SERVICE_URL OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--layers, -l` | (required) | Comma-separated WMS layer names |
| `--min-zoom` | `0` | Minimum zoom level |
| `--max-zoom` | `5` | Maximum zoom level |
| `--bbox` | world | Bounding box: `west,south,east,north` (WGS84) |
| `--tile-size` | `256` | Tile width/height in pixels |
| `--format` | `image/png` | WMS image format |
| `--wms-version` | `1.3.0` | WMS version (1.1.1 or 1.3.0) |
| `--styles` | `""` | WMS styles parameter |
| `--crs` | `EPSG:3857` | Coordinate reference system |
| `--transparent/--no-transparent` | `true` | Request transparent background |
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Examples

```bash
# Canadian weather temperature layer
tilequet-io convert wms "https://geo.weather.gc.ca/geomet" weather.parquet \
  -l GDPS.ETA_TT --max-zoom 4

# With bounding box and custom format
tilequet-io convert wms "https://ows.example.com/wms" output.parquet \
  -l layer1,layer2 --bbox "-10,35,5,45" --max-zoom 8 --format image/jpeg
```

**Requirements:** `pip install "tilequet-io[wms]"`

---

## convert wmts

Convert tiles from a WMTS (Web Map Tile Service) endpoint to TileQuet format. Uses KVP GetTile requests.

```bash
tilequet-io convert wmts SERVICE_URL OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--layer, -l` | (required) | WMTS layer name |
| `--tile-matrix-set` | `GoogleMapsCompatible` | Tile matrix set identifier |
| `--min-zoom` | `0` | Minimum zoom level |
| `--max-zoom` | `5` | Maximum zoom level |
| `--bbox` | world | Bounding box: `west,south,east,north` (WGS84) |
| `--format` | `image/png` | Image format |
| `--style` | `default` | WMTS style name |
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Examples

```bash
tilequet-io convert wmts "https://wmts.example.com/service" output.parquet \
  -l satellite --max-zoom 6

tilequet-io convert wmts "https://wmts.example.com/service" output.parquet \
  -l terrain --tile-matrix-set WebMercatorQuad --bbox "-10,35,5,45"
```

**Requirements:** `pip install "tilequet-io[wmts]"`

---

## convert tilejson

Convert tiles from a TileJSON endpoint to TileQuet format. Fetches the TileJSON metadata, extracts tile URL templates, then downloads tiles.

```bash
tilequet-io convert tilejson TILEJSON_URL OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--min-zoom` | from TileJSON | Override minimum zoom level |
| `--max-zoom` | from TileJSON | Override maximum zoom level |
| `--bbox` | from TileJSON | Bounding box: `west,south,east,north` (WGS84) |
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Examples

```bash
# Uses bounds and zoom from the TileJSON metadata
tilequet-io convert tilejson "https://example.com/tiles.json" output.parquet

# Override zoom range
tilequet-io convert tilejson "https://example.com/tiles.json" output.parquet \
  --min-zoom 2 --max-zoom 8
```

**Requirements:** `pip install "tilequet-io[tilejson]"`

---

## convert ogc-tiles

Convert tiles from an OGC API - Tiles endpoint to TileQuet format. The modern REST successor to WMTS.

```bash
tilequet-io convert ogc-tiles BASE_URL OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--collection, -c` | (required) | Collection identifier |
| `--tile-matrix-set` | `WebMercatorQuad` | Tile matrix set identifier |
| `--min-zoom` | `0` | Minimum zoom level |
| `--max-zoom` | `5` | Maximum zoom level |
| `--bbox` | world | Bounding box: `west,south,east,north` (WGS84) |
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Examples

```bash
tilequet-io convert ogc-tiles "https://api.example.com" output.parquet \
  -c buildings --max-zoom 10

tilequet-io convert ogc-tiles "https://api.example.com" output.parquet \
  -c elevation --bbox "-122.5,37.5,-122.0,38.0"
```

**Requirements:** `pip install "tilequet-io[ogc]"`

---

## convert ogc-maps

Convert map images from an OGC API - Maps endpoint to TileQuet format. The modern REST successor to WMS. Computes WGS84 bounding boxes per tile and requests rendered images.

```bash
tilequet-io convert ogc-maps BASE_URL OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--collection, -c` | (required) | Collection identifier |
| `--min-zoom` | `0` | Minimum zoom level |
| `--max-zoom` | `5` | Maximum zoom level |
| `--bbox` | world | Bounding box: `west,south,east,north` (WGS84) |
| `--tile-size` | `256` | Tile width/height in pixels |
| `--format` | `image/png` | Image format |
| `--transparent/--no-transparent` | `true` | Request transparent background |
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Examples

```bash
tilequet-io convert ogc-maps "https://api.example.com" output.parquet \
  -c temperature --max-zoom 6

tilequet-io convert ogc-maps "https://api.example.com" output.parquet \
  -c landuse --bbox "-10,35,5,45" --format image/jpeg
```

**Requirements:** `pip install "tilequet-io[ogc]"`

---

## convert cog

Convert a tile-aligned Cloud Optimized GeoTIFF (COG) to TileQuet format. Only imports COGs that are perfectly aligned with the Web Mercator tile grid (EPSG:3857, 256x256 or 512x512 blocks, aligned origin, power-of-2 overviews). For non-aligned rasters, use [RaQuet](https://github.com/jatorre/raquet).

```bash
tilequet-io convert cog INPUT OUTPUT [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--min-zoom` | lowest available | Minimum zoom level |
| `--max-zoom` | native resolution | Maximum zoom level |
| `--format` | `png` | Image encoding (`png` or `jpeg`) |
| `--row-group-size` | `200` | Parquet row group size |
| `-v, --verbose` | — | Enable verbose output |

### Examples

```bash
# Convert a tile-aligned COG
tilequet-io convert cog aligned_raster.tif output.parquet -v

# Convert with JPEG encoding for smaller files
tilequet-io convert cog satellite.tif output.parquet --format jpeg
```

If the COG is not tile-aligned, the converter will display the specific alignment issues and suggest using RaQuet instead.

**Requirements:** `pip install "tilequet-io[cog]"`

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
