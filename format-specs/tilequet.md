# TileQuet Specification v0.1.0

## Overview

TileQuet is a specification for storing and querying map tile sets using [Apache Parquet](https://parquet.apache.org/), a column-oriented data file format.

TileQuet supports vector tiles (Mapbox Vector Tiles / PBF), raster tiles (PNG, JPEG, WebP), and 3D tiles (glTF/GLB, b3dm, pnts) — all stored as binary blobs in a standard Parquet file with spatial indexing metadata.

## Motivation

Tile sets are the fundamental unit of web mapping. They are served from formats like PMTiles, MBTiles, or directories of files. These formats are optimized for **serving** — delivering individual tiles to map renderers efficiently.

However, these formats are **opaque to the rest of the data stack**:

- You can't query a PMTiles file with SQL
- You can't catalog MBTiles files in a data warehouse
- You can't JOIN tile metadata with other datasets
- You can't govern tile sets alongside the rest of your data
- You can't run analytics across tile sets (coverage, size distribution, quality checks)

TileQuet addresses this by storing tile sets in Apache Parquet — a format supported by virtually every modern analytics engine. The primary use case is **governance and analytics**, not replacing purpose-built serving formats.

## Data Organization

The format organizes tile data as follows:

1. Each tile in the tile set becomes a **row** in the Parquet file.
2. Tiles are identified by a **spatial index** encoded as a 64-bit integer in the `tile` column. In v0.1.0, the spatial index is [QUADBIN](#tiling-scheme), which encodes Web Mercator tile coordinates (z, x, y) into a single value.
3. Tile content (PBF, PNG, JPEG, WebP, GLB, etc.) is stored as a **binary blob** in the `data` column. The tile content is stored as-is — TileQuet does not decode, re-encode, or transform tile content.
4. Rich metadata is stored as a JSON string in a special **row-zero** entry (`tile = 0`).
5. Rows are sorted by tile ID (QUADBIN) for efficient spatial queries via Parquet row group pruning.

## File Structure

A TileQuet file must contain:

### Primary Table

Required columns:

| Column | Type | Description |
|--------|------|-------------|
| `tile` | uint64 | Spatial index identifier (QUADBIN cell ID in v0.1.0) |
| `metadata` | string | JSON metadata. Only populated where `tile = 0`. NULL in all other rows. |
| `data` | binary | Raw tile content (PBF, PNG, JPEG, WebP, GLB, etc.) |

## Column Specifications

### tile Column

- Type: uint64
- Description: Spatial index identifier that encodes the tile's location and zoom level.
- In v0.1.0, this is a QUADBIN cell ID that encodes Web Mercator tile coordinates (z, x, y).
- Special value: `tile = 0` is reserved for the metadata row. This is valid because QUADBIN cell ID `0` is not a valid tile identifier.

### metadata Column

- Type: string (UTF-8)
- Content: JSON string containing tile set metadata.
- Special handling: Only populated in the row where `tile = 0`. All other rows MUST have NULL in this column.
- Format: See [Metadata Specification](#metadata-specification) for the JSON structure.

### data Column

- Type: binary (bytes)
- Content: Raw tile content, stored exactly as it would be served to a client.
- The binary format depends on the tile type:
  - **Vector tiles**: Protocol Buffer encoded Mapbox Vector Tiles (PBF/MVT)
  - **Raster tiles**: PNG, JPEG, or WebP image data
  - **3D tiles**: glTF/GLB, Batched 3D Model (b3dm), or Point Cloud (pnts) data
- TileQuet does NOT decode or re-encode tile content. The blob is stored and retrieved as-is.
- The `data` column MAY be NULL for the metadata row (`tile = 0`).

## Tiling Scheme

### v0.1.0: QUADBIN

TileQuet v0.1.0 uses the **QUADBIN** tiling scheme for spatial indexing. QUADBIN is a hierarchical geospatial index that encodes Web Mercator tile coordinates (z, x, y) into a single 64-bit integer.

Key properties:
- **Single-column index**: Location and zoom level in one UINT64 value
- **Morton order**: Spatially adjacent tiles have numerically similar IDs, enabling efficient Parquet row group pruning
- **Resolution range**: Zoom levels 0–26

#### Reference Implementations

- Python: [quadbin-py](https://github.com/CartoDB/quadbin-py) — `quadbin.tile_to_cell(z, x, y)`, `quadbin.cell_to_tile(cell)`
- JavaScript: [@carto/quadbin](https://github.com/CartoDB/quadbin-js)
- SQL: [CARTO Analytics Toolbox](https://docs.carto.com/data-and-analysis/analytics-toolbox-for-bigquery/sql-reference/quadbin)

### Future Tiling Schemes

The tiling scheme is identified by the `tiling.scheme` field in metadata. Future versions may support additional schemes:

- **`octbin`** — For octree-based 3D tile sets (3D Morton code encoding level, x, y, z)
- **`node_id`** — For explicit bounding volume hierarchy (BVH) 3D tile trees

Readers MUST check `tiling.scheme` and reject files with unrecognized schemes rather than assuming QUADBIN.

### Row Ordering Recommendation

For optimal random-access performance when reading from cloud storage (S3, GCS, Azure Blob), producers SHOULD sort rows by tile ID. For QUADBIN, this clusters spatially adjacent tiles together, enabling Parquet row group pruning when filtering by spatial location.

### Row Group Size Considerations

The optimal Parquet row group size depends on the primary access pattern:

- **Smaller row groups** (e.g., 50–200 rows): Better for serving individual tiles, as clients can fetch tiles with minimal data transfer overhead.
- **Larger row groups** (e.g., 1000+ rows): Better for analytics workloads that scan many tiles.

The reference implementation uses 200 rows as the default.

### Compression

TileQuet files default to **no Parquet-level compression**. This is because the `data` column — which accounts for virtually all of the file size — contains tile content (PNG, JPEG, WebP, PBF, GLB) that is already compressed by its own format. Applying Parquet compression (zstd, snappy, gzip) on top of pre-compressed data wastes CPU on both read and write with negligible size reduction. The `tile` and `metadata` columns are too small for compression to matter.

Producers MAY enable Parquet compression for tile sets containing uncompressed data (e.g., raw binary formats), but the default SHOULD be no compression.

## Metadata Specification

The metadata is stored as a JSON string in the `metadata` column where `tile = 0`. The JSON object has the following structure:

```json
{
    "file_format": "tilequet",
    "version": "0.1.0",
    "tile_type": "vector",
    "tile_format": "pbf",
    "bounds": [-11.25, 42.55, 11.95, 44.01],
    "bounds_crs": "EPSG:4326",
    "center": [11.25, 43.77, 14],
    "min_zoom": 0,
    "max_zoom": 14,
    "num_tiles": 12345,
    "tiling": {
        "scheme": "quadbin"
    },
    "name": "OpenStreetMap Firenze",
    "description": "Vector tiles for the Firenze area",
    "attribution": "OpenStreetMap contributors",
    "layers": [
        {
            "id": "water",
            "description": "Water polygons",
            "minzoom": 0,
            "maxzoom": 14,
            "fields": {
                "class": "string",
                "intermittent": "number"
            }
        }
    ],
    "tilejson": {
        "tilejson": "3.0.0",
        "tiles": [],
        "bounds": [-11.25, 42.55, 11.95, 44.01],
        "minzoom": 0,
        "maxzoom": 14,
        "name": "OpenStreetMap Firenze",
        "vector_layers": [{"id": "water", "fields": {"class": "string"}}]
    },
    "processing": {
        "source_format": "pmtiles",
        "created_by": "tilequet-io 0.1.0",
        "created_at": "2026-02-09T10:30:00Z"
    }
}
```

### Metadata Fields Description

- **Format Identification**
  - `file_format`: String identifying this as a TileQuet file. MUST be `"tilequet"`.
  - `version`: String indicating the TileQuet specification version. Current version is `"0.1.0"`.

- **Tile Type and Format**
  - `tile_type`: String indicating the type of tiles. Valid values:
    - `"vector"` — Mapbox Vector Tiles (PBF/MVT)
    - `"raster"` — Raster image tiles (PNG, JPEG, WebP)
    - `"3d"` — 3D tile content (glTF/GLB, b3dm, pnts)
  - `tile_format`: String indicating the binary format of tile data. Valid values:
    - Vector: `"pbf"` (Protocol Buffers / Mapbox Vector Tiles)
    - Raster: `"png"`, `"jpeg"`, `"webp"`
    - 3D: `"glb"`, `"b3dm"`, `"pnts"`

- **Geographic Extent**
  - `bounds`: Array [west, south, east, north] specifying the geographic extent in WGS84.
  - `bounds_crs`: String indicating the CRS of the bounds. Always `"EPSG:4326"` (WGS84).
  - `center`: Array [longitude, latitude, zoom] specifying the default center point and zoom level for display.

- **Zoom Range**
  - `min_zoom`: Integer indicating the minimum zoom level available.
  - `max_zoom`: Integer indicating the maximum zoom level available.
  - `num_tiles`: Integer count of tiles in the file (excluding the metadata row).

- **Tiling Information**
  - `tiling`: Object containing tiling scheme configuration:
    - `scheme`: String identifying the tiling scheme. MUST be `"quadbin"` in v0.1.0.

- **Descriptive Metadata**
  - `name`: Optional string with the tile set name.
  - `description`: Optional string with a human-readable description.
  - `attribution`: Optional string with data attribution.

- **Layer Information** (vector tiles only)
  - `layers`: Optional array of layer objects describing the vector tile layers. Each layer object:
    - `id`: String layer identifier (matches the layer name in PBF tiles).
    - `description`: Optional string description of the layer.
    - `minzoom`: Optional integer minimum zoom level for this layer.
    - `maxzoom`: Optional integer maximum zoom level for this layer.
    - `fields`: Optional object mapping field names to their types (`"string"`, `"number"`, `"boolean"`).

  This follows the [TileJSON](https://github.com/mapbox/tilejson-spec) `vector_layers` convention.

- **TileJSON** (recommended)
  - `tilejson`: A [TileJSON 3.0.0](https://github.com/mapbox/tilejson-spec/tree/master/3.0.0) compliant object. This enables downstream tools to reconstruct a standards-compliant TileJSON endpoint without custom parsing. The `tiles` array is stored empty (`[]`) — consumers fill it in with their serving URL.

- **Processing Information** (optional)
  - `processing`: Object documenting the conversion source:
    - `source_format`: String indicating the source format. Known values: `"pmtiles"`, `"mbtiles"`, `"geopackage"`, `"url_template"`, `"tilejson"`, `"wms"`, `"wmts"`, `"ogc_api_tiles"`, `"ogc_api_maps"`, `"arcgis_mapserver"`, `"3dtiles"`, `"cog"`.
    - `created_by`: String identifying the tool and version that created the file.
    - `created_at`: ISO 8601 timestamp of file creation.

- **Source-Specific Metadata** (optional)

  Converters MAY include additional top-level keys to preserve source-specific information. Known sections:

  - `tileset_json`: For 3D Tiles sources — the original tileset.json with content URIs rewritten to reference QUADBIN cell IDs within this file.
  - `cog`: For Cloud Optimized GeoTIFF sources — properties of the source COG including `native_zoom`, `block_size`, `num_bands`, `color_interpretation`, `original_dtypes`, `overview_factors`, `encoding`, and `image_dimensions`.

  Readers MUST ignore unrecognized top-level keys to ensure forward compatibility.

### Examples

#### 1. Vector Tile Set (from PMTiles)

```json
{
    "file_format": "tilequet",
    "version": "0.1.0",
    "tile_type": "vector",
    "tile_format": "pbf",
    "bounds": [11.15, 43.72, 11.35, 43.82],
    "bounds_crs": "EPSG:4326",
    "center": [11.25, 43.77, 14],
    "min_zoom": 0,
    "max_zoom": 14,
    "num_tiles": 85432,
    "tiling": {
        "scheme": "quadbin"
    },
    "name": "Firenze Vector Tiles",
    "description": "OpenStreetMap vector tiles for Florence, Italy",
    "attribution": "OpenStreetMap contributors",
    "layers": [
        {
            "id": "water",
            "minzoom": 0,
            "maxzoom": 14,
            "fields": {"class": "string"}
        },
        {
            "id": "buildings",
            "minzoom": 13,
            "maxzoom": 14,
            "fields": {"render_height": "number", "render_min_height": "number"}
        },
        {
            "id": "roads",
            "minzoom": 5,
            "maxzoom": 14,
            "fields": {"class": "string", "surface": "string"}
        }
    ],
    "processing": {
        "source_format": "pmtiles",
        "created_by": "tilequet-io 0.1.0",
        "created_at": "2026-02-09T10:30:00Z"
    }
}
```

#### 2. Raster Tile Set (from WMS)

```json
{
    "file_format": "tilequet",
    "version": "0.1.0",
    "tile_type": "raster",
    "tile_format": "png",
    "bounds": [-180.0, -85.051129, 180.0, 85.051129],
    "bounds_crs": "EPSG:4326",
    "center": [0.0, 0.0, 2],
    "min_zoom": 0,
    "max_zoom": 4,
    "num_tiles": 341,
    "tiling": {
        "scheme": "quadbin"
    },
    "name": "Weather Temperature",
    "tilejson": {
        "tilejson": "3.0.0",
        "tiles": [],
        "bounds": [-180.0, -85.051129, 180.0, 85.051129],
        "minzoom": 0,
        "maxzoom": 4,
        "center": [0.0, 0.0, 0],
        "name": "Weather Temperature"
    },
    "processing": {
        "source_format": "wms",
        "created_by": "tilequet-io 0.1.0",
        "created_at": "2026-02-09T10:30:00Z"
    }
}
```

#### 3. Raster Tile Set (from COG)

```json
{
    "file_format": "tilequet",
    "version": "0.1.0",
    "tile_type": "raster",
    "tile_format": "png",
    "bounds": [-180.0, -66.51326, 180.0, 66.51326],
    "bounds_crs": "EPSG:4326",
    "center": [0.0, 0.0, 0],
    "min_zoom": 0,
    "max_zoom": 4,
    "num_tiles": 341,
    "tiling": {
        "scheme": "quadbin"
    },
    "tilejson": {
        "tilejson": "3.0.0",
        "tiles": [],
        "bounds": [-180.0, -66.51326, 180.0, 66.51326],
        "minzoom": 0,
        "maxzoom": 4
    },
    "processing": {
        "source_format": "cog",
        "created_by": "tilequet-io 0.1.0",
        "created_at": "2026-02-09T10:30:00Z"
    },
    "cog": {
        "native_zoom": 4,
        "block_size": 256,
        "num_bands": 4,
        "color_interpretation": ["red", "green", "blue", "alpha"],
        "original_dtypes": ["uint8", "uint8", "uint8", "uint8"],
        "overview_factors": [2, 4, 8, 16],
        "source_crs": "EPSG:3857",
        "pixel_size_meters": 2504.688,
        "image_dimensions": [4096, 4096],
        "encoding": "png"
    }
}
```

#### 4. 3D Tile Set

```json
{
    "file_format": "tilequet",
    "version": "0.1.0",
    "tile_type": "3d",
    "tile_format": "glb",
    "bounds": [-3.8, 40.3, -3.6, 40.5],
    "bounds_crs": "EPSG:4326",
    "center": [-3.7, 40.4, 15],
    "min_zoom": 10,
    "max_zoom": 18,
    "num_tiles": 5200,
    "tiling": {
        "scheme": "quadbin"
    },
    "name": "Madrid 3D Buildings",
    "description": "3D building models for Madrid, Spain",
    "tilejson": {
        "tilejson": "3.0.0",
        "tiles": [],
        "bounds": [-3.8, 40.3, -3.6, 40.5],
        "minzoom": 10,
        "maxzoom": 18,
        "name": "Madrid 3D Buildings"
    },
    "tileset_json": {
        "asset": {"version": "1.0"},
        "geometricError": 500,
        "root": { "..." : "rewritten tileset.json referencing QUADBIN cell IDs" }
    },
    "processing": {
        "source_format": "3dtiles",
        "created_by": "tilequet-io 0.1.0",
        "created_at": "2026-02-09T10:30:00Z"
    }
}
```

## File Extension

TileQuet files MUST use `.parquet` as the file extension. This ensures compatibility with existing Parquet tools and maintains consistency with the underlying file format.

## Media Type

If a [media type](https://en.wikipedia.org/wiki/Media_type) is used, a TileQuet file MUST use [application/vnd.apache.parquet](https://www.iana.org/assignments/media-types/application/vnd.apache.parquet) as the media type.

## File Identification

To enable fast identification of TileQuet files without fully parsing the metadata row, producers SHOULD include a hint in the Parquet file-level key-value metadata:

- **Key**: `tilequet:version`
- **Value**: The specification version (e.g., `"0.1.0"`)

This allows readers to quickly distinguish TileQuet files from other Parquet files by reading only the Parquet footer.

**Fallback heuristic** for files without the key-value hint: A Parquet file is likely TileQuet if it contains:
- A `tile` column of type UINT64
- A `metadata` column of type STRING/UTF8
- A `data` column of type BYTE_ARRAY

## Coordinate System Notes

### MBTiles Y-Axis Inversion

MBTiles uses the TMS coordinate system where the Y axis is inverted compared to the XYZ / Web Mercator convention. When converting from MBTiles, the Y coordinate MUST be converted:

```
xyz_y = (2^zoom) - 1 - tms_y
```

The QUADBIN cell ID in TileQuet always uses the XYZ convention (same as PMTiles, Google Maps, OpenStreetMap).

## Custom Metadata Extension

Producers MAY extend the metadata with custom fields. To avoid conflicts with future specification versions:

1. Custom fields SHOULD be placed under a `custom` object:
   ```json
   {
       "file_format": "tilequet",
       "version": "0.1.0",
       "custom": {
           "organization": "ACME Corp",
           "project_id": "basemap-2026"
       }
   }
   ```

2. Alternatively, custom fields at the root level SHOULD use a namespace prefix (e.g., `"acme:project_id"`).

Readers MUST ignore unrecognized fields to ensure forward compatibility.

## Design Rationale

### Why Store Tiles in Parquet?

The primary motivation is **governance and analytics**, not serving performance:

1. **SQL accessibility**: Tile set metadata and content can be queried with standard SQL in DuckDB, BigQuery, Snowflake, or any Parquet-compatible engine.
2. **Data warehouse integration**: Tile sets become tables that can be cataloged, versioned (via Iceberg/Delta Lake), and governed alongside other data.
3. **Cross-tile-set analytics**: Coverage analysis, size distribution, quality checks, and tile-set comparisons become trivial SQL queries.
4. **Cloud-native storage**: Parquet's columnar format with row group pruning enables efficient remote access without specialized tile servers.

### Why Not Replace PMTiles or MBTiles?

TileQuet is complementary to serving-optimized formats:

- **PMTiles** is optimized for serving individual tiles via HTTP range requests (1–2 requests per tile). TileQuet cannot match this serving efficiency.
- **MBTiles** is optimized for local tile serving via SQLite queries. TileQuet targets cloud/warehouse environments.

TileQuet targets the **governance and analytics** layer. A typical workflow might be:

```
Source Data → PMTiles (serving) + TileQuet (governance/analytics)
```

Or:

```
MBTiles → TileQuet (for warehouse import) → Analytics/Governance
```

### Why Metadata in a Row vs Parquet File Metadata?

Following the same rationale as [RaQuet](https://raquet.io):

1. **SQL accessibility**: Metadata can be queried with standard SQL without special Parquet metadata APIs.
2. **Data warehouse compatibility**: BigQuery, Snowflake, Redshift, and DuckDB can read row data easily; accessing file-level metadata varies by platform.
3. **Schema consistency**: The metadata row follows the same schema as data rows.
4. **Streaming writes**: Row-based metadata doesn't require rewriting file footers.

### Why Tiles as Opaque Binary Blobs?

TileQuet stores tile content as-is without decoding. This is intentional:

1. **Format independence**: The same schema works for PBF, PNG, JPEG, WebP, GLB, and any future format.
2. **Lossless round-trip**: Tiles can be extracted and served without any transformation.
3. **Simplicity**: No need for format-specific encoders/decoders in the specification.
4. **Performance**: No encoding/decoding overhead during conversion or retrieval.

### Why QUADBIN?

QUADBIN provides the same benefits for tile sets as it does for RaQuet raster data:

1. **Single-column index**: Z/X/Y in one 64-bit integer.
2. **Morton order**: Enables Parquet row group pruning for spatial queries.
3. **Ecosystem**: Libraries available in Python, JavaScript, and SQL (DuckDB, BigQuery, Snowflake).
