---
layout: default
title: FAQ
page_header: true
page_description: "Frequently asked questions about TileQuet"
---

## General

### What is TileQuet?

TileQuet is an open specification for storing map tile sets (vector tiles, raster tiles, 3D tiles) in Apache Parquet format with QUADBIN spatial indexing. It makes tile sets queryable with SQL and compatible with modern data warehouses.

### How is TileQuet related to RaQuet?

TileQuet is part of the same family as [RaQuet](https://raquet.io). Both use the same design principles:
- **Row-zero metadata**: Tile set metadata stored as JSON in a special row for SQL accessibility
- **QUADBIN spatial indexing**: Efficient spatial queries via Parquet row group pruning
- **Standard Parquet**: No proprietary extensions

RaQuet stores **raster pixel data** (decoded pixel values per band). TileQuet stores **tile blobs** (the encoded tile content as-is: PBF, PNG, JPEG, GLB, etc.).

### Does TileQuet replace PMTiles or MBTiles?

No. TileQuet is **complementary** to serving-optimized formats. PMTiles is excellent for serving tiles via HTTP range requests. MBTiles is great for local serving via SQLite. TileQuet targets a different problem: **governance, analytics, and data warehouse integration**.

A typical workflow might be:
```
Source Data -> PMTiles (serving) + TileQuet (governance)
```

### What source formats are supported?

TileQuet can convert from 12 sources:
- **PMTiles** (v3) — Cloud-optimized tile archive
- **MBTiles** — SQLite-based tile storage
- **GeoPackage** — OGC standard SQLite tiles
- **URL Template** — XYZ and TMS tile servers (`{z}/{x}/{y}`)
- **TileJSON** — Metadata endpoint with tile URL templates
- **WMS** — OGC Web Map Service (GetMap requests)
- **WMTS** — OGC Web Map Tile Service (GetTile requests)
- **OGC API - Tiles** — Modern OGC RESTful tile API
- **OGC API - Maps** — Modern OGC RESTful map rendering API
- **ArcGIS MapServer** — Esri pre-rendered map tiles via REST API
- **OGC 3D Tiles** — tileset.json with glTF/GLB/b3dm/pnts content
- **Cloud Optimized GeoTIFF** — Tile-aligned COGs in EPSG:3857 (non-aligned rasters should use [RaQuet](https://raquet.io))

---

## Technical

### Why store tiles as opaque binary blobs?

TileQuet stores tile content (PBF, PNG, etc.) as-is, without decoding. This is intentional:

1. **Format independence**: The same schema works for any tile format
2. **Lossless round-trip**: Tiles can be extracted and served without transformation
3. **Simplicity**: No format-specific encoders/decoders needed
4. **Performance**: No encoding/decoding overhead

### What tile formats are supported?

Any binary tile format can be stored in TileQuet:
- **Vector**: PBF/MVT (Mapbox Vector Tiles)
- **Raster**: PNG, JPEG, WebP
- **3D**: GLB, b3dm, pnts

The `tile_format` field in metadata identifies the format.

### How does QUADBIN work for tile indexing?

QUADBIN encodes Web Mercator tile coordinates (z, x, y) into a single 64-bit integer. This provides:
- Efficient spatial queries via Parquet row group pruning
- Single-column spatial index (no composite keys)
- Morton order clustering (spatially adjacent tiles are numerically close)

### What about 3D tiles that use octrees?

TileQuet v0.1.0 supports QUADBIN (quadtree) indexing, which works for:
- All 2D tiles (vector and raster)
- 3D tiles with quadtree-based implicit tiling (terrain, buildings)

For 3D tiles with octree subdivision or explicit bounding volume hierarchies, future tiling schemes (`octbin`, `node_id`) are planned. The spec is designed to be extensible via the `tiling.scheme` metadata field.

### How big are TileQuet files compared to the source?

File sizes are typically similar to the source, since tile content is stored as-is. The Parquet overhead (schema, row group metadata, page indexes) is minimal. TileQuet defaults to no Parquet-level compression because tile data (PNG, JPEG, PBF, etc.) is already compressed by its own format.

### Can I query TileQuet files remotely (from S3/GCS)?

Yes. Parquet is designed for remote access, and tools like DuckDB can read Parquet files directly from cloud storage with row group pruning:

```sql
SELECT COUNT(*) FROM read_parquet('s3://bucket/tiles.parquet') WHERE tile != 0;
```

QUADBIN sorting ensures that spatial queries only read relevant row groups.

---

## Governance

### What does "governance" mean for tile sets?

Governance means being able to answer questions like:
- **What tile sets exist?** Catalog them in a data warehouse
- **What area do they cover?** Query bounds and zoom ranges with SQL
- **Who created them and when?** Processing metadata in every file
- **How have they changed?** Version tile sets with Iceberg or Delta Lake
- **What's the quality?** Analyze coverage gaps, tile sizes, empty tiles
- **How do they relate to other data?** JOIN tile metadata with other datasets

None of this is possible with PMTiles or MBTiles. TileQuet makes it trivial.

### Can I version tile sets with Iceberg?

Yes. Since TileQuet files are standard Parquet, they can be registered as Iceberg or Delta Lake tables. This enables:
- Time travel (query tile sets as of a specific date)
- Schema evolution
- ACID transactions
- Partition pruning

### Can I use TileQuet with BigQuery / Snowflake / Databricks?

Yes. Any platform that reads Parquet files can query TileQuet files. The metadata is a standard JSON string in a standard row — no special extensions needed.
