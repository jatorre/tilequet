---
layout: default
title: Overview
hero: true
hero_tagline: "Govern, query, and analyze tile sets with SQL. Bring map tiles into the data warehouse."
---

## What is TileQuet?

<div class="feature-grid">
  <div class="feature-card">
    <h3>Specification ({{ site.spec_version }})</h3>
    <p>TileQuet defines an open specification for storing map tile sets in Apache Parquet. Each tile becomes a row. Standard format, no proprietary extensions. Supports vector tiles, raster tiles, and 3D tiles.</p>
    <a href="https://github.com/jatorre/tilequet/blob/main/format-specs/tilequet.md">Read the specification &rarr;</a>
  </div>
  <div class="feature-card">
    <h3>Tools</h3>
    <p>Convert PMTiles, MBTiles, GeoPackage, XYZ/TMS URLs, ArcGIS MapServer, and 3D Tiles to TileQuet. Query tile sets with DuckDB, BigQuery, or Snowflake.</p>
    <a href="{{ site.baseurl }}/cli">View CLI reference &rarr;</a>
  </div>
  <div class="feature-card">
    <h3>Governance</h3>
    <p>Catalog, audit, and manage tile sets alongside your other data. TileQuet makes tile sets visible to data governance tools, SQL engines, and the modern analytics stack.</p>
    <a href="{{ site.baseurl }}/faq">Learn more &rarr;</a>
  </div>
</div>

---

## Why Parquet for Tiles?

**Tile sets are invisible to the data stack.** PMTiles and MBTiles are excellent for serving, but they're opaque binary files. You can't query them with SQL, catalog them in a data warehouse, or govern them alongside the rest of your data.

TileQuet changes this. By storing tiles in Apache Parquet — an open columnar format supported by every modern analytics engine — tile sets become **queryable tables**.

> **Key insight:** With TileQuet, you can answer questions like "how many tiles are empty at zoom 14?", "which tile sets overlap?", or "what's the total storage by zoom level?" — all with standard SQL.

---

## TileQuet Principles

- **Governance-First** — Tile sets as tables that can be cataloged, versioned (Iceberg/Delta), and governed
- **SQL-Queryable** — Analyze tile sets with DuckDB, BigQuery, Snowflake, or any Parquet engine
- **Format-Agnostic** — Stores vector tiles (PBF), raster tiles (PNG/JPEG/WebP), and 3D tiles (GLB) as binary blobs
- **Cloud-Native** — QUADBIN spatial indexing enables efficient tile lookups via row group pruning
- **Open Format** — Standard Parquet with no proprietary extensions
- **RaQuet Family** — Follows the same design principles as [RaQuet](https://raquet.io) (row-zero metadata, QUADBIN indexing)

---

## Supported Sources

| Source | Format | Description |
|--------|--------|-------------|
| **PMTiles** | `.pmtiles` | Cloud-optimized tile archive (v3) |
| **MBTiles** | `.mbtiles` | SQLite-based tile storage |
| **GeoPackage** | `.gpkg` | OGC standard SQLite tiles |
| **URL Template** | `{z}/{x}/{y}` | XYZ and TMS tile servers |
| **ArcGIS MapServer** | REST API | Pre-rendered map tiles |
| **3D Tiles** | `tileset.json` | OGC 3D Tiles (glTF/GLB/b3dm) |

---

## TileQuet vs PMTiles vs MBTiles

**TileQuet is complementary to serving formats**, not a replacement. It targets a different problem: governance and analytics.

| | **PMTiles** | **MBTiles** | **TileQuet** |
|---|---|---|---|
| **Best for** | Cloud-native tile serving | Local tile serving | Governance, analytics, cataloging |
| **Storage** | Single binary file | SQLite database | Parquet file |
| **SQL queryable** | No | Limited (SQLite) | Yes (DuckDB, BigQuery, Snowflake, ...) |
| **Tile serving** | Excellent (1-2 HTTP requests) | Good (local SQLite) | Viable (row group pruning) |
| **Data warehouse** | No | No | Native integration |
| **Versioning** | Manual | Manual | Iceberg / Delta Lake |

---

## Example Queries

### Count Tiles Per Zoom Level

```sql
SELECT
    quadbin_z(tile) AS zoom,
    COUNT(*) AS num_tiles,
    SUM(LENGTH(data)) / 1e6 AS total_mb
FROM read_parquet('tiles.parquet')
WHERE tile != 0
GROUP BY quadbin_z(tile)
ORDER BY zoom;
```

### Get Metadata

```sql
SELECT metadata
FROM read_parquet('tiles.parquet')
WHERE tile = 0;
```

### Compare Two Tile Sets

```sql
SELECT
    'tileset_a' AS source,
    COUNT(*) AS tiles,
    SUM(LENGTH(data)) / 1e6 AS size_mb
FROM read_parquet('tileset_a.parquet') WHERE tile != 0
UNION ALL
SELECT
    'tileset_b',
    COUNT(*),
    SUM(LENGTH(data)) / 1e6
FROM read_parquet('tileset_b.parquet') WHERE tile != 0;
```

---

## Getting Started

```bash
# Install
pip install "tilequet-io[all]"

# Convert PMTiles to TileQuet
tilequet-io convert pmtiles input.pmtiles output.parquet

# Convert MBTiles to TileQuet
tilequet-io convert mbtiles input.mbtiles output.parquet

# Inspect the output
tilequet-io inspect output.parquet

# Validate
tilequet-io validate output.parquet
```

<div style="margin-top: 2rem;">
<a href="{{ site.baseurl }}/cli" class="btn btn-primary">CLI Reference</a>
<a href="https://github.com/jatorre/tilequet/blob/main/format-specs/tilequet.md" class="btn btn-secondary" style="border-color: var(--color-accent); color: var(--color-accent);">Read the Spec</a>
</div>

---

## Part of the RaQuet Family

TileQuet follows the same design principles as [RaQuet](https://raquet.io), which stores raster data in Parquet:

| | **RaQuet** | **TileQuet** |
|---|---|---|
| **Data** | Raster pixels | Map tiles (vector, raster, 3D) |
| **Row content** | Pixel data per band | Tile binary blob |
| **Spatial index** | QUADBIN | QUADBIN |
| **Metadata** | Row-zero JSON | Row-zero JSON |
| **Governance** | SQL-queryable rasters | SQL-queryable tile sets |

Together, they bring the full spectrum of geospatial data into the modern analytics stack.

---

## Changelog

### v0.1.0 (Experimental)
- **Initial release**: Core specification for storing tile sets in Parquet with QUADBIN spatial indexing
- **6 converters**: PMTiles, MBTiles, GeoPackage, URL template (XYZ/TMS), ArcGIS MapServer, OGC 3D Tiles
- **CLI**: inspect (rich + plain text), convert, validate (with --json), split-zoom
- **Pluggable tiling scheme**: QUADBIN in v0.1.0, with octree and BVH schemes planned
