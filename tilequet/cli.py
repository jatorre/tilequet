#!/usr/bin/env python3
"""TileQuet CLI - Tools for working with TileQuet (Tiles + Parquet) files.

TileQuet stores map tile sets in Parquet format with QUADBIN spatial indexing.
"""

import json
import logging
import sys
from pathlib import Path

import click
import pyarrow.parquet as pq

from . import mbtiles2tilequet, pmtiles2tilequet, validate as validate_module


def setup_logging(verbose: bool):
    """Configure logging based on verbosity."""
    if verbose:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


def _format_bytes(size: int) -> str:
    """Format byte size as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def _print_conversion_result(output_file: str, result: dict):
    """Print standard conversion result."""
    file_size = Path(output_file).stat().st_size
    click.echo(
        f"Done: {result['num_tiles']} {result['tile_type']} tiles "
        f"({result['tile_format']}, z{result['min_zoom']}-{result['max_zoom']}) "
        f"→ {_format_bytes(file_size)}"
    )


# ─── Main CLI Group ──────────────────────────────────────────────────────────


@click.group()
@click.version_option(package_name="tilequet-io")
def cli():
    """TileQuet CLI - Tools for working with TileQuet (Tiles + Parquet) files.

    TileQuet stores map tile sets in Parquet format with QUADBIN spatial indexing.

    \b
    Examples:
        tilequet-io inspect file.parquet
        tilequet-io convert pmtiles input.pmtiles output.parquet
        tilequet-io convert mbtiles input.mbtiles output.parquet
        tilequet-io validate output.parquet
    """
    pass


# ─── Inspect Command ─────────────────────────────────────────────────────────


@cli.command("inspect")
@click.argument("file", type=click.Path(exists=True, path_type=Path))
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def inspect_command(file: Path, verbose: bool):
    """Inspect a TileQuet file and display its metadata.

    FILE is the path to a TileQuet (.parquet) file.

    \b
    Examples:
        tilequet-io inspect tiles.parquet
        tilequet-io inspect /path/to/vector.parquet -v
    """
    setup_logging(verbose)

    try:
        from rich.console import Console
        from rich.table import Table
        console = Console()
        use_rich = True
    except ImportError:
        use_rich = False

    try:
        from .metadata import read_metadata
        metadata = read_metadata(str(file))

        file_size = file.stat().st_size
        pf = pq.ParquetFile(file)
        num_rows = pf.metadata.num_rows
        num_row_groups = pf.metadata.num_row_groups

        if use_rich:
            console.print(f"\n[bold blue]TileQuet File:[/bold blue] {file.name}")
            console.print(f"[dim]Path: {file.absolute()}[/dim]\n")

            info_table = Table(title="General Information", show_header=False)
            info_table.add_column("Property", style="cyan")
            info_table.add_column("Value")
            info_table.add_row("File Size", _format_bytes(file_size))
            info_table.add_row("Total Rows", str(num_rows))
            info_table.add_row("Row Groups", str(num_row_groups))
            info_table.add_row("Tile Type", metadata.get("tile_type", "N/A"))
            info_table.add_row("Tile Format", metadata.get("tile_format", "N/A"))
            info_table.add_row("Zoom Range", f"{metadata.get('min_zoom', '?')}-{metadata.get('max_zoom', '?')}")
            info_table.add_row("Num Tiles", str(metadata.get("num_tiles", "N/A")))
            console.print(info_table)

            bounds = metadata.get("bounds")
            if bounds:
                bounds_table = Table(title="Bounds (WGS84)", show_header=False)
                bounds_table.add_column("Property", style="cyan")
                bounds_table.add_column("Value")
                bounds_table.add_row("West", f"{bounds[0]:.6f}")
                bounds_table.add_row("South", f"{bounds[1]:.6f}")
                bounds_table.add_row("East", f"{bounds[2]:.6f}")
                bounds_table.add_row("North", f"{bounds[3]:.6f}")
                console.print(bounds_table)

            layers = metadata.get("layers")
            if layers:
                layers_table = Table(title=f"Layers ({len(layers)} total)")
                layers_table.add_column("#", style="cyan")
                layers_table.add_column("ID")
                layers_table.add_column("Zoom")
                layers_table.add_column("Fields")
                for i, layer in enumerate(layers, 1):
                    zoom = f"z{layer.get('minzoom', '?')}-{layer.get('maxzoom', '?')}"
                    fields = str(len(layer.get("fields", {})))
                    layers_table.add_row(str(i), layer["id"], zoom, fields)
                console.print(layers_table)

            schema_table = Table(title="Parquet Schema")
            schema_table.add_column("Column", style="cyan")
            schema_table.add_column("Type")
            for field in pf.schema_arrow:
                schema_table.add_row(field.name, str(field.type))
            console.print(schema_table)
            console.print()

        else:
            click.echo(f"TileQuet file: {file}")
            click.echo(f"  File size:    {_format_bytes(file_size)}")
            click.echo(f"  Row groups:   {num_row_groups}")
            click.echo(f"  Total rows:   {num_rows}")
            click.echo()
            click.echo(f"  Format:       {metadata.get('file_format')} v{metadata.get('version')}")
            click.echo(f"  Tile type:    {metadata.get('tile_type')}")
            click.echo(f"  Tile format:  {metadata.get('tile_format')}")
            click.echo(f"  Zoom range:   {metadata.get('min_zoom')} - {metadata.get('max_zoom')}")
            click.echo(f"  Num tiles:    {metadata.get('num_tiles')}")

            bounds = metadata.get("bounds")
            if bounds:
                click.echo(f"  Bounds:       [{bounds[0]:.4f}, {bounds[1]:.4f}, {bounds[2]:.4f}, {bounds[3]:.4f}]")

            center = metadata.get("center")
            if center:
                click.echo(f"  Center:       [{center[0]:.4f}, {center[1]:.4f}] z{center[2]}")

            tiling = metadata.get("tiling", {})
            if tiling:
                click.echo(f"  Tiling:       {tiling.get('scheme', 'unknown')}")

            name = metadata.get("name")
            if name:
                click.echo(f"  Name:         {name}")
            description = metadata.get("description")
            if description:
                click.echo(f"  Description:  {description}")
            attribution = metadata.get("attribution")
            if attribution:
                click.echo(f"  Attribution:  {attribution}")

            layers = metadata.get("layers")
            if layers:
                click.echo(f"\n  Layers ({len(layers)}):")
                for layer in layers:
                    fields = layer.get("fields", {})
                    zoom_info = ""
                    if "minzoom" in layer or "maxzoom" in layer:
                        zoom_info = f" (z{layer.get('minzoom', '?')}-{layer.get('maxzoom', '?')})"
                    click.echo(f"    - {layer['id']}{zoom_info}: {len(fields)} fields")

            processing = metadata.get("processing")
            if processing:
                click.echo(f"\n  Source:       {processing.get('source_format', 'unknown')}")
                click.echo(f"  Created by:   {processing.get('created_by', 'unknown')}")
                click.echo(f"  Created at:   {processing.get('created_at', 'unknown')}")

        if verbose:
            click.echo("\nFull metadata:")
            click.echo(json.dumps(metadata, indent=2))

    except Exception as e:
        click.echo(f"Error reading file: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


# ─── Convert Command Group ───────────────────────────────────────────────────


@cli.group("convert")
def convert_group():
    """Convert various tile formats to TileQuet.

    \b
    Examples:
        tilequet-io convert pmtiles input.pmtiles output.parquet
        tilequet-io convert mbtiles input.mbtiles output.parquet
        tilequet-io convert geopackage input.gpkg output.parquet
        tilequet-io convert url "https://tile.osm.org/{z}/{x}/{y}.png" output.parquet
        tilequet-io convert mapserver https://server/.../MapServer output.parquet
        tilequet-io convert 3dtiles https://example.com/tileset.json output.parquet
        tilequet-io convert wms "https://wms.example.com" output.parquet -l layer1
        tilequet-io convert wmts "https://wmts.example.com" output.parquet -l layer1
    """
    pass


@convert_group.command("pmtiles")
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
@click.option("--row-group-size", type=int, default=200, help="Rows per Parquet row group (default: 200)")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def convert_pmtiles(input_file, output_file, row_group_size, verbose):
    """Convert a PMTiles file to TileQuet format.

    INPUT_FILE is the path to the source .pmtiles file.
    OUTPUT_FILE is the path for the output .parquet file.

    \b
    Examples:
        tilequet-io convert pmtiles map.pmtiles map.parquet
        tilequet-io convert pmtiles tiles.pmtiles output.parquet -v
    """
    setup_logging(verbose)
    click.echo(f"Converting {input_file} → {output_file}")

    try:
        result = pmtiles2tilequet.convert(input_file, output_file, row_group_size=row_group_size, verbose=verbose)
    except ImportError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error during conversion: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    _print_conversion_result(output_file, result)


@convert_group.command("mbtiles")
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
@click.option("--row-group-size", type=int, default=200, help="Rows per Parquet row group (default: 200)")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def convert_mbtiles(input_file, output_file, row_group_size, verbose):
    """Convert an MBTiles file to TileQuet format.

    INPUT_FILE is the path to the source .mbtiles file.
    OUTPUT_FILE is the path for the output .parquet file.

    \b
    Examples:
        tilequet-io convert mbtiles map.mbtiles map.parquet
        tilequet-io convert mbtiles tiles.mbtiles output.parquet -v
    """
    setup_logging(verbose)
    click.echo(f"Converting {input_file} → {output_file}")

    try:
        result = mbtiles2tilequet.convert(input_file, output_file, row_group_size=row_group_size, verbose=verbose)
    except Exception as e:
        click.echo(f"Error during conversion: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    _print_conversion_result(output_file, result)


@convert_group.command("geopackage")
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
@click.option("--table", "table_name", type=str, default=None, help="Tile table name (auto-detect if not specified)")
@click.option("--row-group-size", type=int, default=200, help="Rows per Parquet row group (default: 200)")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def convert_geopackage(input_file, output_file, table_name, row_group_size, verbose):
    """Convert a GeoPackage tile table to TileQuet format.

    INPUT_FILE is the path to the source .gpkg file.
    OUTPUT_FILE is the path for the output .parquet file.

    \b
    Examples:
        tilequet-io convert geopackage tiles.gpkg tiles.parquet
        tilequet-io convert geopackage data.gpkg output.parquet --table my_tiles
    """
    setup_logging(verbose)

    from . import geopackage2tilequet

    click.echo(f"Converting {input_file} → {output_file}")

    try:
        result = geopackage2tilequet.convert(
            input_file, output_file, table_name=table_name,
            row_group_size=row_group_size, verbose=verbose,
        )
    except Exception as e:
        click.echo(f"Error during conversion: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    _print_conversion_result(output_file, result)


@convert_group.command("url")
@click.argument("url_template")
@click.argument("output_file", type=click.Path())
@click.option("--min-zoom", type=int, default=0, help="Minimum zoom level (default: 0)")
@click.option("--max-zoom", type=int, default=5, help="Maximum zoom level (default: 5)")
@click.option("--bbox", type=str, default=None, help="Bounding box: west,south,east,north (WGS84)")
@click.option("--tms", is_flag=True, help="Use TMS Y convention (flipped Y axis)")
@click.option("--row-group-size", type=int, default=200, help="Rows per Parquet row group (default: 200)")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def convert_url(url_template, output_file, min_zoom, max_zoom, bbox, tms, row_group_size, verbose):
    """Convert tiles from a URL template to TileQuet format.

    URL_TEMPLATE is a tile URL with {z}, {x}, {y} placeholders.
    OUTPUT_FILE is the path for the output .parquet file.

    \b
    Examples:
        tilequet-io convert url "https://tile.osm.org/{z}/{x}/{y}.png" osm.parquet --max-zoom 3
        tilequet-io convert url "https://tiles.example.com/{z}/{x}/{y}.pbf" vector.parquet --bbox "-5,36,4,44"
        tilequet-io convert url "https://tms.example.com/{z}/{x}/{y}.png" tms.parquet --tms
    """
    setup_logging(verbose)

    from . import urltemplate2tilequet

    # Parse bbox
    bbox_tuple = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("Must have exactly 4 values")
            bbox_tuple = tuple(parts)
        except ValueError as e:
            click.echo(f"Error: Invalid bbox format. Expected west,south,east,north: {e}", err=True)
            sys.exit(1)

    click.echo(f"Fetching tiles from {url_template}")
    click.echo(f"  Zoom: {min_zoom}-{max_zoom}")
    if bbox_tuple:
        click.echo(f"  Bbox: {bbox_tuple}")
    if tms:
        click.echo("  TMS mode: enabled")

    try:
        result = urltemplate2tilequet.convert(
            url_template, output_file,
            min_zoom=min_zoom, max_zoom=max_zoom,
            bbox=bbox_tuple, tms=tms,
            row_group_size=row_group_size, verbose=verbose,
        )
    except ImportError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error during conversion: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    _print_conversion_result(output_file, result)
    if result.get("tiles_skipped"):
        click.echo(f"  ({result['tiles_skipped']} empty/missing tiles skipped)")


@convert_group.command("mapserver")
@click.argument("url")
@click.argument("output_file", type=click.Path())
@click.option("--token", type=str, default=None, help="ArcGIS authentication token")
@click.option("--bbox", type=str, default=None, help="Bounding box: west,south,east,north (WGS84)")
@click.option("--min-zoom", type=int, default=None, help="Minimum zoom level")
@click.option("--max-zoom", type=int, default=None, help="Maximum zoom level")
@click.option("--row-group-size", type=int, default=200, help="Rows per Parquet row group (default: 200)")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def convert_mapserver(url, output_file, token, bbox, min_zoom, max_zoom, row_group_size, verbose):
    """Convert an ArcGIS MapServer to TileQuet format.

    URL is the ArcGIS MapServer REST endpoint (e.g., .../MapServer).
    OUTPUT_FILE is the path for the output .parquet file.

    \b
    Examples:
        tilequet-io convert mapserver https://server/arcgis/.../MapServer tiles.parquet
        tilequet-io convert mapserver https://server/MapServer output.parquet --max-zoom 10
        tilequet-io convert mapserver https://server/MapServer output.parquet --bbox "-122.5,37.5,-122.0,38.0"
    """
    setup_logging(verbose)

    from . import mapserver2tilequet

    # Parse bbox
    bbox_tuple = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("Must have exactly 4 values")
            bbox_tuple = tuple(parts)
        except ValueError as e:
            click.echo(f"Error: Invalid bbox format. Expected west,south,east,north: {e}", err=True)
            sys.exit(1)

    click.echo(f"Converting MapServer {url} to TileQuet format...")

    try:
        result = mapserver2tilequet.convert(
            url, output_file,
            token=token, bbox=bbox_tuple,
            min_zoom=min_zoom, max_zoom=max_zoom,
            row_group_size=row_group_size, verbose=verbose,
        )
    except ImportError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error during conversion: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    _print_conversion_result(output_file, result)


@convert_group.command("3dtiles")
@click.argument("tileset_url")
@click.argument("output_file", type=click.Path())
@click.option("--max-tiles", type=int, default=None, help="Maximum number of tiles to fetch")
@click.option("--row-group-size", type=int, default=200, help="Rows per Parquet row group (default: 200)")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def convert_3dtiles(tileset_url, output_file, max_tiles, row_group_size, verbose):
    """Convert an OGC 3D Tiles tileset to TileQuet format.

    TILESET_URL is the URL to the tileset.json file.
    OUTPUT_FILE is the path for the output .parquet file.

    \b
    Examples:
        tilequet-io convert 3dtiles https://example.com/tileset.json buildings.parquet
        tilequet-io convert 3dtiles https://example.com/tileset.json output.parquet --max-tiles 100
    """
    setup_logging(verbose)

    from . import tiles3d2tilequet

    click.echo(f"Converting 3D Tiles {tileset_url} to TileQuet format...")

    try:
        result = tiles3d2tilequet.convert(
            tileset_url, output_file,
            max_tiles=max_tiles,
            row_group_size=row_group_size, verbose=verbose,
        )
    except ImportError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error during conversion: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    _print_conversion_result(output_file, result)


@convert_group.command("wms")
@click.argument("service_url")
@click.argument("output_file", type=click.Path())
@click.option("--layers", "-l", required=True, help="Comma-separated WMS layer names")
@click.option("--min-zoom", type=int, default=0, help="Minimum zoom level (default: 0)")
@click.option("--max-zoom", type=int, default=5, help="Maximum zoom level (default: 5)")
@click.option("--bbox", type=str, default=None, help="Bounding box: west,south,east,north (WGS84)")
@click.option("--tile-size", type=int, default=256, help="Tile size in pixels (default: 256)")
@click.option("--format", "image_format", type=str, default="image/png", help="Image format (default: image/png)")
@click.option("--wms-version", type=str, default="1.3.0", help="WMS version (default: 1.3.0)")
@click.option("--styles", type=str, default="", help="WMS styles parameter")
@click.option("--crs", type=str, default="EPSG:3857", help="CRS for requests (default: EPSG:3857)")
@click.option("--transparent/--no-transparent", default=True, help="Request transparent background")
@click.option("--row-group-size", type=int, default=200, help="Rows per Parquet row group (default: 200)")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def convert_wms(service_url, output_file, layers, min_zoom, max_zoom, bbox,
                tile_size, image_format, wms_version, styles, crs, transparent,
                row_group_size, verbose):
    """Convert a WMS (Web Map Service) to TileQuet format.

    SERVICE_URL is the WMS service base URL.
    OUTPUT_FILE is the path for the output .parquet file.

    \b
    Examples:
        tilequet-io convert wms "https://geo.weather.gc.ca/geomet" weather.parquet -l GDPS.ETA_TT
        tilequet-io convert wms "https://ows.example.com/wms" output.parquet -l layer1 --max-zoom 8
        tilequet-io convert wms "https://wms.example.com" output.parquet -l layer1 --bbox "-10,35,5,45"
    """
    setup_logging(verbose)

    from . import wms2tilequet

    # Parse bbox
    bbox_tuple = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("Must have exactly 4 values")
            bbox_tuple = tuple(parts)
        except ValueError as e:
            click.echo(f"Error: Invalid bbox format. Expected west,south,east,north: {e}", err=True)
            sys.exit(1)

    click.echo(f"Fetching WMS tiles from {service_url}")
    click.echo(f"  Layers: {layers}")
    click.echo(f"  Zoom: {min_zoom}-{max_zoom}")
    if bbox_tuple:
        click.echo(f"  Bbox: {bbox_tuple}")

    try:
        result = wms2tilequet.convert(
            service_url, output_file,
            layers=layers,
            min_zoom=min_zoom, max_zoom=max_zoom,
            bbox=bbox_tuple, tile_size=tile_size,
            image_format=image_format, version=wms_version,
            styles=styles, crs=crs, transparent=transparent,
            row_group_size=row_group_size, verbose=verbose,
        )
    except ImportError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error during conversion: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    _print_conversion_result(output_file, result)
    if result.get("tiles_skipped"):
        click.echo(f"  ({result['tiles_skipped']} empty/missing tiles skipped)")


@convert_group.command("wmts")
@click.argument("service_url")
@click.argument("output_file", type=click.Path())
@click.option("--layer", "-l", required=True, help="WMTS layer name")
@click.option("--tile-matrix-set", type=str, default="GoogleMapsCompatible", help="Tile matrix set (default: GoogleMapsCompatible)")
@click.option("--min-zoom", type=int, default=0, help="Minimum zoom level (default: 0)")
@click.option("--max-zoom", type=int, default=5, help="Maximum zoom level (default: 5)")
@click.option("--bbox", type=str, default=None, help="Bounding box: west,south,east,north (WGS84)")
@click.option("--format", "image_format", type=str, default="image/png", help="Image format (default: image/png)")
@click.option("--style", type=str, default="default", help="WMTS style (default: default)")
@click.option("--row-group-size", type=int, default=200, help="Rows per Parquet row group (default: 200)")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def convert_wmts(service_url, output_file, layer, tile_matrix_set, min_zoom, max_zoom,
                 bbox, image_format, style, row_group_size, verbose):
    """Convert a WMTS (Web Map Tile Service) to TileQuet format.

    SERVICE_URL is the WMTS service base URL.
    OUTPUT_FILE is the path for the output .parquet file.

    \b
    Examples:
        tilequet-io convert wmts "https://geo.weather.gc.ca/geomet" weather.parquet -l GDPS.ETA_TT
        tilequet-io convert wmts "https://wmts.example.com" output.parquet -l layer1 --max-zoom 8
    """
    setup_logging(verbose)

    from . import wmts2tilequet

    # Parse bbox
    bbox_tuple = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("Must have exactly 4 values")
            bbox_tuple = tuple(parts)
        except ValueError as e:
            click.echo(f"Error: Invalid bbox format. Expected west,south,east,north: {e}", err=True)
            sys.exit(1)

    click.echo(f"Fetching WMTS tiles from {service_url}")
    click.echo(f"  Layer: {layer}")
    click.echo(f"  TileMatrixSet: {tile_matrix_set}")
    click.echo(f"  Zoom: {min_zoom}-{max_zoom}")
    if bbox_tuple:
        click.echo(f"  Bbox: {bbox_tuple}")

    try:
        result = wmts2tilequet.convert(
            service_url, output_file,
            layer=layer, tile_matrix_set=tile_matrix_set,
            min_zoom=min_zoom, max_zoom=max_zoom,
            bbox=bbox_tuple, image_format=image_format,
            style=style,
            row_group_size=row_group_size, verbose=verbose,
        )
    except ImportError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error during conversion: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    _print_conversion_result(output_file, result)
    if result.get("tiles_skipped"):
        click.echo(f"  ({result['tiles_skipped']} empty/missing tiles skipped)")


# ─── Validate Command ────────────────────────────────────────────────────────


@cli.command("validate")
@click.argument("file", type=click.Path(exists=True, path_type=Path))
@click.option("-v", "--verbose", is_flag=True, help="Show detailed validation output")
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
def validate_command(file: Path, verbose: bool, json_output: bool):
    """Validate a TileQuet file for correctness.

    Performs comprehensive validation including:
    - Schema validation (required columns)
    - Metadata validation (version, structure)
    - Tile data integrity checks

    FILE is the path to a TileQuet (.parquet) file.

    \b
    Examples:
        tilequet-io validate tiles.parquet
        tilequet-io validate tiles.parquet -v
        tilequet-io validate tiles.parquet --json
    """
    try:
        result = validate_module.validate_tilequet(str(file))

        if json_output:
            output = {
                "is_valid": result.is_valid,
                "errors": result.errors,
                "warnings": result.warnings,
                "stats": result.stats,
            }
            click.echo(json.dumps(output, indent=2, default=str))
        else:
            click.echo(str(result))

            if verbose and result.metadata:
                click.echo("\nFull Metadata:")
                click.echo(json.dumps(result.metadata, indent=2))

        sys.exit(0 if result.is_valid else 1)

    except Exception as e:
        click.echo(f"Error validating file: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


# ─── Split-Zoom Command ──────────────────────────────────────────────────────


@cli.command("split-zoom")
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.argument("output_dir", type=click.Path(path_type=Path))
@click.option("--row-group-size", type=int, default=200, help="Rows per Parquet row group (default: 200)")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def split_zoom_command(input_file: Path, output_dir: Path, row_group_size: int, verbose: bool):
    """Split a TileQuet file by zoom level for optimized remote access.

    Creates separate files for each zoom level, enabling clients to query
    only the zoom level they need.

    INPUT_FILE is the path to a TileQuet (.parquet) file.
    OUTPUT_DIR is the directory for output files (zoom_N.parquet).

    \b
    Examples:
        tilequet-io split-zoom tiles.parquet ./split_output/
        tilequet-io split-zoom large.parquet ./by_zoom/ --row-group-size 100
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    from pyarrow.parquet import SortingColumn
    import quadbin

    setup_logging(verbose)

    try:
        output_dir.mkdir(parents=True, exist_ok=True)

        click.echo(f"Reading {input_file}...")
        table = pq.read_table(input_file)

        # Separate metadata and data rows
        metadata_mask = pc.equal(table.column("tile"), 0)
        metadata_table = table.filter(metadata_mask)
        data_table = table.filter(pc.invert(metadata_mask))

        if len(metadata_table) == 0:
            click.echo("Error: No metadata row found", err=True)
            sys.exit(1)

        metadata_json = json.loads(metadata_table.column("metadata")[0].as_py())

        # Group by zoom level
        tiles = data_table.column("tile").to_pylist()
        zoom_indices: dict[int, list[int]] = {}

        for i, tile_id in enumerate(tiles):
            x, y, z = quadbin.cell_to_tile(tile_id)
            if z not in zoom_indices:
                zoom_indices[z] = []
            zoom_indices[z].append(i)

        click.echo(f"Found {len(zoom_indices)} zoom levels: {sorted(zoom_indices.keys())}")

        files_written = []
        for zoom in sorted(zoom_indices.keys()):
            indices = zoom_indices[zoom]
            zoom_table = data_table.take(indices)

            # Sort by tile ID
            sort_indices = pc.sort_indices(zoom_table.column("tile"))
            zoom_table = zoom_table.take(sort_indices)

            # Update metadata
            zoom_metadata = metadata_json.copy()
            zoom_metadata["min_zoom"] = zoom
            zoom_metadata["max_zoom"] = zoom
            zoom_metadata["num_tiles"] = len(indices)

            # Create metadata row
            metadata_row = pa.table({
                "tile": pa.array([0], type=pa.uint64()),
                "metadata": [json.dumps(zoom_metadata)],
                "data": pa.array([None], type=pa.binary()),
            })

            final_table = pa.concat_tables([metadata_row, zoom_table])

            output_path = output_dir / f"zoom_{zoom}.parquet"
            pq.write_table(
                final_table, output_path,
                compression="zstd",
                row_group_size=row_group_size,
                write_page_index=True,
                write_statistics=True,
                sorting_columns=[SortingColumn(0)],
            )

            size_mb = output_path.stat().st_size / (1024 * 1024)
            click.echo(f"  zoom_{zoom}.parquet: {len(indices)} tiles, {size_mb:.1f} MB")
            files_written.append(output_path)

        total_size = sum(f.stat().st_size for f in files_written) / (1024 * 1024)
        orig_size = input_file.stat().st_size / (1024 * 1024)
        click.echo("\nSplit complete:")
        click.echo(f"  Original: {orig_size:.1f} MB")
        click.echo(f"  Split total: {total_size:.1f} MB")
        click.echo(f"  Files: {len(files_written)}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
