#!/usr/bin/env python3
"""
usgs_to_canadian.py
===================
Convert USGS Water Data API JSON (GeoJSON) to Canadian hydrometric CSV
format.

Dependencies: Python 3.7+ standard library only (json, csv, datetime,
argparse, os, sys). No third-party packages required -- suitable for
minimal HPC environments.

Overview
--------
The USGS Water Data API (https://api.waterdata.usgs.gov) returns
observations as GeoJSON FeatureCollections.  Each Feature contains a
single timestamped observation with properties including station ID,
parameter code, value, units, and approval status.  This script reads
one or more of those JSON files and writes CSV files matching the
Environment and Climate Change Canada (ECCC) hydrometric data format.

Three modes of operation:

  extract  -- Extract a single station from a concatenated river
              file (multiple FeatureCollections appended
              back-to-back).  Optionally filter to a single
              parameter code, or extract all parameters into
              one file.
  split    -- Split a concatenated river file into one CSV per
              station, each containing all parameters.  Output
              files are named ``<station_id>_hydrometric.csv``.
  convert  -- Convert one or more single-station USGS JSON files
              (original behavior).

The conversion performs the following transformations on each record:

  1. Station ID    -- strips the "USGS-" prefix
                      (e.g. "USGS-14105700" -> "14105700")
  2. Timestamp     -- converts to UTC and formats as ISO 8601 with
                      "Z" suffix
  3. Parameter     -- USGS parameter codes are preserved as-is
                      (e.g. 00060 for Discharge, 00065 for
                      Gage height).
  4. Value         -- converts to metric (by default):
                        00060: ft^3/s * 0.0283168466 = m^3/s
                        00065: ft * 0.3048 = m
                        00011: (F - 32) * 5/9 = C
                      Parameters already in metric (00010, 00095,
                      00300) pass through unchanged.
  5. Approval      -- maps to bilingual format:
                        "Provisional" -> "Provisional/Provisoire"
                        "Approved"    -> "Approved/Approuve"
                        "Working"     -> "Provisional/Provisoire"
  6. Other fields  -- Qualifier is carried over if present; Symbol,
                      Grade, and Qualifiers columns are left empty
                      (matching ECCC convention).

Output records are sorted chronologically by timestamp.

Output CSV columns (matching ECCC standard):
  ID, Date, Parameter/Parametre, Value/Valeur,
  Qualifier/Qualificatif, Symbol/Symbole, Approval/Approbation,
  Grade/Classification, Qualifiers/Qualificatifs

Subcommands
-----------
  extract STATION_ID [PARAMETER_CODE] INPUT OUTPUT [--no-convert]
      Extract one station from a concatenated river file and
      write to a CSV file.  If PARAMETER_CODE is omitted, all
      parameters for the station are included in one file.

  split INPUT [-o OUTPUT_DIR] [--no-convert]
      Split a concatenated river file into one CSV per
      station.  Each file contains all parameters for that
      station and is named ``<station_id>_hydrometric.csv``.
      If ``-o`` is given, files are written to that directory
      (created if needed); otherwise the current directory.

  convert INPUT [INPUT...] [-o OUTPUT] [--no-convert]
      Convert one or more single-station USGS JSON files
      (original behavior).

  (legacy) INPUT [INPUT...] [-o OUTPUT] [--no-convert]
      If no subcommand is given, falls back to 'convert' mode.

Supported parameter codes
-------------------------
  00010  Water temperature (degrees C) -- pass-through
  00011  Water temperature (degrees F) -- converted to C
  00060  Discharge (ft^3/s)            -- converted to m^3/s
  00065  Gage height (ft)              -- converted to m
  00095  Specific conductance (uS/cm)  -- pass-through
  00300  Dissolved oxygen              -- pass-through

Examples
--------
  # Extract one parameter from a concatenated river file:
  python usgs_to_canadian.py extract 01046500 00065 \\
      usgs_river.1600 01046500_00065.csv

  # Extract all parameters for a station into one file:
  python usgs_to_canadian.py extract 01046500 \\
      usgs_river.1600 01046500_all.csv

  # Split a river file into one CSV per station:
  python usgs_to_canadian.py split usgs_river.1600 -o output/

  # Convert a single file, auto-name output:
  python usgs_to_canadian.py convert 14105700.json

  # Legacy mode (no subcommand, same as convert):
  python usgs_to_canadian.py 14105700.json -o out.csv
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

# Unit conversion factors: USGS imperial -> metric
UNIT_CONVERSIONS: Dict[str, float] = {
    "00060": 0.0283168466,  # ft^3/s -> m^3/s
    "00065": 0.3048,        # ft -> m
}

# Rounding precision per parameter code.
# ECCC uses whole numbers for discharge (m^3/s) and 3 decimal places
# for water level (m).  Unmapped parameters default to 3 dp.
ROUNDING_PRECISION: Dict[str, int] = {
    "00010": 1,   # Water temperature (C)
    "00011": 1,   # Water temperature (F -> C)
    "00060": 0,   # Discharge (m^3/s)
    "00065": 3,   # Gage height (m)
    "00095": 0,   # Specific conductance (uS/cm)
    "00300": 1,   # Dissolved oxygen
}

# USGS approval -> Canadian bilingual approval
APPROVAL_MAP: Dict[str, str] = {
    "Provisional": "Provisional/Provisoire",
    "Approved": "Approved/Approuvé",
    "Working": "Provisional/Provisoire",
}

CSV_HEADER: List[str] = [
    "ID",
    "Date",
    "Parameter/Paramètre",
    "Value/Valeur",
    "Qualifier/Qualificatif",
    "Symbol/Symbole",
    "Approval/Approbation",
    "Grade/Classification",
    "Qualifiers/Qualificatifs",
]


def _sanitize_station_id(raw_id: str) -> str:
    """Strip 'USGS-' prefix and reject unsafe station IDs.

    Applies ``os.path.basename`` and rejects IDs that contain path
    separators or ``..`` to prevent path-traversal attacks when the
    ID is used in output file names.
    """
    sid = raw_id
    if sid.startswith("USGS-"):
        sid = sid[5:]
    sid = os.path.basename(sid)
    if ".." in sid or os.sep in sid or "/" in sid:
        raise ValueError(
            f"Unsafe station ID rejected: {raw_id!r}"
        )
    return sid


def parse_usgs_json(filepath: str) -> List[Dict[str, Any]]:
    """Parse a USGS Water Data API GeoJSON file.

    Returns the list of Feature dicts.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if data.get("type") != "FeatureCollection":
        raise ValueError(
            "Expected GeoJSON FeatureCollection, "
            f"got: {data.get('type')}"
        )

    return data.get("features", [])


def parse_concatenated_geojson(
    filepath: str,
) -> List[Dict[str, Any]]:
    """Parse a file containing concatenated GeoJSON
    FeatureCollections.

    Production river files concatenate multiple
    FeatureCollection JSON objects back-to-back with no
    delimiter.  Uses ``json.JSONDecoder.raw_decode`` to iterate
    through all objects and collect their features.

    Returns a flat list of all Feature dicts across all
    FeatureCollections in the file.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()

    decoder = json.JSONDecoder()
    all_features: List[Dict[str, Any]] = []
    idx = 0
    length = len(text)

    while idx < length:
        # Skip whitespace between JSON objects
        while idx < length and text[idx] in " \t\n\r":
            idx += 1
        if idx >= length:
            break

        try:
            obj, end_idx = decoder.raw_decode(text, idx)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Failed to parse JSON at position {idx} "
                f"in {filepath}: {exc}"
            ) from exc

        if obj.get("type") != "FeatureCollection":
            raise ValueError(
                f"Expected FeatureCollection at position "
                f"{idx}, got: {obj.get('type')}"
            )

        all_features.extend(obj.get("features", []))
        idx = end_idx

    return all_features


def filter_features(
    features: List[Dict[str, Any]],
    station_id: str,
    parameter_code: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Filter features by station ID and optionally parameter code.

    *station_id* should be the bare numeric ID (no ``USGS-``
    prefix).  Comparison strips the prefix from each feature's
    ``monitoring_location_id`` via ``_sanitize_station_id``.

    If *parameter_code* is ``None``, all parameters for the
    station are returned.
    """
    result = []
    for feat in features:
        props = feat.get("properties", {})
        raw_sid = props.get(
            "monitoring_location_id", ""
        )
        feat_sid = _sanitize_station_id(raw_sid)
        if feat_sid != station_id:
            continue
        if (parameter_code is not None
                and props.get("parameter_code", "")
                != parameter_code):
            continue
        result.append(feat)
    return result


def _convert_value(
    usgs_param: str,
    raw_value: float,
    convert_units: bool,
) -> Any:
    """Convert a raw USGS value to the target unit system.

    Handles multiplicative conversions (00060, 00065) via
    ``UNIT_CONVERSIONS``, affine conversion for 00011 (F -> C),
    and pass-through for all other parameters.  Rounds according
    to ``ROUNDING_PRECISION``.
    """
    value: Any = raw_value
    if convert_units:
        if usgs_param == "00011":
            # Fahrenheit -> Celsius
            value = (value - 32.0) * 5.0 / 9.0
        elif usgs_param in UNIT_CONVERSIONS:
            value = value * UNIT_CONVERSIONS[usgs_param]
    precision = ROUNDING_PRECISION.get(usgs_param, 3)
    value = round(value, precision)
    if precision == 0:
        value = int(value)
    return value


def convert_timestamp(iso_str: str) -> str:
    """Convert ISO 8601 timestamp to UTC 'Z' format."""
    try:
        dt = datetime.fromisoformat(iso_str)
    except ValueError:
        # Fallback: strip offset and assume UTC
        dt = datetime.strptime(
            iso_str[:19], "%Y-%m-%dT%H:%M:%S"
        ).replace(tzinfo=timezone.utc)

    # Convert to UTC
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def convert_feature(
    props: Dict[str, Any],
    convert_units: bool = True,
) -> Optional[Dict[str, Any]]:
    """Convert a single USGS feature's properties to a Canadian
    CSV row dict.

    Returns ``None`` if the record is missing a timestamp (with a
    warning to stderr).
    """
    # Station ID: sanitize
    raw_id = props.get("monitoring_location_id", "")
    station_id = _sanitize_station_id(raw_id)

    # Timestamp -- skip record if missing
    time_str = props.get("time")
    if time_str is None:
        print(
            "Warning: skipping record with missing "
            f"'time' (station {station_id})",
            file=sys.stderr,
        )
        return None
    date_str = convert_timestamp(time_str)

    # Parameter code
    usgs_param = props.get("parameter_code", "")

    # Value with optional unit conversion
    raw_value = props.get("value")
    if raw_value is not None:
        value = _convert_value(
            usgs_param, float(raw_value), convert_units
        )
    else:
        value = ""

    # Approval status
    approval = props.get("approval_status", "")
    approval = APPROVAL_MAP.get(approval, approval)

    # Qualifier
    qualifier = props.get("qualifier") or ""

    return {
        "ID": station_id,
        "Date": date_str,
        "Parameter/Paramètre": usgs_param,
        "Value/Valeur": value,
        "Qualifier/Qualificatif": qualifier,
        "Symbol/Symbole": "",
        "Approval/Approbation": approval,
        "Grade/Classification": "",
        "Qualifiers/Qualificatifs": "",
    }


def convert_file(
    input_path: Optional[str] = None,
    output_path: Optional[str] = None,
    convert_units: bool = True,
    features: Optional[List[Dict[str, Any]]] = None,
) -> Optional[str]:
    """Convert USGS JSON data to Canadian hydrometric CSV.

    If *features* is provided, those are used directly (the file
    is not re-read).  Otherwise *input_path* is parsed from disk.
    """
    if features is None:
        if input_path is None:
            raise ValueError(
                "Either input_path or features must be "
                "provided"
            )
        features = parse_usgs_json(input_path)

    source = input_path or "<pre-parsed>"
    if not features:
        print(
            f"Warning: no features found in {source}",
            file=sys.stderr,
        )
        return None

    rows: List[Dict[str, Any]] = []
    for feat in features:
        feat_props = feat.get("properties", {})
        row = convert_feature(feat_props, convert_units)
        if row is not None:
            rows.append(row)

    if not rows:
        print(
            f"Warning: no valid rows produced from {source}",
            file=sys.stderr,
        )
        return None

    # Sort by date
    rows.sort(key=lambda r: r["Date"])

    # Default output filename: <station_id>_hydrometric.csv
    if output_path is None:
        station_id = rows[0]["ID"] if rows else "unknown"
        output_path = f"{station_id}_hydrometric.csv"

    with open(
        output_path, "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def _resolve_extract_args(
    positionals: List[str],
) -> tuple:
    """Resolve the positional arguments for extract.

    Accepts either ``[PARAMETER_CODE] INPUT OUTPUT`` (2 or 3
    values).  When 3 are given the first is the parameter code.
    When 2 are given, parameter code is ``None`` (all params).

    Returns ``(parameter_code, input_path, output_path)``.
    """
    if len(positionals) == 3:
        return positionals[0], positionals[1], positionals[2]
    elif len(positionals) == 2:
        return None, positionals[0], positionals[1]
    else:
        raise SystemExit(
            "extract requires 2 or 3 positional arguments "
            "after STATION_ID: [PARAMETER_CODE] INPUT OUTPUT"
        )


def _cmd_extract(args: argparse.Namespace) -> None:
    """Handle the 'extract' subcommand."""
    convert_units = not args.no_convert
    param, input_path, output_path = _resolve_extract_args(
        args.positionals
    )
    all_features = parse_concatenated_geojson(input_path)
    matched = filter_features(
        all_features, args.station_id, param
    )
    if not matched:
        if param:
            label = (
                f"station {args.station_id} parameter "
                f"{param}"
            )
        else:
            label = f"station {args.station_id}"
        print(
            f"Warning: no features found for {label}",
            file=sys.stderr,
        )
        sys.exit(1)

    result = convert_file(
        input_path=input_path,
        output_path=output_path,
        convert_units=convert_units,
        features=matched,
    )
    if result:
        print(f"Wrote {result}")


def _cmd_split(args: argparse.Namespace) -> None:
    """Handle the 'split' subcommand.

    Reads a concatenated river file, discovers all unique
    station IDs, and writes one CSV per station containing
    all parameters.  Output files are named
    ``<station_id>_hydrometric.csv`` in the output directory.
    """
    convert_units = not args.no_convert
    all_features = parse_concatenated_geojson(args.input)

    if not all_features:
        print(
            f"Warning: no features found in {args.input}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Discover unique station IDs (preserving order)
    seen: Dict[str, bool] = {}
    for feat in all_features:
        props = feat.get("properties", {})
        raw_sid = props.get(
            "monitoring_location_id", ""
        )
        sid = _sanitize_station_id(raw_sid)
        if sid not in seen:
            seen[sid] = True
    station_ids = list(seen.keys())

    # Create output directory if needed
    out_dir = args.output_dir or "."
    if out_dir != "." and not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    for sid in station_ids:
        matched = filter_features(all_features, sid)
        out_path = os.path.join(
            out_dir,
            f"{sid}_hydrometric.csv",
        )
        result = convert_file(
            input_path=args.input,
            output_path=out_path,
            convert_units=convert_units,
            features=matched,
        )
        if result:
            print(f"Wrote {result}")


def _cmd_convert(args: argparse.Namespace) -> None:
    """Handle the 'convert' subcommand (original behavior)."""
    convert_units = not args.no_convert

    if len(args.input) == 1:
        result = convert_file(
            args.input[0], args.output_path, convert_units
        )
        if result:
            print(f"Wrote {result}")
    else:
        out_dir = args.output_path or "."
        if args.output_path and not os.path.isdir(out_dir):
            os.makedirs(out_dir, exist_ok=True)

        for input_path in args.input:
            features = parse_usgs_json(input_path)
            if not features:
                print(
                    f"Skipping {input_path}: no features",
                    file=sys.stderr,
                )
                continue

            first_props = features[0].get(
                "properties", {}
            )
            raw_sid = first_props.get(
                "monitoring_location_id", "unknown"
            )
            station_id = _sanitize_station_id(raw_sid)

            out_path = os.path.join(
                out_dir,
                f"{station_id}_hydrometric.csv",
            )

            result = convert_file(
                input_path=input_path,
                output_path=out_path,
                convert_units=convert_units,
                features=features,
            )
            if result:
                print(f"Wrote {result}")


def _build_legacy_parser() -> argparse.ArgumentParser:
    """Build the original (pre-subcommand) argument parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Convert USGS Water Data API JSON to "
            "Canadian hydrometric CSV format."
        ),
    )
    parser.add_argument(
        "input",
        nargs="+",
        help="One or more USGS JSON files to convert.",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        dest="output_path",
        help=(
            "Output CSV file path, or directory for "
            "batch mode."
        ),
    )
    parser.add_argument(
        "--no-convert",
        action="store_true",
        help=(
            "Skip unit conversion (keep original USGS "
            "imperial units)."
        ),
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for CLI invocation."""
    parser = argparse.ArgumentParser(
        description=(
            "Convert USGS Water Data API JSON to "
            "Canadian hydrometric CSV format."
        ),
    )
    subparsers = parser.add_subparsers(dest="command")

    # --- extract subcommand ---
    extract_p = subparsers.add_parser(
        "extract",
        help=(
            "Extract one station (all or one parameter) "
            "from a concatenated river file."
        ),
    )
    extract_p.add_argument(
        "station_id",
        help=(
            "Station ID (e.g. 01046500, without "
            "USGS- prefix)."
        ),
    )
    extract_p.add_argument(
        "positionals",
        nargs="+",
        metavar="ARG",
        help=(
            "[PARAMETER_CODE] INPUT OUTPUT.  If three "
            "values are given, the first is a USGS "
            "parameter code (e.g. 00065).  If two, all "
            "parameters for the station are extracted."
        ),
    )
    extract_p.add_argument(
        "--no-convert",
        action="store_true",
        help="Skip unit conversion.",
    )

    # --- split subcommand ---
    split_p = subparsers.add_parser(
        "split",
        help=(
            "Split a concatenated river file into one "
            "CSV per station (all parameters)."
        ),
    )
    split_p.add_argument(
        "input",
        help="Path to concatenated river file.",
    )
    split_p.add_argument(
        "-o", "--output-dir",
        default=None,
        dest="output_dir",
        help=(
            "Output directory for per-station CSV files. "
            "Defaults to current directory."
        ),
    )
    split_p.add_argument(
        "--no-convert",
        action="store_true",
        help="Skip unit conversion.",
    )

    # --- convert subcommand ---
    convert_p = subparsers.add_parser(
        "convert",
        help=(
            "Convert single-station USGS JSON files "
            "(original behavior)."
        ),
    )
    convert_p.add_argument(
        "input",
        nargs="+",
        help="One or more USGS JSON files to convert.",
    )
    convert_p.add_argument(
        "-o", "--output",
        default=None,
        dest="output_path",
        help=(
            "Output CSV file path, or directory for "
            "batch mode."
        ),
    )
    convert_p.add_argument(
        "--no-convert",
        action="store_true",
        help="Skip unit conversion.",
    )

    # Determine effective argv
    effective = argv if argv is not None else sys.argv[1:]

    # Route to subcommand, help, or legacy fallback
    if effective and effective[0] in (
        "extract", "split", "convert"
    ):
        args = parser.parse_args(effective)
        if args.command == "extract":
            _cmd_extract(args)
        elif args.command == "split":
            _cmd_split(args)
        else:
            _cmd_convert(args)
    elif not effective or effective[0] in ("-h", "--help"):
        # Show main help with subcommands listed
        parser.parse_args(effective)
    else:
        # Legacy fallback: first arg is a file path
        legacy_parser = _build_legacy_parser()
        legacy_args = legacy_parser.parse_args(effective)
        _cmd_convert(legacy_args)


if __name__ == "__main__":
    main()
