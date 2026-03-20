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

The conversion performs the following transformations on each record:

  1. Station ID    -- strips the "USGS-" prefix
                      (e.g. "USGS-14105700" -> "14105700")
  2. Timestamp     -- converts to UTC and formats as ISO 8601 with
                      "Z" suffix
  3. Parameter     -- maps USGS parameter codes to Canadian numeric
                      codes:
                        00060 (Discharge)    -> 47 (Debit)
                        00065 (Gage height)  -> 46 (Niveau d'eau)
                      Unmapped codes are passed through as-is.
  4. Value         -- converts imperial to metric (by default):
                        Discharge:   ft^3/s * 0.0283168466 = m^3/s
                        Gage height: ft * 0.3048 = m
                      Rounding is parameter-dependent: discharge
                      values are rounded to whole numbers (0 dp) to
                      match ECCC convention, while water-level values
                      use 3 decimal places.  Unmapped parameters
                      default to 3 dp.
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

Arguments
---------
  input              One or more USGS JSON files to convert
                     (positional, required).

Options
-------
  -o, --output PATH  Output destination.
                       - Single file mode (one input): treated as the
                         output file path.
                       - Batch mode (multiple inputs): treated as the
                         output directory.  The directory is created
                         if it does not exist.  Each output file is
                         auto-named as <station_id>_hydrometric.csv.
                       - If omitted: output is written to the current
                         directory as <station_id>_hydrometric.csv.

  --no-convert       Skip unit conversion.  Values are kept in
                     original USGS imperial units (ft^3/s for
                     discharge, ft for gage height).  Parameter codes
                     are still mapped to Canadian codes.  Useful when
                     downstream processing handles its own unit
                     conversion.

  -h, --help         Show the argparse help message and exit.

Examples
--------
  # Convert a single file, auto-name output
  # (writes 14105700_hydrometric.csv):
  python usgs_to_canadian.py 14105700.json

  # Convert a single file to a specific output path:
  python usgs_to_canadian.py 14105700.json -o columbia_river.csv

  # Batch-convert all JSON files in a directory:
  python usgs_to_canadian.py data/*.json -o converted/

  # Convert without unit conversion (keep ft^3/s, ft):
  python usgs_to_canadian.py 14105700.json --no-convert

  # Combine flags:
  python usgs_to_canadian.py data/*.json -o converted/ --no-convert
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

# USGS parameter code -> Canadian parameter code
PARAMETER_MAP: Dict[str, int] = {
    "00060": 47,  # Discharge / Debit
    "00065": 46,  # Water level / Niveau d'eau (gage height)
}

# Unit conversion factors: USGS imperial -> metric
UNIT_CONVERSIONS: Dict[str, float] = {
    "00060": 0.0283168466,  # ft^3/s -> m^3/s
    "00065": 0.3048,        # ft -> m
}

# Rounding precision per parameter code.
# ECCC uses whole numbers for discharge (m^3/s) and 3 decimal places
# for water level (m).  Unmapped parameters default to 3 dp.
ROUNDING_PRECISION: Dict[str, int] = {
    "00060": 0,
    "00065": 3,
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

    # Parameter code mapping
    usgs_param = props.get("parameter_code", "")
    canadian_param = PARAMETER_MAP.get(
        usgs_param, usgs_param
    )

    # Value with optional unit conversion
    raw_value = props.get("value")
    if raw_value is not None:
        value: Any = float(raw_value)
        if convert_units and usgs_param in UNIT_CONVERSIONS:
            value = value * UNIT_CONVERSIONS[usgs_param]
        precision = ROUNDING_PRECISION.get(usgs_param, 3)
        value = round(value, precision)
        # Convert to int when precision is 0 for clean output
        if precision == 0:
            value = int(value)
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
        "Parameter/Paramètre": canadian_param,
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


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for CLI invocation."""
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
    args = parser.parse_args(argv)

    convert_units = not args.no_convert

    if len(args.input) == 1:
        # Single file mode
        result = convert_file(
            args.input[0], args.output, convert_units
        )
        if result:
            print(f"Wrote {result}")
    else:
        # Batch mode
        out_dir = args.output or "."
        if args.output and not os.path.isdir(out_dir):
            os.makedirs(out_dir, exist_ok=True)

        for input_path in args.input:
            features = parse_usgs_json(input_path)
            if not features:
                print(
                    f"Skipping {input_path}: no features",
                    file=sys.stderr,
                )
                continue

            # Extract station ID from first feature
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

            # Pass pre-parsed features to avoid
            # double-reading
            result = convert_file(
                input_path=input_path,
                output_path=out_path,
                convert_units=convert_units,
                features=features,
            )
            if result:
                print(f"Wrote {result}")


if __name__ == "__main__":
    main()
