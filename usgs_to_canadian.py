#!/usr/bin/env python3
"""
usgs_to_canadian.py
===================
Convert USGS Water Data API JSON (GeoJSON) to Canadian hydrometric CSV format.

Dependencies: Python 3.7+ standard library only (json, csv, datetime, argparse,
os, sys). No third-party packages required — suitable for minimal HPC environments.

Overview
--------
The USGS Water Data API (https://api.waterdata.usgs.gov) returns observations as
GeoJSON FeatureCollections. Each Feature contains a single timestamped observation
with properties including station ID, parameter code, value, units, and approval
status. This script reads one or more of those JSON files and writes CSV files
matching the Environment and Climate Change Canada (ECCC) hydrometric data format.

The conversion performs the following transformations on each record:

  1. Station ID    — strips the "USGS-" prefix (e.g. "USGS-14105700" -> "14105700")
  2. Timestamp     — converts to UTC and formats as ISO 8601 with "Z" suffix
  3. Parameter     — maps USGS parameter codes to Canadian numeric codes:
                       00060 (Discharge)    -> 47 (Débit)
                       00065 (Gage height)  -> 46 (Niveau d'eau)
                     Unmapped codes are passed through as-is.
  4. Value         — converts imperial to metric (by default):
                       Discharge:   ft^3/s * 0.0283168466 = m^3/s
                       Gage height: ft * 0.3048 = m
                     Values are rounded to 3 decimal places.
  5. Approval      — maps to bilingual format:
                       "Provisional" -> "Provisional/Provisoire"
                       "Approved"    -> "Approved/Approuvé"
  6. Other fields  — Qualifier is carried over if present; Symbol, Grade, and
                     Qualifiers columns are left empty (matching ECCC convention).

Output records are sorted chronologically by timestamp.

Output CSV columns (matching ECCC standard):
  ID, Date, Parameter/Paramètre, Value/Valeur, Qualifier/Qualificatif,
  Symbol/Symbole, Approval/Approbation, Grade/Classification,
  Qualifiers/Qualificatifs

Arguments
---------
  input              One or more USGS JSON files to convert (positional, required).

Options
-------
  -o, --output PATH  Output destination.
                       - Single file mode (one input): treated as the output file path.
                       - Batch mode (multiple inputs): treated as the output directory.
                         The directory is created if it does not exist. Each output file
                         is auto-named as <station_id>_hydrometric.csv.
                       - If omitted: output is written to the current directory as
                         <station_id>_hydrometric.csv.

  --no-convert       Skip unit conversion. Values are kept in original USGS imperial
                     units (ft^3/s for discharge, ft for gage height). Parameter codes
                     are still mapped to Canadian codes. Useful when downstream
                     processing handles its own unit conversion.

  -h, --help         Show the argparse help message and exit.

Examples
--------
  # Convert a single file, auto-name output (writes 14105700_hydrometric.csv):
  python usgs_to_canadian.py 14105700.json

  # Convert a single file to a specific output path:
  python usgs_to_canadian.py 14105700.json -o columbia_river.csv

  # Batch-convert all JSON files in a directory, output to a subfolder:
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

# USGS parameter code -> Canadian parameter code
PARAMETER_MAP = {
    "00060": 47,  # Discharge / Débit
    "00065": 46,  # Water level / Niveau d'eau (gage height)
}

# Unit conversion factors: USGS imperial -> metric
UNIT_CONVERSIONS = {
    "00060": 0.0283168466,  # ft^3/s -> m^3/s
    "00065": 0.3048,        # ft -> m
}

# USGS approval -> Canadian bilingual approval
APPROVAL_MAP = {
    "Provisional": "Provisional/Provisoire",
    "Approved": "Approved/Approuvé",
}

CSV_HEADER = [
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


def parse_usgs_json(filepath):
    """Parse a USGS Water Data API GeoJSON file and return features."""
    with open(filepath, "r") as f:
        data = json.load(f)

    if data.get("type") != "FeatureCollection":
        raise ValueError(f"Expected GeoJSON FeatureCollection, got: {data.get('type')}")

    return data.get("features", [])


def convert_timestamp(iso_str):
    """Convert ISO 8601 timestamp (possibly with offset) to UTC 'Z' format."""
    # Handle various offset formats
    ts = iso_str.replace("+00:00", "+0000").replace("-", "T", 0)
    # Parse with timezone awareness
    try:
        dt = datetime.fromisoformat(iso_str)
    except ValueError:
        # Fallback: strip offset and assume UTC
        dt = datetime.strptime(iso_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)

    # Convert to UTC
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def convert_feature(props, convert_units=True):
    """Convert a single USGS feature's properties to a Canadian CSV row dict."""
    # Station ID: strip "USGS-" prefix
    station_id = props.get("monitoring_location_id", "")
    if station_id.startswith("USGS-"):
        station_id = station_id[5:]

    # Timestamp
    date_str = convert_timestamp(props["time"])

    # Parameter code mapping
    usgs_param = props.get("parameter_code", "")
    canadian_param = PARAMETER_MAP.get(usgs_param, usgs_param)

    # Value with optional unit conversion
    raw_value = props.get("value")
    if raw_value is not None:
        value = float(raw_value)
        if convert_units and usgs_param in UNIT_CONVERSIONS:
            value = value * UNIT_CONVERSIONS[usgs_param]
        # Round to 3 decimal places for metric values
        value = round(value, 3)
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


def convert_file(input_path, output_path=None, convert_units=True):
    """Convert a single USGS JSON file to Canadian hydrometric CSV."""
    features = parse_usgs_json(input_path)

    if not features:
        print(f"Warning: no features found in {input_path}", file=sys.stderr)
        return None

    rows = [convert_feature(f["properties"], convert_units) for f in features]

    # Sort by date
    rows.sort(key=lambda r: r["Date"])

    # Default output filename: <station_id>_hydrometric.csv
    if output_path is None:
        station_id = rows[0]["ID"] if rows else "unknown"
        output_path = f"{station_id}_hydrometric.csv"

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Convert USGS Water Data API JSON to Canadian hydrometric CSV format."
    )
    parser.add_argument(
        "input",
        nargs="+",
        help="One or more USGS JSON files to convert.",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output CSV file path, or directory for batch mode.",
    )
    parser.add_argument(
        "--no-convert",
        action="store_true",
        help="Skip unit conversion (keep original USGS imperial units).",
    )
    args = parser.parse_args()

    convert_units = not args.no_convert

    if len(args.input) == 1:
        # Single file mode
        out = args.output
        result = convert_file(args.input[0], out, convert_units)
        if result:
            print(f"Wrote {result}")
    else:
        # Batch mode
        out_dir = args.output or "."
        if args.output and not os.path.isdir(out_dir):
            os.makedirs(out_dir, exist_ok=True)

        for input_path in args.input:
            # Auto-name each output file
            features = parse_usgs_json(input_path)
            if not features:
                print(f"Skipping {input_path}: no features", file=sys.stderr)
                continue
            station_id = features[0]["properties"].get("monitoring_location_id", "unknown")
            if station_id.startswith("USGS-"):
                station_id = station_id[5:]
            out_path = os.path.join(out_dir, f"{station_id}_hydrometric.csv")
            result = convert_file(input_path, out_path, convert_units)
            if result:
                print(f"Wrote {result}")


if __name__ == "__main__":
    main()
