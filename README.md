# usgs_to_canadian.py

Convert USGS Water Data API JSON (GeoJSON) to Canadian hydrometric CSV format.

**Dependencies:** Python 3.7+ standard library only (json, csv, datetime, argparse,
os, sys). No third-party packages required — suitable for minimal HPC environments.

## Overview

The USGS Water Data API (https://api.waterdata.usgs.gov) returns observations as
GeoJSON FeatureCollections. Each Feature contains a single timestamped observation
with properties including station ID, parameter code, value, units, and approval
status. This script reads one or more of those JSON files and writes CSV files
matching the Environment and Climate Change Canada (ECCC) hydrometric data format.

The conversion performs the following transformations on each record:

1. **Station ID** — strips the "USGS-" prefix (e.g. "USGS-14105700" -> "14105700")
2. **Timestamp** — converts to UTC and formats as ISO 8601 with "Z" suffix
3. **Parameter** — maps USGS parameter codes to Canadian numeric codes:
   - 00060 (Discharge) -> 47 (Débit)
   - 00065 (Gage height) -> 46 (Niveau d'eau)
   - Unmapped codes are passed through as-is.
4. **Value** — converts imperial to metric (by default):
   - Discharge: ft^3/s * 0.0283168466 = m^3/s
   - Gage height: ft * 0.3048 = m
   - Values are rounded to 3 decimal places.
5. **Approval** — maps to bilingual format:
   - "Provisional" -> "Provisional/Provisoire"
   - "Approved" -> "Approved/Approuvé"
6. **Other fields** — Qualifier is carried over if present; Symbol, Grade, and
   Qualifiers columns are left empty (matching ECCC convention).

Output records are sorted chronologically by timestamp.

Output CSV columns (matching ECCC standard):
`ID, Date, Parameter/Paramètre, Value/Valeur, Qualifier/Qualificatif, Symbol/Symbole, Approval/Approbation, Grade/Classification, Qualifiers/Qualificatifs`

## Arguments

| Argument | Description |
|---|---|
| `input` | One or more USGS JSON files to convert (positional, required). |

## Options

| Option | Description |
|---|---|
| `-o`, `--output PATH` | Output destination. **Single file mode** (one input): treated as the output file path. **Batch mode** (multiple inputs): treated as the output directory; created if it does not exist; each file is auto-named as `<station_id>_hydrometric.csv`. **If omitted:** output is written to the current directory as `<station_id>_hydrometric.csv`. |
| `--no-convert` | Skip unit conversion. Values are kept in original USGS imperial units (ft^3/s for discharge, ft for gage height). Parameter codes are still mapped to Canadian codes. Useful when downstream processing handles its own unit conversion. |
| `-h`, `--help` | Show the argparse help message and exit. |

## Examples

```bash
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
```
