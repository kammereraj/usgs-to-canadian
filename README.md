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

Two modes of operation:

- **`extract`** — Extract a single station from a concatenated river file
  (multiple FeatureCollections appended back-to-back). Optionally filter to a
  single parameter code, or extract all parameters into one file.
- **`convert`** — Convert one or more single-station USGS JSON files (original
  behavior).

The conversion performs the following transformations on each record:

1. **Station ID** — strips the "USGS-" prefix (e.g. "USGS-14105700" -> "14105700"). IDs are sanitized to prevent path traversal.
2. **Timestamp** — converts to UTC and formats as ISO 8601 with "Z" suffix
3. **Parameter** — maps USGS parameter codes to Canadian numeric codes:
   - 00060 (Discharge) -> 47 (Débit)
   - 00065 (Gage height) -> 46 (Niveau d'eau)
   - Unmapped codes are passed through as-is.
4. **Value** — converts to metric (by default):
   - 00060: ft³/s × 0.0283168466 = m³/s (rounded to whole numbers)
   - 00065: ft × 0.3048 = m (rounded to 3 decimal places)
   - 00011: (°F − 32) × 5/9 = °C (rounded to 1 decimal place)
   - 00010, 00095, 00300: pass-through (already in metric/SI units)
5. **Approval** — maps to bilingual format:
   - "Provisional" -> "Provisional/Provisoire"
   - "Approved" -> "Approved/Approuvé"
   - "Working" -> "Provisional/Provisoire"
6. **Other fields** — Qualifier is carried over if present; Symbol, Grade, and
   Qualifiers columns are left empty (matching ECCC convention).

Output records are sorted chronologically by timestamp.

Output CSV columns (matching ECCC standard):
`ID, Date, Parameter/Paramètre, Value/Valeur, Qualifier/Qualificatif, Symbol/Symbole, Approval/Approbation, Grade/Classification, Qualifiers/Qualificatifs`

## Supported Parameter Codes

| Code  | Description              | USGS Units | Conversion        | Rounding |
|-------|--------------------------|------------|-------------------|----------|
| 00010 | Water temperature        | °C         | Pass-through      | 1 dp     |
| 00011 | Water temperature        | °F         | (F−32)×5/9 → °C   | 1 dp     |
| 00060 | Discharge                | ft³/s      | ×0.0283168466 → m³/s | 0 dp  |
| 00065 | Gage height              | ft         | ×0.3048 → m       | 3 dp     |
| 00095 | Specific conductance     | µS/cm      | Pass-through      | 0 dp     |
| 00300 | Dissolved oxygen         | mg/L       | Pass-through      | 1 dp     |

## Subcommands

### `extract` — concatenated river files

Extract a single station from a production river file (concatenated GeoJSON
FeatureCollections). When `PARAMETER_CODE` is provided, only that parameter is
extracted. When omitted, all parameters for the station are written to one file.

```
python usgs_to_canadian.py extract STATION_ID [PARAMETER_CODE] INPUT OUTPUT [--no-convert]
```

| Argument | Description |
|---|---|
| `STATION_ID` | Station ID without USGS- prefix (e.g. `01046500`). |
| `PARAMETER_CODE` | USGS parameter code (e.g. `00065`). Optional — if omitted, all parameters for the station are included. |
| `INPUT` | Path to river file (e.g. `usgs_river.1600`). |
| `OUTPUT` | Output CSV file path. |
| `--no-convert` | Skip unit conversion. |

### `convert` — single-station JSON files

Convert one or more single-station USGS JSON files (original behavior).

```
python usgs_to_canadian.py convert INPUT [INPUT...] [-o OUTPUT] [--no-convert]
```

| Argument / Option | Description |
|---|---|
| `INPUT` | One or more USGS JSON files to convert (positional, required). |
| `-o`, `--output PATH` | Output destination. Single file: output path. Multiple files: output directory (auto-named as `<station_id>_hydrometric.csv`). If omitted: current directory. |
| `--no-convert` | Skip unit conversion. |

### Legacy mode

If no subcommand is given, the script falls back to `convert` behavior:

```
python usgs_to_canadian.py INPUT [INPUT...] [-o OUTPUT] [--no-convert]
```

## Examples

```bash
# Extract one parameter from a river file:
python usgs_to_canadian.py extract 01046500 00065 \
    usgs_river.1600 01046500_00065.csv

# Extract all parameters for a station into one file:
python usgs_to_canadian.py extract 01046500 \
    usgs_river.1600 01046500_all.csv

# Extract temperature (F->C conversion):
python usgs_to_canadian.py extract 01046500 00011 \
    usgs_river.1600 01046500_00011.csv

# Convert a single file, auto-name output (writes 14105700_hydrometric.csv):
python usgs_to_canadian.py convert 14105700.json

# Convert a single file to a specific output path:
python usgs_to_canadian.py convert 14105700.json -o columbia_river.csv

# Batch-convert all JSON files in a directory, output to a subfolder:
python usgs_to_canadian.py convert data/*.json -o converted/

# Legacy mode (no subcommand):
python usgs_to_canadian.py 14105700.json -o columbia_river.csv
```

## Testing

Tests require `pytest` (the only non-stdlib dependency):

```bash
pip install pytest
python -m pytest tests/ -v
```

To run tests against a real USGS JSON file, set the `USGS_TEST_JSON` environment variable:

```bash
export USGS_TEST_JSON=/path/to/14105700.json
python -m pytest tests/ -v
```
