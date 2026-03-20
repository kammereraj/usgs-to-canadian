"""
Tests for usgs_to_canadian.py -- validates conversion
correctness and data consistency.
"""

import csv
import json
import os
import re
from pathlib import Path

import pytest

from usgs_to_canadian import (
    CSV_HEADER,
    UNIT_CONVERSIONS,
    convert_feature,
    convert_file,
    convert_timestamp,
    parse_usgs_json,
)

# Import ROUNDING_PRECISION if the main module exports it;
# fall back to a local definition so rounding tests work
# even before the main module is updated.
try:
    from usgs_to_canadian import ROUNDING_PRECISION
except ImportError:
    ROUNDING_PRECISION: dict = {"00060": 0, "00065": 3}

# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------

SAMPLE_FEATURE: dict = {
    "type": "Feature",
    "id": "abc123",
    "geometry": None,
    "properties": {
        "id": "abc123",
        "time_series_id": "ts001",
        "monitoring_location_id": "USGS-14105700",
        "parameter_code": "00060",
        "statistic_id": "00011",
        "time": "2026-03-19T17:30:00+00:00",
        "value": "121000",
        "unit_of_measure": "ft^3/s",
        "approval_status": "Provisional",
        "qualifier": None,
        "last_modified": (
            "2026-03-19T18:30:17.041697+00:00"
        ),
    },
}


def _make_geojson(features: list) -> dict:
    """Build a minimal valid GeoJSON FeatureCollection."""
    return {
        "type": "FeatureCollection",
        "features": features,
    }


@pytest.fixture
def single_feature_json(tmp_path: Path) -> str:
    """Write a one-record GeoJSON file; return its path."""
    path = tmp_path / "single.json"
    path.write_text(
        json.dumps(_make_geojson([SAMPLE_FEATURE]))
    )
    return str(path)


@pytest.fixture
def multi_feature_json(tmp_path: Path) -> str:
    """Write a multi-record GeoJSON file; return path."""
    features = []
    values = ["100", "200", "300"]
    times = [
        "2026-03-19T19:00:00+00:00",
        "2026-03-19T17:00:00+00:00",  # out of order
        "2026-03-19T18:00:00+00:00",
    ]
    for i, (val, t) in enumerate(zip(values, times)):
        feat = json.loads(json.dumps(SAMPLE_FEATURE))
        feat["properties"]["value"] = val
        feat["properties"]["time"] = t
        feat["id"] = f"id-{i}"
        feat["properties"]["id"] = f"id-{i}"
        features.append(feat)
    path = tmp_path / "multi.json"
    path.write_text(json.dumps(_make_geojson(features)))
    return str(path)


@pytest.fixture
def real_json() -> str:
    """Path to a real example file (skips if absent)."""
    path = os.environ.get(
        "USGS_TEST_JSON",
        "/mnt/c/Users/ajkcr/Downloads/14105700.json",
    )
    if not os.path.exists(path):
        pytest.skip("Real example JSON not found")
    return path


# -------------------------------------------------------------------
# parse_usgs_json
# -------------------------------------------------------------------

class TestParseUsgsJson:
    def test_valid_file(
        self, single_feature_json: str
    ) -> None:
        features = parse_usgs_json(single_feature_json)
        assert len(features) == 1
        assert features[0]["properties"]["value"] == "121000"

    def test_invalid_type_raises(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "bad.json"
        path.write_text(json.dumps({"type": "Feature"}))
        with pytest.raises(
            ValueError, match="FeatureCollection"
        ):
            parse_usgs_json(str(path))

    def test_empty_features(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "empty.json"
        path.write_text(json.dumps(_make_geojson([])))
        assert parse_usgs_json(str(path)) == []


# -------------------------------------------------------------------
# convert_timestamp
# -------------------------------------------------------------------

class TestConvertTimestamp:
    def test_utc_offset(self) -> None:
        result = convert_timestamp(
            "2026-03-19T17:30:00+00:00"
        )
        assert result == "2026-03-19T17:30:00Z"

    def test_negative_offset(self) -> None:
        # -05:00 should shift forward 5 hours to UTC
        result = convert_timestamp(
            "2026-03-19T12:00:00-05:00"
        )
        assert result == "2026-03-19T17:00:00Z"

    def test_positive_offset(self) -> None:
        result = convert_timestamp(
            "2026-03-20T02:00:00+09:00"
        )
        assert result == "2026-03-19T17:00:00Z"

    def test_already_utc_z(self) -> None:
        # "Z" suffix: fromisoformat handles it on 3.11+;
        # the fallback path handles older Pythons.
        result = convert_timestamp(
            "2026-03-19T17:30:00Z"
        )
        assert result == "2026-03-19T17:30:00Z"


# -------------------------------------------------------------------
# convert_feature
# -------------------------------------------------------------------

class TestConvertFeature:
    def test_station_id_stripped(self) -> None:
        row = convert_feature(
            SAMPLE_FEATURE["properties"]
        )
        assert row["ID"] == "14105700"

    def test_station_id_no_prefix(self) -> None:
        props = {
            **SAMPLE_FEATURE["properties"],
            "monitoring_location_id": "99999999",
        }
        row = convert_feature(props)
        assert row["ID"] == "99999999"

    def test_parameter_code_discharge(self) -> None:
        row = convert_feature(
            SAMPLE_FEATURE["properties"]
        )
        assert row["Parameter/Paramètre"] == 47

    def test_parameter_code_gage_height(self) -> None:
        props = {
            **SAMPLE_FEATURE["properties"],
            "parameter_code": "00065",
        }
        row = convert_feature(props)
        assert row["Parameter/Paramètre"] == 46

    def test_parameter_code_unmapped_passthrough(
        self,
    ) -> None:
        props = {
            **SAMPLE_FEATURE["properties"],
            "parameter_code": "99999",
        }
        row = convert_feature(props)
        assert row["Parameter/Paramètre"] == "99999"

    def test_unit_conversion_discharge(self) -> None:
        row = convert_feature(
            SAMPLE_FEATURE["properties"],
            convert_units=True,
        )
        precision = ROUNDING_PRECISION.get("00060", 3)
        raw = 121000 * UNIT_CONVERSIONS["00060"]
        expected = round(raw, precision)
        if precision == 0:
            expected = int(expected)
        assert row["Value/Valeur"] == expected

    def test_unit_conversion_gage_height(self) -> None:
        props = {
            **SAMPLE_FEATURE["properties"],
            "parameter_code": "00065",
            "value": "10",
        }
        row = convert_feature(props, convert_units=True)
        expected = round(
            10 * UNIT_CONVERSIONS["00065"], 3
        )
        assert row["Value/Valeur"] == expected

    def test_no_convert_flag(self) -> None:
        row = convert_feature(
            SAMPLE_FEATURE["properties"],
            convert_units=False,
        )
        assert row["Value/Valeur"] == 121000.0

    def test_null_value(self) -> None:
        props = {
            **SAMPLE_FEATURE["properties"],
            "value": None,
        }
        row = convert_feature(props)
        assert row["Value/Valeur"] == ""

    def test_approval_provisional(self) -> None:
        row = convert_feature(
            SAMPLE_FEATURE["properties"]
        )
        assert (
            row["Approval/Approbation"]
            == "Provisional/Provisoire"
        )

    def test_approval_approved(self) -> None:
        props = {
            **SAMPLE_FEATURE["properties"],
            "approval_status": "Approved",
        }
        row = convert_feature(props)
        assert (
            row["Approval/Approbation"]
            == "Approved/Approuvé"
        )

    def test_approval_working(self) -> None:
        """'Working' maps to 'Provisional/Provisoire'."""
        props = {
            **SAMPLE_FEATURE["properties"],
            "approval_status": "Working",
        }
        row = convert_feature(props)
        assert (
            row["Approval/Approbation"]
            == "Provisional/Provisoire"
        )

    def test_qualifier_carried_over(self) -> None:
        props = {
            **SAMPLE_FEATURE["properties"],
            "qualifier": "Ice",
        }
        row = convert_feature(props)
        assert row["Qualifier/Qualificatif"] == "Ice"

    def test_qualifier_null_becomes_empty(self) -> None:
        row = convert_feature(
            SAMPLE_FEATURE["properties"]
        )
        assert row["Qualifier/Qualificatif"] == ""

    def test_empty_columns(self) -> None:
        row = convert_feature(
            SAMPLE_FEATURE["properties"]
        )
        assert row["Symbol/Symbole"] == ""
        assert row["Grade/Classification"] == ""
        assert row["Qualifiers/Qualificatifs"] == ""

    def test_all_csv_columns_present(self) -> None:
        row = convert_feature(
            SAMPLE_FEATURE["properties"]
        )
        assert set(row.keys()) == set(CSV_HEADER)


# -------------------------------------------------------------------
# Rounding precision
# -------------------------------------------------------------------

class TestRoundingPrecision:
    """Discharge -> 0 decimals; water level -> 3."""

    def test_discharge_rounded_whole(self) -> None:
        """Param 00060 values should be whole numbers."""
        props = {
            **SAMPLE_FEATURE["properties"],
            "parameter_code": "00060",
            "value": "121000",
        }
        row = convert_feature(props, convert_units=True)
        value = row["Value/Valeur"]
        precision = ROUNDING_PRECISION.get("00060", 3)
        expected = round(
            121000 * UNIT_CONVERSIONS["00060"],
            precision,
        )
        assert value == expected
        # Must be a whole number (0 decimal places)
        assert float(value) == int(value)

    def test_water_level_rounded_3dp(self) -> None:
        """Param 00065 values should keep 3 decimals."""
        props = {
            **SAMPLE_FEATURE["properties"],
            "parameter_code": "00065",
            "value": "10.123456",
        }
        row = convert_feature(props, convert_units=True)
        value = row["Value/Valeur"]
        precision = ROUNDING_PRECISION.get("00065", 3)
        expected = round(
            10.123456 * UNIT_CONVERSIONS["00065"],
            precision,
        )
        assert value == expected
        # Should have at most 3 decimal places
        text = str(value)
        if "." in text:
            decimals = len(text.split(".")[1])
            assert decimals <= 3


# -------------------------------------------------------------------
# Station ID sanitization
# -------------------------------------------------------------------

class TestStationIdSanitization:
    """IDs with path-traversal chars must be sanitized."""

    def test_path_traversal_stripped(self) -> None:
        props = {
            **SAMPLE_FEATURE["properties"],
            "monitoring_location_id": (
                "USGS-../../etc/foo"
            ),
        }
        row = convert_feature(props)
        station_id = row["ID"]
        # Must not contain path separators or ".."
        assert ".." not in station_id
        assert "/" not in station_id
        assert "\\" not in station_id

    def test_sanitized_id_is_safe_filename(
        self,
    ) -> None:
        props = {
            **SAMPLE_FEATURE["properties"],
            "monitoring_location_id": (
                "USGS-../secret/../../passwd"
            ),
        }
        row = convert_feature(props)
        station_id = row["ID"]
        # Only safe characters remain
        assert re.match(r"^[A-Za-z0-9_\-]+$", station_id)


# -------------------------------------------------------------------
# convert_file (end-to-end)
# -------------------------------------------------------------------

class TestConvertFile:
    def test_produces_valid_csv(
        self,
        single_feature_json: str,
        tmp_path: Path,
    ) -> None:
        out = str(tmp_path / "out.csv")
        convert_file(single_feature_json, out)
        with open(out) as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == CSV_HEADER
            rows = list(reader)
            assert len(rows) == 1

    def test_record_count_preserved(
        self,
        multi_feature_json: str,
        tmp_path: Path,
    ) -> None:
        out = str(tmp_path / "out.csv")
        convert_file(multi_feature_json, out)
        with open(out) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 3

    def test_output_sorted_by_date(
        self,
        multi_feature_json: str,
        tmp_path: Path,
    ) -> None:
        out = str(tmp_path / "out.csv")
        convert_file(multi_feature_json, out)
        with open(out) as f:
            dates = [
                r["Date"] for r in csv.DictReader(f)
            ]
        assert dates == sorted(dates)

    def test_auto_naming(
        self,
        single_feature_json: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = convert_file(single_feature_json)
        assert result == "14105700_hydrometric.csv"
        assert os.path.exists(
            tmp_path / "14105700_hydrometric.csv"
        )

    def test_empty_features_returns_none(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "empty.json"
        path.write_text(json.dumps(_make_geojson([])))
        assert convert_file(str(path)) is None

    def test_no_convert_values_match_raw(
        self,
        multi_feature_json: str,
        tmp_path: Path,
    ) -> None:
        out = str(tmp_path / "out.csv")
        convert_file(
            multi_feature_json, out, convert_units=False
        )
        with open(out) as f:
            values = [
                float(r["Value/Valeur"])
                for r in csv.DictReader(f)
            ]
        # Raw: 100, 200, 300 (sorted by time: 200,300,100)
        assert sorted(values) == [100.0, 200.0, 300.0]


# -------------------------------------------------------------------
# Data consistency: round-trip checks against the input
# -------------------------------------------------------------------

class TestDataConsistency:
    """Every input record is accounted for and values
    are consistent."""

    def test_all_values_convertible_back(
        self,
        multi_feature_json: str,
        tmp_path: Path,
    ) -> None:
        """Converted metric values divided by the factor
        should recover original imperial values within
        the tolerance imposed by rounding."""
        with open(multi_feature_json) as f:
            data = json.load(f)
        original_values = sorted(
            float(feat["properties"]["value"])
            for feat in data["features"]
        )

        out = str(tmp_path / "out.csv")
        convert_file(
            multi_feature_json, out, convert_units=True
        )
        with open(out) as f:
            metric_values = sorted(
                float(r["Value/Valeur"])
                for r in csv.DictReader(f)
            )

        factor = UNIT_CONVERSIONS["00060"]
        precision = ROUNDING_PRECISION.get("00060", 3)
        for orig, metric in zip(
            original_values, metric_values
        ):
            recovered = metric / factor
            # Tolerance depends on rounding precision:
            # e.g. 0 decimals -> +-0.5/factor in the
            # original domain.
            tol = 0.5 * (10 ** -precision) / factor + 1
            assert abs(recovered - orig) < tol, (
                f"recovered {recovered} != orig {orig}"
            )

    def test_timestamps_all_present(
        self,
        multi_feature_json: str,
        tmp_path: Path,
    ) -> None:
        """Every input timestamp should appear (as UTC)
        in the output."""
        with open(multi_feature_json) as f:
            data = json.load(f)
        input_times = {
            convert_timestamp(
                feat["properties"]["time"]
            )
            for feat in data["features"]
        }

        out = str(tmp_path / "out.csv")
        convert_file(multi_feature_json, out)
        with open(out) as f:
            output_times = {
                r["Date"] for r in csv.DictReader(f)
            }

        assert input_times == output_times

    def test_station_ids_all_present(
        self,
        multi_feature_json: str,
        tmp_path: Path,
    ) -> None:
        """All station IDs should appear (stripped)."""
        with open(multi_feature_json) as f:
            data = json.load(f)
        input_ids = {
            feat["properties"][
                "monitoring_location_id"
            ].replace("USGS-", "")
            for feat in data["features"]
        }

        out = str(tmp_path / "out.csv")
        convert_file(multi_feature_json, out)
        with open(out) as f:
            output_ids = {
                r["ID"] for r in csv.DictReader(f)
            }

        assert input_ids == output_ids


# -------------------------------------------------------------------
# Real-file validation (only when the example file exists)
# -------------------------------------------------------------------

class TestRealFile:
    def test_record_count_matches(
        self, real_json: str, tmp_path: Path
    ) -> None:
        with open(real_json) as f:
            data = json.load(f)
        expected_count = data["numberReturned"]

        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == expected_count

    def test_header_matches_eccc(
        self, real_json: str, tmp_path: Path
    ) -> None:
        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == CSV_HEADER

    def test_no_empty_dates(
        self, real_json: str, tmp_path: Path
    ) -> None:
        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            for row in csv.DictReader(f):
                assert row["Date"] != ""
                assert row["Date"].endswith("Z")

    def test_no_empty_values(
        self, real_json: str, tmp_path: Path
    ) -> None:
        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            for row in csv.DictReader(f):
                assert row["Value/Valeur"] != ""
                float(row["Value/Valeur"])

    def test_values_are_metric(
        self, real_json: str, tmp_path: Path
    ) -> None:
        """All discharge values from the example are
        >100k cfs. In m^3/s they should be ~2800-7000."""
        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            for row in csv.DictReader(f):
                val = float(row["Value/Valeur"])
                assert val < 100000, (
                    f"Value {val} looks unconverted"
                )

    def test_all_input_timestamps_in_output(
        self, real_json: str, tmp_path: Path
    ) -> None:
        with open(real_json) as f:
            data = json.load(f)
        input_times = {
            convert_timestamp(
                feat["properties"]["time"]
            )
            for feat in data["features"]
        }

        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            output_times = {
                r["Date"] for r in csv.DictReader(f)
            }

        assert input_times == output_times
