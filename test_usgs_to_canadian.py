"""Tests for usgs_to_canadian.py — validates conversion correctness and data consistency."""

import csv
import json
import os
import tempfile

import pytest

from usgs_to_canadian import (
    APPROVAL_MAP,
    CSV_HEADER,
    PARAMETER_MAP,
    UNIT_CONVERSIONS,
    convert_feature,
    convert_file,
    convert_timestamp,
    parse_usgs_json,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_FEATURE = {
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
        "last_modified": "2026-03-19T18:30:17.041697+00:00",
    },
}


def _make_geojson(features):
    """Build a minimal valid GeoJSON FeatureCollection."""
    return {"type": "FeatureCollection", "features": features}


@pytest.fixture
def single_feature_json(tmp_path):
    """Write a one-record GeoJSON file and return its path."""
    path = tmp_path / "single.json"
    path.write_text(json.dumps(_make_geojson([SAMPLE_FEATURE])))
    return str(path)


@pytest.fixture
def multi_feature_json(tmp_path):
    """Write a multi-record GeoJSON file with known values, return path."""
    features = []
    values = ["100", "200", "300"]
    times = [
        "2026-03-19T19:00:00+00:00",
        "2026-03-19T17:00:00+00:00",  # intentionally out of order
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
def real_json():
    """Path to the real example file if available (skips if not found)."""
    path = "/mnt/c/Users/ajkcr/Downloads/14105700.json"
    if not os.path.exists(path):
        pytest.skip("Real example JSON not found")
    return path


# ---------------------------------------------------------------------------
# parse_usgs_json
# ---------------------------------------------------------------------------

class TestParseUsgsJson:
    def test_valid_file(self, single_feature_json):
        features = parse_usgs_json(single_feature_json)
        assert len(features) == 1
        assert features[0]["properties"]["value"] == "121000"

    def test_invalid_type_raises(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text(json.dumps({"type": "Feature"}))
        with pytest.raises(ValueError, match="FeatureCollection"):
            parse_usgs_json(str(path))

    def test_empty_features(self, tmp_path):
        path = tmp_path / "empty.json"
        path.write_text(json.dumps(_make_geojson([])))
        assert parse_usgs_json(str(path)) == []


# ---------------------------------------------------------------------------
# convert_timestamp
# ---------------------------------------------------------------------------

class TestConvertTimestamp:
    def test_utc_offset(self):
        assert convert_timestamp("2026-03-19T17:30:00+00:00") == "2026-03-19T17:30:00Z"

    def test_negative_offset(self):
        # -05:00 should shift forward 5 hours in UTC
        assert convert_timestamp("2026-03-19T12:00:00-05:00") == "2026-03-19T17:00:00Z"

    def test_positive_offset(self):
        assert convert_timestamp("2026-03-20T02:00:00+09:00") == "2026-03-19T17:00:00Z"

    def test_already_utc_z(self):
        # fromisoformat handles Z in Python 3.11+; fallback handles older
        result = convert_timestamp("2026-03-19T17:30:00+00:00")
        assert result == "2026-03-19T17:30:00Z"


# ---------------------------------------------------------------------------
# convert_feature
# ---------------------------------------------------------------------------

class TestConvertFeature:
    def test_station_id_stripped(self):
        row = convert_feature(SAMPLE_FEATURE["properties"])
        assert row["ID"] == "14105700"

    def test_station_id_no_prefix(self):
        props = {**SAMPLE_FEATURE["properties"], "monitoring_location_id": "99999999"}
        row = convert_feature(props)
        assert row["ID"] == "99999999"

    def test_parameter_code_discharge(self):
        row = convert_feature(SAMPLE_FEATURE["properties"])
        assert row["Parameter/Paramètre"] == 47

    def test_parameter_code_gage_height(self):
        props = {**SAMPLE_FEATURE["properties"], "parameter_code": "00065"}
        row = convert_feature(props)
        assert row["Parameter/Paramètre"] == 46

    def test_parameter_code_unmapped_passthrough(self):
        props = {**SAMPLE_FEATURE["properties"], "parameter_code": "99999"}
        row = convert_feature(props)
        assert row["Parameter/Paramètre"] == "99999"

    def test_unit_conversion_discharge(self):
        row = convert_feature(SAMPLE_FEATURE["properties"], convert_units=True)
        expected = round(121000 * UNIT_CONVERSIONS["00060"], 3)
        assert row["Value/Valeur"] == expected

    def test_unit_conversion_gage_height(self):
        props = {**SAMPLE_FEATURE["properties"], "parameter_code": "00065", "value": "10"}
        row = convert_feature(props, convert_units=True)
        expected = round(10 * UNIT_CONVERSIONS["00065"], 3)
        assert row["Value/Valeur"] == expected

    def test_no_convert_flag(self):
        row = convert_feature(SAMPLE_FEATURE["properties"], convert_units=False)
        assert row["Value/Valeur"] == 121000.0

    def test_null_value(self):
        props = {**SAMPLE_FEATURE["properties"], "value": None}
        row = convert_feature(props)
        assert row["Value/Valeur"] == ""

    def test_approval_provisional(self):
        row = convert_feature(SAMPLE_FEATURE["properties"])
        assert row["Approval/Approbation"] == "Provisional/Provisoire"

    def test_approval_approved(self):
        props = {**SAMPLE_FEATURE["properties"], "approval_status": "Approved"}
        row = convert_feature(props)
        assert row["Approval/Approbation"] == "Approved/Approuvé"

    def test_qualifier_carried_over(self):
        props = {**SAMPLE_FEATURE["properties"], "qualifier": "Ice"}
        row = convert_feature(props)
        assert row["Qualifier/Qualificatif"] == "Ice"

    def test_qualifier_null_becomes_empty(self):
        row = convert_feature(SAMPLE_FEATURE["properties"])
        assert row["Qualifier/Qualificatif"] == ""

    def test_empty_columns(self):
        row = convert_feature(SAMPLE_FEATURE["properties"])
        assert row["Symbol/Symbole"] == ""
        assert row["Grade/Classification"] == ""
        assert row["Qualifiers/Qualificatifs"] == ""

    def test_all_csv_columns_present(self):
        row = convert_feature(SAMPLE_FEATURE["properties"])
        assert set(row.keys()) == set(CSV_HEADER)


# ---------------------------------------------------------------------------
# convert_file (end-to-end)
# ---------------------------------------------------------------------------

class TestConvertFile:
    def test_produces_valid_csv(self, single_feature_json, tmp_path):
        out = str(tmp_path / "out.csv")
        convert_file(single_feature_json, out)
        with open(out) as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == CSV_HEADER
            rows = list(reader)
            assert len(rows) == 1

    def test_record_count_preserved(self, multi_feature_json, tmp_path):
        out = str(tmp_path / "out.csv")
        convert_file(multi_feature_json, out)
        with open(out) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 3

    def test_output_sorted_by_date(self, multi_feature_json, tmp_path):
        out = str(tmp_path / "out.csv")
        convert_file(multi_feature_json, out)
        with open(out) as f:
            dates = [r["Date"] for r in csv.DictReader(f)]
        assert dates == sorted(dates)

    def test_auto_naming(self, single_feature_json, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = convert_file(single_feature_json)
        assert result == "14105700_hydrometric.csv"
        assert os.path.exists(tmp_path / "14105700_hydrometric.csv")

    def test_empty_features_returns_none(self, tmp_path):
        path = tmp_path / "empty.json"
        path.write_text(json.dumps(_make_geojson([])))
        assert convert_file(str(path)) is None

    def test_no_convert_values_match_raw(self, multi_feature_json, tmp_path):
        out = str(tmp_path / "out.csv")
        convert_file(multi_feature_json, out, convert_units=False)
        with open(out) as f:
            values = [float(r["Value/Valeur"]) for r in csv.DictReader(f)]
        # Raw values were 100, 200, 300 (sorted by time: 200, 300, 100)
        assert sorted(values) == [100.0, 200.0, 300.0]


# ---------------------------------------------------------------------------
# Data consistency: round-trip checks against the input JSON
# ---------------------------------------------------------------------------

class TestDataConsistency:
    """Verify that every input record is accounted for and values are consistent."""

    def test_all_values_convertible_back(self, multi_feature_json, tmp_path):
        """Converted metric values, divided by the conversion factor, should
        recover the original imperial values (within rounding tolerance)."""
        with open(multi_feature_json) as f:
            data = json.load(f)
        original_values = sorted(
            float(feat["properties"]["value"]) for feat in data["features"]
        )

        out = str(tmp_path / "out.csv")
        convert_file(multi_feature_json, out, convert_units=True)
        with open(out) as f:
            metric_values = sorted(
                float(r["Value/Valeur"]) for r in csv.DictReader(f)
            )

        factor = UNIT_CONVERSIONS["00060"]
        recovered = sorted(round(v / factor) for v in metric_values)
        assert recovered == [int(v) for v in original_values]

    def test_timestamps_all_present(self, multi_feature_json, tmp_path):
        """Every input timestamp should appear (as UTC) in the output."""
        with open(multi_feature_json) as f:
            data = json.load(f)
        input_times = {
            convert_timestamp(feat["properties"]["time"])
            for feat in data["features"]
        }

        out = str(tmp_path / "out.csv")
        convert_file(multi_feature_json, out)
        with open(out) as f:
            output_times = {r["Date"] for r in csv.DictReader(f)}

        assert input_times == output_times

    def test_station_ids_all_present(self, multi_feature_json, tmp_path):
        """All station IDs should appear (stripped) in the output."""
        with open(multi_feature_json) as f:
            data = json.load(f)
        input_ids = {
            feat["properties"]["monitoring_location_id"].replace("USGS-", "")
            for feat in data["features"]
        }

        out = str(tmp_path / "out.csv")
        convert_file(multi_feature_json, out)
        with open(out) as f:
            output_ids = {r["ID"] for r in csv.DictReader(f)}

        assert input_ids == output_ids


# ---------------------------------------------------------------------------
# Real-file validation (runs only when the example download is present)
# ---------------------------------------------------------------------------

class TestRealFile:
    def test_record_count_matches(self, real_json, tmp_path):
        with open(real_json) as f:
            data = json.load(f)
        expected_count = data["numberReturned"]

        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == expected_count

    def test_header_matches_eccc(self, real_json, tmp_path):
        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == CSV_HEADER

    def test_no_empty_dates(self, real_json, tmp_path):
        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            for row in csv.DictReader(f):
                assert row["Date"] != ""
                assert row["Date"].endswith("Z")

    def test_no_empty_values(self, real_json, tmp_path):
        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            for row in csv.DictReader(f):
                assert row["Value/Valeur"] != ""
                float(row["Value/Valeur"])  # should not raise

    def test_values_are_metric(self, real_json, tmp_path):
        """All discharge values from the example are >100k cfs.
        In m^3/s they should be roughly 2800-7000 — not 100k+."""
        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            for row in csv.DictReader(f):
                val = float(row["Value/Valeur"])
                assert val < 100000, f"Value {val} looks unconverted (still imperial)"

    def test_all_input_timestamps_in_output(self, real_json, tmp_path):
        with open(real_json) as f:
            data = json.load(f)
        input_times = {
            convert_timestamp(feat["properties"]["time"])
            for feat in data["features"]
        }

        out = str(tmp_path / "real_out.csv")
        convert_file(real_json, out)
        with open(out) as f:
            output_times = {r["Date"] for r in csv.DictReader(f)}

        assert input_times == output_times
