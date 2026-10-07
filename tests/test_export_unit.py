from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from uk_travel_pipeline.export import export_adjusted_trip_csv


def test_adjusted_csv_export_roundtrip_and_rerun(tmp_path: Path):
    source = tmp_path / "trips_adjusted.parquet"
    output = tmp_path / "trip_records"
    records = pd.DataFrame(
        {
            "origin_msoa": ["A", "B", "C", "D", "E", "F"],
            "destination_msoa": ["B", "C", "D", "E", "F", "A"],
            "mode_of_transport": ["PRIVATE_CAR", "BUS", "RAIL", "WALKING", "CYCLE", "BUS"],
            "description": ['Central, "Station"', "École", None, "Line one\nLine two", "Oxford", "Bath"],
            "volume_adj": [10.25, 2.5, 7.0, 0.0, 3.25, 1.0],
            "optional_value": [1.0, None, 3.0, 4.0, None, 6.0],
        },
        index=pd.Index(range(100, 106), name="saved_dataframe_index"),
    )
    dd.from_pandas(records, npartitions=3).to_parquet(source)
    output.mkdir()
    unrelated = output / "notes.csv"
    unrelated.write_text("review notes\n", encoding="utf-8")
    similarly_named = output / "part-notes.csv"
    similarly_named.write_text("keep me\n", encoding="utf-8")

    files = export_adjusted_trip_csv(source, output)

    assert [path.name for path in files] == ["part-00000.csv", "part-00001.csv", "part-00002.csv"]
    parts = [pd.read_csv(path) for path in files]
    assert all(part.columns.tolist() == records.columns.tolist() for part in parts)
    exported = pd.concat(parts, ignore_index=True).sort_values("origin_msoa").reset_index(drop=True)
    expected = records.reset_index(drop=True)
    expected["description"] = expected["description"].fillna(float("nan"))
    pd.testing.assert_frame_equal(exported, expected, check_dtype=False)
    assert exported["volume_adj"].sum() == records["volume_adj"].sum()
    assert not list(output.glob(".adjusted-trip-csv-*"))

    smaller_source = tmp_path / "smaller.parquet"
    records.iloc[:2].to_parquet(smaller_source, index=False)
    rerun_files = export_adjusted_trip_csv(smaller_source, output)

    assert rerun_files == [output / "part-00000.csv"]
    assert not (output / "part-00001.csv").exists()
    assert not (output / "part-00002.csv").exists()
    assert pd.read_csv(rerun_files[0])["volume_adj"].sum() == 12.75
    assert unrelated.read_text(encoding="utf-8") == "review notes\n"
    assert similarly_named.read_text(encoding="utf-8") == "keep me\n"


def test_adjusted_csv_export_requires_existing_input(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="Missing required input files"):
        export_adjusted_trip_csv(tmp_path / "missing.parquet", tmp_path / "csv")
    assert not (tmp_path / "csv").exists()


def test_adjusted_csv_export_keeps_source_dataset_read_only(tmp_path: Path):
    source = tmp_path / "trips.parquet"
    dd.from_pandas(pd.DataFrame({"volume_adj": [1.0]}), npartitions=1).to_parquet(source)

    with pytest.raises(ValueError, match="outside the source parquet dataset"):
        export_adjusted_trip_csv(source, source / "csv")
    assert not (source / "csv").exists()
