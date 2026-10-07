from pathlib import Path

import pandas as pd

from uk_travel_pipeline.config import MatrixConfig
from uk_travel_pipeline.matrix import run_matrices


def test_run_matrices_outputs_files(tmp_path: Path):
    adjusted = tmp_path / "data" / "processed" / "reassign" / "trips_adjusted.parquet"
    adjusted.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "origin_msoa": ["A", "A", "B"],
            "destination_msoa": ["B", "C", "A"],
            "mode_of_transport": ["ROAD", "RAIL", "ROAD"],
            "time_period": ["AM_peak", "AM_peak", "Inter_peak"],
            "weekend_flag": [0, 0, 1],
            "days_used": [1, 1, 2],
            "volume_adj": [10, 20, 6],
        }
    )
    df.to_parquet(adjusted, index=False)

    outputs = tmp_path / "outputs"
    cfg = MatrixConfig(adjusted_parquet=adjusted, purpose_parquet=None, outputs_root=outputs, modes=("ROAD", "RAIL"))
    run_matrices(cfg)

    assert (outputs / "matrices" / "typical_week_by_mode" / "OD_matrix_ROAD_adjusted.csv").exists()
    assert (outputs / "matrices" / "weekday_AMpeak_by_mode" / "OD_matrix_RAIL_adjusted.csv").exists()
    road = pd.read_csv(outputs / "matrices" / "typical_week_by_mode" / "OD_matrix_ROAD_adjusted.csv")
    assert road.shape == (3, 4)
    assert road["origin_msoa"].tolist() == ["A", "B", "C"]
    assert list(road.columns) == ["origin_msoa", "A", "B", "C"]
    assert road.loc[road["origin_msoa"] == "C", ["A", "B", "C"]].sum(axis=1).item() == 0
    all_week_mode = pd.read_csv(outputs / "qa" / "all_week_mode.csv")
    assert all_week_mode["mode"].tolist() == ["RAIL", "ROAD"]
    assert all_week_mode["trips"].tolist() == [20.0, 16.0]
    am_weekday_mode = pd.read_csv(outputs / "qa" / "am_weekday_mode.csv")
    assert am_weekday_mode["mode"].tolist() == ["RAIL", "ROAD"]
    assert am_weekday_mode["trips"].tolist() == [20.0, 10.0]


def test_run_matrices_outputs_purpose_files(tmp_path: Path):
    adjusted = tmp_path / "data" / "processed" / "reassign" / "trips_adjusted.parquet"
    purpose = tmp_path / "data" / "processed" / "reassign" / "trips_adjusted_by_purpose.parquet"
    nts_split = tmp_path / "mode_time_split.csv"
    purposes = tmp_path / "purposes.csv"
    adjusted.parent.mkdir(parents=True, exist_ok=True)
    base_df = pd.DataFrame(
        {
            "origin_msoa": ["A", "A"],
            "destination_msoa": ["B", "C"],
            "mode_of_transport": ["ROAD", "ROAD"],
            "time_period": ["AM_peak", "Inter_peak"],
            "weekend_flag": [0, 0],
            "days_used": [1, 1],
            "volume_adj": [8, 4],
        }
    )
    purpose_df = pd.DataFrame(
        {
            "origin_msoa": ["A", "A", "A", "A"],
            "destination_msoa": ["B", "B", "C", "C"],
            "mode_of_transport": ["ROAD", "ROAD", "ROAD", "ROAD"],
            "time_period": ["AM_peak", "AM_peak", "Inter_peak", "Inter_peak"],
            "weekend_flag": [0, 0, 0, 0],
            "days_used": [1, 1, 1, 1],
            "volume_adj": [8, 8, 4, 4],
            "purpose": [1, 2, 1, 2],
            "purpose_desc": ["Commuting", "Business", "Commuting", "Business"],
            "volume_adj_purpose": [6.0, 2.0, 1.0, 3.0],
        }
    )
    pd.DataFrame(
        {
            "hh_type": [1, 1, 1, 1],
            "tfn_at": [1, 1, 1, 1],
            "purpose": [1, 2, 1, 2],
            "mode": [3, 3, 3, 3],
            "period": [1, 1, 2, 2],
            "trips": [3.0, 1.0, 1.0, 3.0],
            "trips.est": [3.0, 1.0, 1.0, 3.0],
            "rho": [1.0, 1.0, 1.0, 1.0],
        }
    ).to_csv(nts_split, index=False)
    pd.DataFrame(
        {
            "Purpose": [1, 2],
            "Description": ["Commuting", "Business"],
        }
    ).to_csv(purposes, index=False)
    base_df.to_parquet(adjusted, index=False)
    purpose_df.to_parquet(purpose, index=False)

    outputs = tmp_path / "outputs"
    cfg = MatrixConfig(
        adjusted_parquet=adjusted,
        purpose_parquet=purpose,
        nts_mode_time_split_csv=nts_split,
        purposes_csv=purposes,
        outputs_root=outputs,
        modes=("ROAD",),
    )
    run_matrices(cfg)

    assert (outputs / "matrices" / "typical_week_by_mode" / "OD_matrix_ROAD_adjusted_by_purpose1.csv").exists()
    purpose_matrix = pd.read_csv(
        outputs / "matrices" / "typical_week_by_mode" / "OD_matrix_ROAD_adjusted_by_purpose1.csv"
    )
    assert purpose_matrix.shape == (3, 4)
    assert purpose_matrix["origin_msoa"].tolist() == ["A", "B", "C"]
    assert list(purpose_matrix.columns) == ["origin_msoa", "A", "B", "C"]
    assert purpose_matrix.loc[purpose_matrix["origin_msoa"] == "B", ["A", "B", "C"]].sum(axis=1).item() == 0
    all_week_purpose = pd.read_csv(outputs / "qa" / "all_week_purpose.csv")
    assert all_week_purpose[["purpose", "purpose_desc", "trips"]].to_dict("records") == [
        {"purpose": 1, "purpose_desc": "Commuting", "trips": 7.0},
        {"purpose": 2, "purpose_desc": "Business", "trips": 5.0},
    ]
    am_purpose = pd.read_csv(outputs / "qa" / "am_weekday_purpose.csv")
    assert am_purpose["trips"].tolist() == [6.0, 2.0]
    other_purpose = pd.read_csv(outputs / "qa" / "other_purpose.csv")
    assert other_purpose["trips"].tolist() == [1.0, 3.0]
    period_bucket_trips = pd.read_csv(outputs / "qa" / "period_bucket_trips.csv")
    assert period_bucket_trips["period_bucket"].tolist() == [
        "AM trips",
        "Other trips",
        "Total",
    ]
    assert period_bucket_trips["trips"].tolist() == [8.0, 4.0, 12.0]
    reweighted = pd.read_csv(outputs / "qa" / "purpose_share_reweighted_by_output_period.csv")
    assert reweighted["purpose"].tolist() == [1, 2]
    assert abs(reweighted.loc[0, "raw_reweighted_by_output_period_proportion"] - (7 / 12)) < 1e-9
    assert abs(reweighted.loc[0, "raw_reweighted_minus_output_proportion"]) < 1e-9
