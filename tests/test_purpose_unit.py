from pathlib import Path
from dataclasses import replace

import pandas as pd

from uk_travel_pipeline.config import ReassignConfig
from uk_travel_pipeline.purpose import (
    _bt_period_key,
    _bt_purpose_period_key,
    build_nts0502_raked_purpose_shares,
    build_period_purpose_shares,
    build_msoa_mode_time_shares,
    build_msoa_purpose_shares,
    run_purpose_estimation,
)


def _write_purpose_inputs(tmp_path: Path) -> ReassignConfig:
    pop = tmp_path / "pop_lsoa_internal.csv"
    tfn = tmp_path / "tfn_area_type_lsoa21.csv"
    lookup = tmp_path / "lsoa_msoa.csv"
    nts = tmp_path / "mode_time_split.csv"
    purposes = tmp_path / "purposes.csv"
    adjusted = tmp_path / "trips_adjusted.parquet"
    purpose_out = tmp_path / "trips_adjusted_by_purpose.parquet"

    pd.DataFrame(
        {
            "adult_nssec": [1, 2, 1],
            "gender_3": [1, 2, 1],
            "ns_sec": [1, 1, 2],
            "soc": [1, 1, 1],
            "aws": [1, 1, 1],
            "hh_type": [1, 1, 2],
            "notes": ["ignored", "ignored", "ignored"],
            "E01000001": [10.0, 5.0, 0.0],
            "W01000001": [20.0, 5.0, 10.0],
        }
    ).to_csv(pop, index=False)
    pd.DataFrame(
        {
            "lsoa21_code": ["E01000001", "W01000001"],
            "tfn_area_type": [1, 1],
        }
    ).to_csv(tfn, index=False)
    pd.DataFrame(
        {
            "LSOA21CD": ["E01000001", "W01000001"],
            "MSOA21CD": ["E02000001", "W02000001"],
        }
    ).to_csv(lookup, index=False)
    pd.DataFrame(
        {
            "hh_type": [1, 1, 2, 2],
            "tfn_at": [1, 1, 1, 1],
            "mode": [3, 3, 3, 3],
            "period": [1, 1, 1, 1],
            "purpose": [1, 2, 1, 2],
            "trips.est": [10.0, 30.0, 20.0, 20.0],
            "rho": [1.0, 3.0, 2.0, 2.0],
        }
    ).to_csv(nts, index=False)
    pd.DataFrame(
        {
            "Purpose": [1, 2],
            "Description": ["Commuting", "Business"],
        }
    ).to_csv(purposes, index=False)
    pd.DataFrame(
        {
            "origin_msoa": ["E02000001", "W02000001"],
            "destination_msoa": ["W02000001", "E02000001"],
            "mode_of_transport": ["ROAD", "ROAD"],
            "time_period": ["AM_peak", "AM_peak"],
            "weekend_flag": [0, 0],
            "days_used": [1, 1],
            "volume_adj": [100.0, 200.0],
        }
    ).to_parquet(adjusted, index=False)

    return ReassignConfig(
        adjusted_parquet=adjusted,
        purpose_parquet=purpose_out,
        pop_lsoa_internal_csv=pop,
        tfn_area_type_lsoa_csv=tfn,
        lsoa_msoa_lookup_csv=lookup,
        nts_mode_time_split_csv=nts,
        purposes_csv=purposes,
    )


def test_build_msoa_purpose_shares_handles_all_population_style_file(tmp_path: Path):
    cfg = _write_purpose_inputs(tmp_path)

    shares = build_msoa_purpose_shares(cfg)

    assert set(shares["MSOA21CD"]) == {"E02000001", "W02000001"}
    assert set(shares["mode_of_transport"]) == {"ROAD"}
    share_sums = shares.groupby(["MSOA21CD", "mode_of_transport", "period_key"])["purpose_share"].sum()
    assert all(abs(v - 1.0) < 1e-9 for v in share_sums)


def test_build_msoa_mode_time_shares_aggregates_other_periods(tmp_path: Path):
    cfg = _write_purpose_inputs(tmp_path)
    pd.DataFrame(
        {
            "hh_type": [1, 1, 1, 1, 1, 1],
            "tfn_at": [1, 1, 1, 1, 1, 1],
            "mode": [1, 3, 1, 3, 1, 3],
            "period": [1, 1, 5, 5, 6, 6],
            "purpose": [1, 1, 1, 1, 1, 1],
            "trips.est": [1.0, 3.0, 1.0, 1.0, 3.0, 5.0],
            "rho": [1.0, 3.0, 1.0, 1.0, 3.0, 5.0],
        }
    ).to_csv(cfg.nts_mode_time_split_csv, index=False)

    shares = build_msoa_mode_time_shares(cfg)

    share_sums = shares.groupby(["MSOA21CD", "period_key"])["nts_mode_time_share"].sum()
    assert all(abs(v - 1.0) < 1e-9 for v in share_sums)
    e_am = shares[(shares["MSOA21CD"] == "E02000001") & (shares["period_key"] == "weekday_AM")]
    e_am_shares = dict(zip(e_am["mode_time_group"], e_am["nts_mode_time_share"]))
    assert abs(e_am_shares["WALKING"] - 0.25) < 1e-9
    assert abs(e_am_shares["ROAD"] - 0.75) < 1e-9
    e_other = shares[(shares["MSOA21CD"] == "E02000001") & (shares["period_key"] == "other")]
    e_other_shares = dict(zip(e_other["mode_time_group"], e_other["nts_mode_time_share"]))
    assert abs(e_other_shares["WALKING"] - 0.4) < 1e-9


def test_nts_non_am_periods_are_merged_into_other(tmp_path: Path):
    cfg = _write_purpose_inputs(tmp_path)
    pd.DataFrame(
        {
            "hh_type": [1, 1, 1, 1],
            "tfn_at": [1, 1, 1, 1],
            "mode": [3, 3, 3, 3],
            "period": [2, 2, 4, 4],
            "purpose": [1, 4, 1, 4],
            "trips.est": [1.0, 1.0, 1.0, 1.0],
            "rho": [1.0, 3.0, 1.0, 5.0],
        }
    ).to_csv(cfg.nts_mode_time_split_csv, index=False)

    shares = build_period_purpose_shares(cfg)

    assert set(shares["period_key"]) == {"other"}
    share_map = dict(zip(shares["purpose"], shares["purpose_share"]))
    assert abs(share_map[1] - 0.2) < 1e-9
    assert abs(share_map[4] - 0.8) < 1e-9


def test_purpose_shares_include_tfn_household_purpose_prior(tmp_path: Path):
    cfg = _write_purpose_inputs(tmp_path)
    pd.DataFrame(
        {
            "hh_type": [1, 1],
            "tfn_at": [1, 1],
            "mode": [3, 3],
            "period": [1, 1],
            "purpose": [1, 2],
            "trips.est": [90.0, 10.0],
            "rho": [1.0, 1.0],
        }
    ).to_csv(cfg.nts_mode_time_split_csv, index=False)

    shares = build_period_purpose_shares(cfg)

    share_map = dict(zip(shares["purpose"], shares["purpose_share"]))
    assert abs(share_map[1] - 0.9) < 1e-9
    assert abs(share_map[2] - 0.1) < 1e-9


def test_bt_period_labels_map_to_weekday_am_or_other():
    pdf = pd.DataFrame(
        {
            "time_period": ["Inter_peak", "Off_peak", "off_peak", "AM_peak", "PM_peak", "AM_peak"],
            "weekend_flag": [0, 0, 0, 0, 0, 1],
        }
    )

    out = _bt_period_key(pdf)

    assert out["period_key"].tolist() == [
        "other",
        "other",
        "other",
        "weekday_AM",
        "other",
        "other",
    ]


def test_bt_purpose_period_labels_map_to_bt_buckets():
    pdf = pd.DataFrame(
        {
            "time_period": ["Inter_peak", "Off_peak", "off_peak", "AM_peak", "PM_peak", "AM_peak"],
            "weekend_flag": [0, 0, 0, 0, 0, 1],
        }
    )

    out = _bt_purpose_period_key(pdf)

    assert out["purpose_period_key"].tolist() == [
        "weekday_off_peak",
        "weekday_off_peak",
        "weekday_off_peak",
        "weekday_AM",
        "weekday_PM",
        "weekend",
    ]


def test_nts0502_raking_preserves_rows_and_hits_controls(tmp_path: Path):
    cfg = _write_purpose_inputs(tmp_path)
    controls = tmp_path / "nts0502_controls.csv"
    pd.DataFrame(
        {
            "purpose_period_key": ["weekday_AM", "weekday_AM"],
            "control_group": ["commuting", "business"],
            "control_share": [0.4, 0.6],
            "source_year": ["test", "test"],
        }
    ).to_csv(controls, index=False)
    cfg = replace(cfg, nts0502_period_purpose_csv=controls)

    row_totals = pd.DataFrame(
        {
            "origin_msoa": ["E02000001"],
            "mode_of_transport": ["ROAD"],
            "purpose_period_key": ["weekday_AM"],
            "row_volume": [100.0],
        }
    )

    shares, check = build_nts0502_raked_purpose_shares(cfg, row_totals)

    assert abs(shares["purpose_share"].sum() - 1.0) < 1e-9
    share_map = dict(zip(shares["purpose"], shares["purpose_share"]))
    assert abs(share_map[1] - 0.4) < 1e-9
    assert abs(share_map[2] - 0.6) < 1e-9
    assert check["diff_trips"].abs().max() < 1e-6


def test_run_purpose_estimation_keeps_england_and_wales_origins(tmp_path: Path):
    cfg = _write_purpose_inputs(tmp_path)

    run_purpose_estimation(cfg)
    out = pd.read_parquet(cfg.purpose_parquet)

    assert set(out["origin_msoa"]) == {"E02000001", "W02000001"}
    totals = out.groupby("origin_msoa")["volume_adj_purpose"].sum().to_dict()
    assert abs(totals["E02000001"] - 100.0) < 1e-9
    assert abs(totals["W02000001"] - 200.0) < 1e-9


def test_run_purpose_estimation_falls_back_to_origin_period_purpose_shares(tmp_path: Path):
    cfg = _write_purpose_inputs(tmp_path)
    pd.DataFrame(
        {
            "origin_msoa": ["E02000001"],
            "destination_msoa": ["W02000001"],
            "mode_of_transport": ["RAIL"],
            "time_period": ["AM_peak"],
            "weekend_flag": [0],
            "days_used": [1],
            "volume_adj": [100.0],
        }
    ).to_parquet(cfg.adjusted_parquet, index=False)

    run_purpose_estimation(cfg)
    out = pd.read_parquet(cfg.purpose_parquet)

    assert set(out["mode_of_transport"]) == {"RAIL"}
    totals = out.groupby("origin_msoa")["volume_adj_purpose"].sum().to_dict()
    assert abs(totals["E02000001"] - 100.0) < 1e-9
    assert set(out["purpose"]) == {1, 2}


def test_run_purpose_estimation_nts0502_preserves_od_rows_and_writes_check(tmp_path: Path):
    cfg = _write_purpose_inputs(tmp_path)
    controls = tmp_path / "nts0502_controls.csv"
    pd.DataFrame(
        {
            "purpose_period_key": ["weekday_AM", "weekday_AM"],
            "control_group": ["commuting", "business"],
            "control_share": [0.4, 0.6],
            "source_year": ["test", "test"],
        }
    ).to_csv(controls, index=False)
    cfg = replace(
        cfg,
        purpose_calibration="nts0502",
        nts0502_period_purpose_csv=controls,
        outputs_root=tmp_path / "outputs",
    )

    run_purpose_estimation(cfg)
    out = pd.read_parquet(cfg.purpose_parquet)

    row_totals = out.groupby(["origin_msoa", "destination_msoa", "mode_of_transport"])[
        "volume_adj_purpose"
    ].sum()
    assert abs(row_totals.loc[("E02000001", "W02000001", "ROAD")] - 100.0) < 1e-9
    assert abs(row_totals.loc[("W02000001", "E02000001", "ROAD")] - 200.0) < 1e-9
    assert "purpose_period_key" in out.columns
    assert "local_prior_share" in out.columns

    purpose_totals = out.groupby("purpose")["volume_adj_purpose"].sum().to_dict()
    assert abs(purpose_totals[1] - 120.0) < 1e-6
    assert abs(purpose_totals[2] - 180.0) < 1e-6

    check = pd.read_csv(cfg.outputs_root / "reassign" / "nts0502_purpose_control_check.csv")
    assert check["diff_trips"].abs().max() < 1e-6


def test_split_road_purpose_shares_include_motorcycle_fallback(tmp_path: Path):
    cfg = replace(_write_purpose_inputs(tmp_path), split_road_mode=True)

    shares = build_msoa_purpose_shares(cfg)

    assert "MOTORCYCLE" in set(shares["mode_of_transport"])
    private_car = shares[shares["mode_of_transport"] == "PRIVATE_CAR"].sort_values(
        ["MSOA21CD", "period_key", "purpose"]
    )
    motorcycle = shares[shares["mode_of_transport"] == "MOTORCYCLE"].sort_values(
        ["MSOA21CD", "period_key", "purpose"]
    )
    assert private_car["purpose_share"].tolist() == motorcycle["purpose_share"].tolist()
