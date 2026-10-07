import dask.dataframe as dd
import pandas as pd
import pytest

from uk_travel_pipeline.reassign import (
    add_distance_bands,
    apply_mode_time_constraint,
    apply_child_origin_uplift,
    calculate_mode_time_factors,
    expand_nts_mode_shares_to_split_road_modes,
    build_child_origin_uplift_factors,
    build_nts_mode_shares_by_region,
    build_road_split_shares_by_region,
    calculate_factors,
    _build_child_origin_uplift_diagnostics,
    _build_msoa_centroids,
)


def test_add_distance_bands_has_expected_labels():
    pdf = pd.DataFrame(
        {
            "distance_miles": [0.5, 1.5, 3.0, 11.0],
            "volume": [1, 1, 1, 1],
            "mode_of_transport": ["ROAD", "ROAD", "RAIL", "WALKING"],
        }
    )
    labels = ["0-1", "1-2", "2-5", "5-10", "10-25", "25-50", "50-100", "100+"]
    out = add_distance_bands(dd.from_pandas(pdf, npartitions=1), labels).compute()
    assert set(out["distance_band"].astype(str)) == {"0-1", "1-2", "2-5", "10-25"}


def test_build_msoa_centroids_prefers_bng_columns():
    msoa = pd.DataFrame(
        {
            "MSOA21CD": ["E02000001"],
            "origin_region": ["London"],
            "BNG_E": [532384],
            "BNG_N": [181355],
        }
    )

    out = _build_msoa_centroids(msoa)

    assert out.loc[0, "x"] == 532384
    assert out.loc[0, "y"] == 181355


def test_build_child_origin_uplift_factors_aggregates_lsoa_to_msoa(tmp_path):
    pop = tmp_path / "pop_lsoa_internal.csv"
    lookup = tmp_path / "lsoa_msoa.csv"
    pd.DataFrame(
        {
            "gender_3": [1, 2, 3, 2],
            "aws": [1, 2, 3, 2],
            "hh_type": [1, 1, 1, 2],
            "E01000001": [20.0, 70.0, 30.0, 0.0],
            "E01000002": [5.0, 5.0, 5.0, 10.0],
        }
    ).to_csv(pop, index=False)
    pd.DataFrame(
        {
            "LSOA21CD": ["E01000001", "E01000002"],
            "MSOA21CD": ["E02000001", "E02000001"],
        }
    ).to_csv(lookup, index=False)

    out = build_child_origin_uplift_factors(pop, lookup)

    row = out[out["origin_msoa"] == "E02000001"].iloc[0]
    assert row["adult_population"] == 120.0
    assert row["child_population"] == 25.0
    assert abs(row["child_share"] - (25.0 / 145.0)) < 1e-9
    assert abs(row["child_origin_uplift_factor"] - (145.0 / 120.0)) < 1e-9


def test_apply_child_origin_uplift_preserves_adult_volume_and_adds_all_age_base():
    trips = pd.DataFrame(
        {
            "origin_msoa": ["E02000001", "E02000001", "E02000002"],
            "volume": [10.0, 30.0, 5.0],
        }
    )
    factors = pd.DataFrame(
        {
            "origin_msoa": ["E02000001", "E02000002"],
            "adult_population": [80.0, 100.0],
            "child_population": [20.0, 0.0],
            "child_share": [0.2, 0.0],
            "child_origin_uplift_factor": [1.25, 1.0],
        }
    )

    out_dd, diagnostics = apply_child_origin_uplift(dd.from_pandas(trips, npartitions=1), factors, enabled=True)
    out = out_dd.compute()

    assert out["volume"].tolist() == [10.0, 30.0, 5.0]
    assert out["volume_all_age_base"].tolist() == [12.5, 37.5, 5.0]
    diag = diagnostics.set_index("origin_msoa")
    assert diag.loc["E02000001", "adult_bt_origin_volume"] == 40.0
    assert diag.loc["E02000001", "all_age_base_volume"] == 50.0
    assert diag.loc["E02000001", "child_added_volume"] == 10.0


def test_child_origin_uplift_errors_for_bt_origin_with_zero_adult_population():
    origin_volumes = pd.DataFrame(
        {
            "origin_msoa": ["E02000001"],
            "adult_bt_origin_volume": [10.0],
        }
    )
    factors = pd.DataFrame(
        {
            "origin_msoa": ["E02000001"],
            "adult_population": [0.0],
            "child_population": [10.0],
            "child_share": [1.0],
            "child_origin_uplift_factor": [pd.NA],
        }
    )

    with pytest.raises(ValueError, match="zero adult population"):
        _build_child_origin_uplift_diagnostics(origin_volumes, factors)


def test_build_nts_mode_shares_by_region_maps_modes():
    nts = pd.DataFrame(
        {
            "Year": ["2024", "2024", "2022 to 2023"],
            "Trip length": ["0-1", "0-1", "0-1"],
            "Region of residence": ["East of England", "East of England", "North East"],
            "Walk": [10, 10, 1],
            "Pedal cycle": [0, 0, 0],
            "Car or van driver": [20, 20, 2],
            "Car or van passenger": [5, 5, 1],
            "Motorcycle": [0, 0, 0],
            "Other private transport": [0, 0, 0],
            "Bus in London": [0, 0, 0],
            "Other local bus": [0, 0, 0],
            "Non-local bus": [0, 0, 0],
            "London Underground": [5, 5, 1],
            "Surface Rail": [5, 5, 1],
            "Taxi or minicab": [0, 0, 0],
            "Other public transport": [0, 0, 0],
        }
    )
    shares = build_nts_mode_shares_by_region(nts, year=2024)
    assert set(shares["bt_mode"]) == {"WALKING", "ROAD", "SUBWAY", "RAIL"}
    assert abs(shares.groupby(["origin_region", "distance_band"])["nts_share"].sum().iloc[0] - 1.0) < 1e-9
    assert set(shares["origin_region"]) == {"East of England"}


def test_build_nts_mode_shares_by_region_matches_year_ranges():
    nts = pd.DataFrame(
        {
            "Year": ["2022 to 2023", "2024"],
            "Trip length": ["0-1", "0-1"],
            "Region of residence": ["East of England", "East of England"],
            "Walk": [10, 1],
            "Pedal cycle": [0, 0],
            "Car or van driver": [20, 1],
            "Car or van passenger": [5, 1],
            "Motorcycle": [0, 0],
            "Other private transport": [0, 0],
            "Bus in London": [0, 0],
            "Other local bus": [0, 0],
            "Non-local bus": [0, 0],
            "London Underground": [5, 1],
            "Surface Rail": [5, 1],
            "Taxi or minicab": [0, 0],
            "Other public transport": [0, 0],
        }
    )
    shares = build_nts_mode_shares_by_region(nts, year=2023)
    road_share = shares.loc[shares["bt_mode"] == "ROAD", "nts_share"].iloc[0]
    assert road_share > 0.5


def test_factor_clipping():
    pdf = pd.DataFrame(
        {
            "distance_band": ["0-1", "0-1", "0-1"],
            "mode_of_transport": ["ROAD", "RAIL", "WALKING"],
            "volume": [100, 1, 1],
        }
    )
    nts_band = pd.DataFrame(
        {
            "origin_region": ["East of England", "East of England", "East of England"],
            "distance_band": ["0-1", "0-1", "0-1"],
            "bt_mode": ["ROAD", "RAIL", "WALKING"],
            "nts_share": [0.1, 0.45, 0.45],
        }
    )
    pdf["origin_region"] = "East of England"
    factors = calculate_factors(dd.from_pandas(pdf, npartitions=1), nts_band, 0.01, 100.0)
    assert factors["factor"].between(0.01, 100.0).all()


def test_calculate_factors_can_use_all_age_base_volume():
    pdf = pd.DataFrame(
        {
            "origin_region": ["East of England", "East of England"],
            "distance_band": ["0-1", "0-1"],
            "mode_of_transport": ["ROAD", "WALKING"],
            "volume": [100.0, 100.0],
            "volume_all_age_base": [300.0, 100.0],
        }
    )
    nts_band = pd.DataFrame(
        {
            "origin_region": ["East of England", "East of England"],
            "distance_band": ["0-1", "0-1"],
            "bt_mode": ["ROAD", "WALKING"],
            "nts_share": [0.5, 0.5],
        }
    )

    factors = calculate_factors(
        dd.from_pandas(pdf, npartitions=1),
        nts_band,
        0.01,
        100.0,
        value_col="volume_all_age_base",
    )

    factor_map = dict(zip(factors["bt_mode"], factors["factor"]))
    assert abs(factor_map["ROAD"] - (0.5 / 0.75)) < 1e-9
    assert abs(factor_map["WALKING"] - (0.5 / 0.25)) < 1e-9


def test_calculate_factors_fails_when_no_distance_groups():
    pdf = pd.DataFrame(
        {
            "origin_region": ["East of England"],
            "distance_band": [pd.NA],
            "mode_of_transport": ["ROAD"],
            "volume": [100],
        }
    )
    nts_band = pd.DataFrame(
        {
            "origin_region": ["East of England"],
            "distance_band": ["0-1"],
            "bt_mode": ["ROAD"],
            "nts_share": [1.0],
        }
    )

    with pytest.raises(ValueError, match="No BT trips"):
        calculate_factors(dd.from_pandas(pdf, npartitions=1), nts_band, 0.01, 100.0)


def test_apply_mode_time_constraint_preserves_period_total_and_reduces_am_walk():
    pdf = pd.DataFrame(
        {
            "origin_msoa": ["E02000001", "E02000001"],
            "destination_msoa": ["E02000002", "E02000003"],
            "mode_of_transport": ["WALKING", "ROAD"],
            "time_period": ["AM_peak", "AM_peak"],
            "weekend_flag": [0, 0],
            "volume_adj": [80.0, 20.0],
            "adj_factor": [1.0, 1.0],
        }
    )
    targets = pd.DataFrame(
        {
            "MSOA21CD": ["E02000001", "E02000001"],
            "period_key": ["weekday_AM", "weekday_AM"],
            "mode_time_group": ["WALKING", "ROAD"],
            "nts_mode_time_share": [0.3, 0.7],
        }
    )

    out_dd, _, check = apply_mode_time_constraint(dd.from_pandas(pdf, npartitions=1), targets, split_road_mode=False)
    out = out_dd.compute()

    totals = out.groupby("origin_msoa")["volume_adj"].sum().to_dict()
    assert abs(totals["E02000001"] - 100.0) < 1e-9
    mode_totals = out.groupby("mode_of_transport")["volume_adj"].sum().to_dict()
    assert abs(mode_totals["WALKING"] - 30.0) < 1e-9
    assert abs(mode_totals["ROAD"] - 70.0) < 1e-9
    walk_check = check[check["mode_time_group"] == "WALKING"].iloc[0]
    assert abs(walk_check["bt_mode_time_share_after"] - 0.3) < 1e-9


def test_calculate_mode_time_factors_renormalizes_over_present_modes():
    volumes = pd.DataFrame(
        {
            "origin_msoa": ["E02000001"],
            "period_key": ["weekday_AM"],
            "mode_time_group": ["WALKING"],
            "volume_adj": [50.0],
        }
    )
    targets = pd.DataFrame(
        {
            "MSOA21CD": ["E02000001", "E02000001"],
            "period_key": ["weekday_AM", "weekday_AM"],
            "mode_time_group": ["WALKING", "ROAD"],
            "nts_mode_time_share": [0.3, 0.7],
        }
    )

    factors, check = calculate_mode_time_factors(volumes, targets)

    assert len(factors) == 1
    assert factors.iloc[0]["mode_time_group"] == "WALKING"
    assert abs(factors.iloc[0]["mode_time_factor"] - 1.0) < 1e-9
    assert abs(check.iloc[0]["target_mode_time_share"] - 1.0) < 1e-9


def test_apply_mode_time_constraint_groups_motorcycle_with_private_car():
    pdf = pd.DataFrame(
        {
            "origin_msoa": ["E02000001", "E02000001", "E02000001"],
            "destination_msoa": ["E02000002", "E02000003", "E02000004"],
            "mode_of_transport": ["PRIVATE_CAR", "MOTORCYCLE", "WALKING"],
            "time_period": ["AM_peak", "AM_peak", "AM_peak"],
            "weekend_flag": [0, 0, 0],
            "volume_adj": [60.0, 20.0, 20.0],
            "adj_factor": [1.0, 1.0, 1.0],
        }
    )
    targets = pd.DataFrame(
        {
            "MSOA21CD": ["E02000001", "E02000001"],
            "period_key": ["weekday_AM", "weekday_AM"],
            "mode_time_group": ["PRIVATE_CAR", "WALKING"],
            "nts_mode_time_share": [0.4, 0.6],
        }
    )

    out_dd, factors, _ = apply_mode_time_constraint(dd.from_pandas(pdf, npartitions=1), targets, split_road_mode=True)
    out = out_dd.compute()
    mode_totals = out.groupby("mode_of_transport")["volume_adj"].sum().to_dict()

    assert abs(mode_totals["PRIVATE_CAR"] - 30.0) < 1e-9
    assert abs(mode_totals["MOTORCYCLE"] - 10.0) < 1e-9
    assert abs(mode_totals["WALKING"] - 60.0) < 1e-9
    private_factor = factors[factors["mode_time_group"] == "PRIVATE_CAR"]["mode_time_factor"].iloc[0]
    assert abs(private_factor - 0.5) < 1e-9


def test_build_road_split_shares_by_region():
    nts = pd.DataFrame(
        {
            "Year": ["2024"],
            "Trip length": ["0-1"],
            "Region of residence": ["East of England"],
            "Pedal cycle": [10.0],
            "Car or van driver": [20.0],
            "Car or van passenger": [5.0],
            "Motorcycle": [2.0],
            "Other private transport": [1.0],
            "Bus in London": [3.0],
            "Other local bus": [4.0],
            "Non-local bus": [5.0],
        }
    )
    out = build_road_split_shares_by_region(nts, year=2024)
    assert set(out["road_mode"]) == {"CYCLE", "PRIVATE_CAR", "MOTORCYCLE", "BUS"}
    assert abs(out["road_share"].sum() - 1.0) < 1e-9


def test_build_road_split_shares_by_region_zero_signal_falls_back_to_private_car():
    nts = pd.DataFrame(
        {
            "Year": ["2024"],
            "Trip length": ["0-1"],
            "Region of residence": ["East of England"],
            "Pedal cycle": [0.0],
            "Car or van driver": [0.0],
            "Car or van passenger": [0.0],
            "Motorcycle": [0.0],
            "Other private transport": [0.0],
            "Bus in London": [0.0],
            "Other local bus": [0.0],
            "Non-local bus": [0.0],
        }
    )
    out = build_road_split_shares_by_region(nts, year=2024)
    share_map = dict(zip(out["road_mode"], out["road_share"]))
    assert share_map["PRIVATE_CAR"] == 1.0
    assert share_map["CYCLE"] == 0.0
    assert share_map["MOTORCYCLE"] == 0.0
    assert share_map["BUS"] == 0.0


def test_expand_nts_mode_shares_to_split_road_modes():
    nts_band_mode = pd.DataFrame(
        {
            "origin_region": ["East of England", "East of England", "East of England"],
            "distance_band": ["0-1", "0-1", "0-1"],
            "bt_mode": ["ROAD", "WALKING", "RAIL"],
            "nts_share": [0.6, 0.3, 0.1],
        }
    )
    road_shares = pd.DataFrame(
        {
            "origin_region": ["East of England"] * 4,
            "distance_band": ["0-1"] * 4,
            "road_mode": ["CYCLE", "PRIVATE_CAR", "MOTORCYCLE", "BUS"],
            "road_share": [0.1, 0.5, 0.2, 0.2],
        }
    )
    out = expand_nts_mode_shares_to_split_road_modes(nts_band_mode, road_shares)
    share_map = dict(zip(out["bt_mode"], out["nts_share"]))
    assert abs(share_map["CYCLE"] - 0.06) < 1e-9
    assert abs(share_map["PRIVATE_CAR"] - 0.30) < 1e-9
    assert abs(share_map["MOTORCYCLE"] - 0.12) < 1e-9
    assert abs(share_map["BUS"] - 0.12) < 1e-9
    assert abs(share_map["WALKING"] - 0.3) < 1e-9
    assert abs(share_map["RAIL"] - 0.1) < 1e-9
