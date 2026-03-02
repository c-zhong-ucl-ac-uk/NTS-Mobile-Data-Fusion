import dask.dataframe as dd
import pandas as pd

from uk_travel_pipeline.reassign import add_distance_bands, build_nts_mode_shares_by_region, calculate_factors


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
