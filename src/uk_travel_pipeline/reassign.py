from __future__ import annotations

from pathlib import Path
import re

import dask.dataframe as dd
import geopandas as gpd
import numpy as np
import pandas as pd

from .config import ReassignConfig
from .io import assert_dataframe_columns, assert_dask_columns, assert_files_exist, ensure_dir, ensure_parent, write_csv
from .purpose import _bt_period_key, build_msoa_mode_time_shares, run_purpose_estimation

MILE_IN_M = 1609.344

BT_REQUIRED_COLUMNS = [
    "origin_msoa",
    "destination_msoa",
    "volume",
    "days_used",
    "journey_time_mean",
    "mode_of_transport",
    "time_period",
    "weekend_flag",
]

NTS_MODE_COLS = [
    "Walk",
    "Pedal cycle",
    "Car or van driver",
    "Car or van passenger",
    "Motorcycle",
    "Other private transport",
    "Bus in London",
    "Other local bus",
    "Non-local bus",
    "London Underground",
    "Surface Rail",
    "Taxi or minicab",
    "Other public transport",
]

NTS_ROAD_SPLIT_COLS = [
    "Pedal cycle",
    "Car or van driver",
    "Car or van passenger",
    "Motorcycle",
    "Other private transport",
    "Bus in London",
    "Other local bus",
    "Non-local bus",
]

NTS_TO_BT = {
    "Walk": "WALKING",
    "Pedal cycle": "ROAD",
    "Car or van driver": "ROAD",
    "Car or van passenger": "ROAD",
    "Motorcycle": "ROAD",
    "Other private transport": "ROAD",
    "Bus in London": "ROAD",
    "Other local bus": "ROAD",
    "Non-local bus": "ROAD",
    "Taxi or minicab": "ROAD",
    "London Underground": "SUBWAY",
    "Surface Rail": "RAIL",
    "Other public transport": "RAIL",
}

NTS9916A_SHEET = "NTS9916a_trips_region"
REGION_COLUMN_CANDIDATES = [
    "Region of residence",
    "RGN21NM",
    "RGN22NM",
    "RGN11NM",
    "region",
    "region_name",
]

REGION_NAME_NORMALIZATION = {
    "east": "East of England",
    "east of england": "East of England",
    "north east": "North East",
    "north west": "North West",
    "yorkshire and the humber": "Yorkshire and The Humber",
    "east midlands": "East Midlands",
    "west midlands": "West Midlands",
    "south east": "South East",
    "south west": "South West",
    "london": "London",
    "wales": "Wales",
}


def _split_road_modes_partition(pdf: pd.DataFrame, road_shares: pd.DataFrame) -> pd.DataFrame:
    if pdf.empty:
        return pdf

    road = pdf[pdf["mode_of_transport"] == "ROAD"].copy()
    if road.empty:
        return pdf

    non_road = pdf[pdf["mode_of_transport"] != "ROAD"].copy()
    road_split = road.merge(road_shares, on=["origin_region", "distance_band"], how="left")
    road_split["road_mode"] = road_split["road_mode"].fillna("PRIVATE_CAR")
    road_split["road_share"] = pd.to_numeric(road_split["road_share"], errors="coerce").fillna(1.0)
    road_split = road_split[road_split["road_share"] > 0].copy()
    road_split["mode_of_transport"] = road_split["road_mode"]
    road_split["volume"] = pd.to_numeric(road_split["volume"], errors="coerce").fillna(0.0) * road_split["road_share"]
    if "volume_all_age_base" in road_split.columns:
        road_split["volume_all_age_base"] = (
            pd.to_numeric(road_split["volume_all_age_base"], errors="coerce").fillna(0.0)
            * road_split["road_share"]
        )
    road_split["volume_adj"] = (
        pd.to_numeric(road_split["volume_adj"], errors="coerce").fillna(0.0) * road_split["road_share"]
    )
    road_split = road_split.drop(columns=["road_mode", "road_share"])
    return pd.concat([non_road, road_split], ignore_index=True)


def _add_distances(pdf: pd.DataFrame) -> pd.DataFrame:
    dx = pdf["ox"].to_numpy() - pdf["dx"].to_numpy()
    dy = pdf["oy"].to_numpy() - pdf["dy"].to_numpy()
    pdf["distance_m"] = np.sqrt(dx * dx + dy * dy).astype("float32")
    pdf["distance_miles"] = (pdf["distance_m"] / MILE_IN_M).astype("float32")
    return pdf


def _build_msoa_centroids(msoa: gpd.GeoDataFrame) -> pd.DataFrame:
    cent = pd.DataFrame(
        {
            "MSOA21CD": msoa["MSOA21CD"].astype(str).values,
            "origin_region": msoa["origin_region"].astype(str).values,
        }
    )

    if {"BNG_E", "BNG_N"}.issubset(msoa.columns):
        cent["x"] = pd.to_numeric(msoa["BNG_E"], errors="coerce").astype("float32")
        cent["y"] = pd.to_numeric(msoa["BNG_N"], errors="coerce").astype("float32")
    else:
        cent["x"] = np.nan
        cent["y"] = np.nan

    missing = cent["x"].isna() | cent["y"].isna()
    if missing.any():
        geom_centroids = msoa.loc[missing, "geometry"].centroid
        cent.loc[missing, "x"] = geom_centroids.x.values.astype("float32")
        cent.loc[missing, "y"] = geom_centroids.y.values.astype("float32")

    missing_after = cent["x"].isna() | cent["y"].isna()
    if missing_after.any():
        examples = cent.loc[missing_after, "MSOA21CD"].head(5).tolist()
        raise ValueError(
            "Could not derive valid MSOA centroid coordinates. "
            "Provide BNG_E/BNG_N columns or valid projected geometries. "
            f"First missing MSOAs: {examples}"
        )

    return cent


def add_distance_bands(trips_dd: dd.DataFrame, labels: list[str]) -> dd.DataFrame:
    bins = [0, 1, 2, 5, 10, 25, 50, 100, np.inf]

    def _add_band(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf["distance_band"] = pd.cut(
            pdf["distance_miles"],
            bins=bins,
            labels=labels,
            right=False,
            include_lowest=True,
        )
        return pdf

    meta = trips_dd._meta.assign(
        distance_band=pd.Categorical(pd.Series([], dtype="object"), categories=labels, ordered=True)
    )
    return trips_dd.map_partitions(_add_band, meta=meta)


def _clean_nts_column_name(col: str) -> str:
    clean = re.sub(r"\s*\[note [^\]]+\]", "", str(col), flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", clean).strip()


def _lsoa_population_columns(columns: pd.Index) -> list[str]:
    return [c for c in columns if re.match(r"^[A-Z]010\d{5}$", str(c))]


def build_child_origin_uplift_factors(pop_lsoa_internal_csv: Path, lsoa_msoa_lookup_csv: Path) -> pd.DataFrame:
    columns = pd.read_csv(pop_lsoa_internal_csv, nrows=0).columns
    required = ["gender_3", "aws"]
    assert_dataframe_columns(pd.DataFrame(columns=columns), required, "Population file")
    lsoa_cols = _lsoa_population_columns(columns)
    if not lsoa_cols:
        raise ValueError(f"Population file {pop_lsoa_internal_csv} does not contain LSOA21CD population columns.")

    pop = pd.read_csv(pop_lsoa_internal_csv, usecols=[*required, *lsoa_cols])
    pop["gender_3"] = pd.to_numeric(pop["gender_3"], errors="coerce")
    pop["aws"] = pd.to_numeric(pop["aws"], errors="coerce")
    gender_child = pop["gender_3"] == 1
    aws_child = pop["aws"] == 1
    mismatch = gender_child != aws_child
    if mismatch.any():
        examples = pop.loc[mismatch, ["gender_3", "aws"]].drop_duplicates().head(5).to_dict("records")
        raise ValueError(
            "Population child markers are inconsistent; expected `gender_3 == 1` and `aws == 1` "
            f"to identify the same rows. First mismatches: {examples}"
        )

    pop[lsoa_cols] = pop[lsoa_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    child_pop = pop.loc[gender_child, lsoa_cols].sum(axis=0)
    adult_pop = pop.loc[~gender_child, lsoa_cols].sum(axis=0)
    pop_lsoa = pd.DataFrame(
        {
            "LSOA21CD": lsoa_cols,
            "adult_population": adult_pop.to_numpy(dtype="float64"),
            "child_population": child_pop.to_numpy(dtype="float64"),
        }
    )

    lsoa_msoa = pd.read_csv(lsoa_msoa_lookup_csv, usecols=["LSOA21CD", "MSOA21CD"]).drop_duplicates()
    lsoa_msoa["LSOA21CD"] = lsoa_msoa["LSOA21CD"].astype("string")
    lsoa_msoa["MSOA21CD"] = lsoa_msoa["MSOA21CD"].astype("string")
    pop_msoa = (
        pop_lsoa.merge(lsoa_msoa, on="LSOA21CD", how="left")
        .dropna(subset=["MSOA21CD"])
        .groupby("MSOA21CD", as_index=False)[["adult_population", "child_population"]]
        .sum()
        .rename(columns={"MSOA21CD": "origin_msoa"})
    )
    total_population = pop_msoa["adult_population"] + pop_msoa["child_population"]
    pop_msoa["child_share"] = np.where(total_population > 0, pop_msoa["child_population"] / total_population, 0.0)
    pop_msoa["child_origin_uplift_factor"] = np.where(
        pop_msoa["adult_population"] > 0,
        total_population / pop_msoa["adult_population"],
        np.nan,
    )
    pop_msoa["origin_msoa"] = pop_msoa["origin_msoa"].astype("string")
    return pop_msoa[
        [
            "origin_msoa",
            "adult_population",
            "child_population",
            "child_share",
            "child_origin_uplift_factor",
        ]
    ]


def _build_child_origin_uplift_diagnostics(
    origin_volumes: pd.DataFrame,
    uplift_factors: pd.DataFrame,
) -> pd.DataFrame:
    required_origin = ["origin_msoa", "adult_bt_origin_volume"]
    required_factors = [
        "origin_msoa",
        "adult_population",
        "child_population",
        "child_share",
        "child_origin_uplift_factor",
    ]
    assert_dataframe_columns(origin_volumes, required_origin, "BT origin volume table")
    assert_dataframe_columns(uplift_factors, required_factors, "Child origin uplift factors")

    diagnostics = origin_volumes.copy()
    diagnostics["origin_msoa"] = diagnostics["origin_msoa"].astype("string")
    diagnostics["adult_bt_origin_volume"] = pd.to_numeric(
        diagnostics["adult_bt_origin_volume"], errors="coerce"
    ).fillna(0.0)
    factors = uplift_factors.copy()
    factors["origin_msoa"] = factors["origin_msoa"].astype("string")
    diagnostics = diagnostics.merge(factors, on="origin_msoa", how="left")

    bad = diagnostics["child_origin_uplift_factor"].isna() | (diagnostics["adult_population"] <= 0)
    bad = bad & (diagnostics["adult_bt_origin_volume"] > 0)
    if bad.any():
        examples = diagnostics.loc[
            bad, ["origin_msoa", "adult_population", "child_population", "adult_bt_origin_volume"]
        ].head(5).to_dict("records")
        raise ValueError(
            "Cannot apply child origin uplift because BT origins with trips are missing adult population "
            f"or have zero adult population. First examples: {examples}"
        )

    diagnostics["child_origin_uplift_factor"] = pd.to_numeric(
        diagnostics["child_origin_uplift_factor"], errors="coerce"
    ).fillna(1.0)
    diagnostics["all_age_base_volume"] = (
        diagnostics["adult_bt_origin_volume"] * diagnostics["child_origin_uplift_factor"]
    )
    diagnostics["child_added_volume"] = diagnostics["all_age_base_volume"] - diagnostics["adult_bt_origin_volume"]
    return diagnostics[
        [
            "origin_msoa",
            "adult_population",
            "child_population",
            "child_share",
            "child_origin_uplift_factor",
            "adult_bt_origin_volume",
            "all_age_base_volume",
            "child_added_volume",
        ]
    ]


def _build_disabled_child_origin_uplift_diagnostics(origin_volumes: pd.DataFrame) -> pd.DataFrame:
    diagnostics = origin_volumes.copy()
    diagnostics["origin_msoa"] = diagnostics["origin_msoa"].astype("string")
    diagnostics["adult_bt_origin_volume"] = pd.to_numeric(
        diagnostics["adult_bt_origin_volume"], errors="coerce"
    ).fillna(0.0)
    diagnostics["adult_population"] = np.nan
    diagnostics["child_population"] = np.nan
    diagnostics["child_share"] = 0.0
    diagnostics["child_origin_uplift_factor"] = 1.0
    diagnostics["all_age_base_volume"] = diagnostics["adult_bt_origin_volume"]
    diagnostics["child_added_volume"] = 0.0
    return diagnostics[
        [
            "origin_msoa",
            "adult_population",
            "child_population",
            "child_share",
            "child_origin_uplift_factor",
            "adult_bt_origin_volume",
            "all_age_base_volume",
            "child_added_volume",
        ]
    ]


def apply_child_origin_uplift(
    trips_dd: dd.DataFrame,
    uplift_factors: pd.DataFrame | None,
    enabled: bool,
) -> tuple[dd.DataFrame, pd.DataFrame]:
    trips_dd["origin_msoa"] = trips_dd["origin_msoa"].astype("string")
    trips_dd["volume"] = dd.to_numeric(trips_dd["volume"], errors="coerce").fillna(0.0)
    origin_volumes = (
        trips_dd.groupby("origin_msoa")["volume"]
        .sum()
        .compute()
        .reset_index()
        .rename(columns={"volume": "adult_bt_origin_volume"})
    )

    if not enabled:
        trips_dd["child_origin_uplift_factor"] = 1.0
        trips_dd["volume_all_age_base"] = trips_dd["volume"].astype("float64")
        return trips_dd, _build_disabled_child_origin_uplift_diagnostics(origin_volumes)

    if uplift_factors is None:
        raise ValueError("Child origin uplift factors are required when child origin uplift is enabled.")

    diagnostics = _build_child_origin_uplift_diagnostics(origin_volumes, uplift_factors)
    merge_factors = diagnostics[["origin_msoa", "child_origin_uplift_factor"]].copy()
    merge_factors["origin_msoa"] = merge_factors["origin_msoa"].astype("string")
    trips_dd = trips_dd.merge(merge_factors, on="origin_msoa", how="left")
    trips_dd["child_origin_uplift_factor"] = dd.to_numeric(
        trips_dd["child_origin_uplift_factor"], errors="coerce"
    ).fillna(1.0)
    trips_dd["volume_all_age_base"] = trips_dd["volume"].astype("float64") * trips_dd[
        "child_origin_uplift_factor"
    ]
    return trips_dd, diagnostics


def normalize_region_name(value: object) -> str | None:
    if pd.isna(value):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    key = re.sub(r"\s+", " ", raw).strip().lower()
    return REGION_NAME_NORMALIZATION.get(key, raw)


def _extract_nts_table_from_ods(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=NTS9916A_SHEET, engine="odf", header=None)
    header_idx = None
    for i in range(len(raw)):
        c0 = _clean_nts_column_name(raw.iloc[i, 0])
        c1 = _clean_nts_column_name(raw.iloc[i, 1]) if raw.shape[1] > 1 else ""
        c2 = _clean_nts_column_name(raw.iloc[i, 2]) if raw.shape[1] > 2 else ""
        if c0 == "Year" and c1 == "Trip length" and c2 == "Region of residence":
            header_idx = i
            break

    if header_idx is None:
        raise ValueError(f"Could not locate header row in ODS sheet '{NTS9916A_SHEET}'.")

    header = [_clean_nts_column_name(x) for x in raw.iloc[header_idx].tolist()]
    nts = raw.iloc[header_idx + 1 :].copy()
    nts.columns = header
    nts = nts.dropna(how="all")
    nts = nts.loc[:, ~nts.columns.str.contains("^Unnamed", na=False)]
    return nts


def load_nts_trips_region(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".ods":
        nts = _extract_nts_table_from_ods(path)
    else:
        nts = pd.read_csv(path)
        nts.columns = [_clean_nts_column_name(c) for c in nts.columns]
    return nts


def _year_label_matches(year_label: object, target_year: int) -> bool:
    if pd.isna(year_label):
        return False
    text = str(year_label).strip()
    if not text:
        return False
    values = [int(x) for x in re.findall(r"\d{4}", text)]
    if not values:
        return False
    if len(values) == 1:
        return values[0] == target_year
    return min(values) <= target_year <= max(values)


def build_nts_mode_shares_by_region(nts_df: pd.DataFrame, year: int) -> pd.DataFrame:
    required = ["Year", "Trip length", "Region of residence"] + NTS_MODE_COLS
    assert_dataframe_columns(nts_df, required, "NTS file")

    nts = nts_df.copy()
    nts["Year"] = nts["Year"].astype(str).str.strip()
    nts = nts[nts["Year"].map(lambda x: _year_label_matches(x, year))].copy()
    nts = nts[nts["Trip length"].astype(str).str.strip().ne("All lengths")].copy()
    nts["Region of residence"] = nts["Region of residence"].map(normalize_region_name)
    nts = nts.dropna(subset=["Region of residence"])

    for col in NTS_MODE_COLS:
        nts[col] = pd.to_numeric(nts[col], errors="coerce")

    nts_long = nts.melt(
        id_vars=["Year", "Trip length", "Region of residence"],
        value_vars=NTS_MODE_COLS,
        var_name="nts_mode",
        value_name="nts_trips",
    )
    nts_long["bt_mode"] = nts_long["nts_mode"].map(NTS_TO_BT)
    nts_long = nts_long.dropna(subset=["bt_mode"]).copy()

    nts_bt = nts_long.groupby(["Region of residence", "Trip length", "bt_mode"], as_index=False)["nts_trips"].sum()
    nts_bt = nts_bt.rename(columns={"Region of residence": "origin_region", "Trip length": "distance_band"})
    nts_bt["nts_share"] = nts_bt.groupby(["origin_region", "distance_band"])["nts_trips"].transform(
        lambda s: s / s.sum()
    )
    return nts_bt[["origin_region", "distance_band", "bt_mode", "nts_share"]]


def build_road_split_shares_by_region(nts_df: pd.DataFrame, year: int) -> pd.DataFrame:
    required = ["Year", "Trip length", "Region of residence"] + NTS_ROAD_SPLIT_COLS
    assert_dataframe_columns(nts_df, required, "NTS file")
    nts = nts_df.copy()
    nts["Year"] = nts["Year"].astype(str).str.strip()
    nts = nts[nts["Year"].map(lambda x: _year_label_matches(x, year))].copy()
    nts = nts[nts["Trip length"].astype(str).str.strip().ne("All lengths")].copy()
    nts["Region of residence"] = nts["Region of residence"].map(normalize_region_name)
    nts = nts.dropna(subset=["Region of residence"])

    for col in NTS_ROAD_SPLIT_COLS:
        nts[col] = pd.to_numeric(nts[col], errors="coerce").fillna(0.0)

    nts["CYCLE"] = nts["Pedal cycle"]
    nts["PRIVATE_CAR"] = (
        nts["Car or van driver"] + nts["Car or van passenger"] + nts["Other private transport"]
    )
    nts["MOTORCYCLE"] = nts["Motorcycle"]
    nts["BUS"] = nts["Bus in London"] + nts["Other local bus"] + nts["Non-local bus"]

    road = nts[["Region of residence", "Trip length", "CYCLE", "PRIVATE_CAR", "MOTORCYCLE", "BUS"]].rename(
        columns={"Region of residence": "origin_region", "Trip length": "distance_band"}
    )
    long = road.melt(
        id_vars=["origin_region", "distance_band"],
        value_vars=["CYCLE", "PRIVATE_CAR", "MOTORCYCLE", "BUS"],
        var_name="road_mode",
        value_name="road_volume",
    )
    out = long.groupby(["origin_region", "distance_band", "road_mode"], as_index=False)["road_volume"].sum()
    out["group_total"] = out.groupby(["origin_region", "distance_band"])["road_volume"].transform("sum")
    out["road_share"] = out["road_volume"] / out["group_total"].replace(0, np.nan)
    out["road_share"] = out["road_share"].fillna(0.0)
    # Fallback when no road split signal exists: keep all ROAD in PRIVATE_CAR.
    zero_mask = out["group_total"] == 0
    out.loc[zero_mask, "road_share"] = 0.0
    out.loc[zero_mask & (out["road_mode"] == "PRIVATE_CAR"), "road_share"] = 1.0
    out = out.drop(columns=["group_total"])
    return out[["origin_region", "distance_band", "road_mode", "road_share"]]


def expand_nts_mode_shares_to_split_road_modes(
    nts_band_mode: pd.DataFrame, road_shares: pd.DataFrame
) -> pd.DataFrame:
    non_road = nts_band_mode[nts_band_mode["bt_mode"] != "ROAD"].copy()
    road = nts_band_mode[nts_band_mode["bt_mode"] == "ROAD"].copy()
    if road.empty:
        return nts_band_mode.copy()

    road_expanded = road.merge(road_shares, on=["origin_region", "distance_band"], how="left")
    road_expanded["road_mode"] = road_expanded["road_mode"].fillna("PRIVATE_CAR")
    road_expanded["road_share"] = pd.to_numeric(road_expanded["road_share"], errors="coerce").fillna(0.0)
    road_expanded["nts_share"] = (
        pd.to_numeric(road_expanded["nts_share"], errors="coerce").fillna(0.0) * road_expanded["road_share"]
    )
    road_expanded["bt_mode"] = road_expanded["road_mode"]
    road_expanded = road_expanded[["origin_region", "distance_band", "bt_mode", "nts_share"]]

    combined = pd.concat([non_road, road_expanded], ignore_index=True)
    return combined.groupby(["origin_region", "distance_band", "bt_mode"], as_index=False)["nts_share"].sum()


def calculate_factors(
    trips_dd: dd.DataFrame,
    nts_band_mode: pd.DataFrame,
    factor_min: float,
    factor_max: float,
    value_col: str = "volume",
) -> pd.DataFrame:
    assert_dask_columns(trips_dd, [value_col], "BT trips")
    bt_band_mode = (
        trips_dd.groupby(["origin_region", "distance_band", "mode_of_transport"])[value_col]
        .sum()
        .compute()
        .reset_index()
        .rename(columns={"mode_of_transport": "bt_mode", value_col: "bt_volume"})
    )
    if bt_band_mode.empty:
        raise ValueError(
            "No BT trips could be grouped by origin region, distance band, and mode. "
            "Check MSOA region lookup coverage and centroid/distance inputs."
        )

    bt_band_mode["bt_share"] = bt_band_mode.groupby(["origin_region", "distance_band"])["bt_volume"].transform(
        lambda s: s / s.sum()
    )

    factors = bt_band_mode.merge(nts_band_mode, on=["origin_region", "distance_band", "bt_mode"], how="left")
    factors["nts_share"] = factors["nts_share"].fillna(factors["bt_share"])
    eps = 1e-12
    factors["factor"] = factors["nts_share"] / (factors["bt_share"] + eps)
    factors["factor"] = factors["factor"].clip(factor_min, factor_max)
    return factors[["origin_region", "distance_band", "bt_mode", "factor"]]


def _add_mode_time_columns(pdf: pd.DataFrame, split_road_mode: bool) -> pd.DataFrame:
    out = _bt_period_key(pdf)
    out["mode_time_group"] = out["mode_of_transport"].astype("string")
    if split_road_mode:
        out.loc[out["mode_time_group"] == "MOTORCYCLE", "mode_time_group"] = "PRIVATE_CAR"
    out["mode_time_group"] = out["mode_time_group"].astype("string")
    return out


def calculate_mode_time_factors(
    mode_time_volumes: pd.DataFrame, mode_time_targets: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    required_volumes = ["origin_msoa", "period_key", "mode_time_group", "volume_adj"]
    required_targets = ["MSOA21CD", "period_key", "mode_time_group", "nts_mode_time_share"]
    assert_dataframe_columns(mode_time_volumes, required_volumes, "Mode/time volume table")
    assert_dataframe_columns(mode_time_targets, required_targets, "NTS mode/time target table")

    volumes = mode_time_volumes.copy()
    volumes["volume_adj"] = pd.to_numeric(volumes["volume_adj"], errors="coerce").fillna(0.0)
    volumes["origin_msoa"] = volumes["origin_msoa"].astype("string")
    volumes["period_key"] = volumes["period_key"].astype("string")
    volumes["mode_time_group"] = volumes["mode_time_group"].astype("string")
    volumes["period_volume_before"] = volumes.groupby(["origin_msoa", "period_key"])["volume_adj"].transform("sum")
    volumes["bt_mode_time_share_before"] = np.where(
        volumes["period_volume_before"] > 0,
        volumes["volume_adj"] / volumes["period_volume_before"],
        0.0,
    )

    targets = mode_time_targets.rename(columns={"MSOA21CD": "origin_msoa"}).copy()
    targets["origin_msoa"] = targets["origin_msoa"].astype("string")
    targets["period_key"] = targets["period_key"].astype("string")
    targets["mode_time_group"] = targets["mode_time_group"].astype("string")
    targets["nts_mode_time_share"] = pd.to_numeric(targets["nts_mode_time_share"], errors="coerce")

    factors = volumes.merge(targets, on=["origin_msoa", "period_key", "mode_time_group"], how="left")
    factors["nts_mode_time_share"] = factors["nts_mode_time_share"].fillna(
        factors["bt_mode_time_share_before"]
    )

    positive = factors["volume_adj"] > 0
    factors["target_present_total"] = 0.0
    factors.loc[positive, "target_present_total"] = factors.loc[positive].groupby(
        ["origin_msoa", "period_key"]
    )["nts_mode_time_share"].transform("sum")
    factors["target_mode_time_share"] = factors["bt_mode_time_share_before"]
    targetable = positive & (factors["target_present_total"] > 0)
    factors.loc[targetable, "target_mode_time_share"] = (
        factors.loc[targetable, "nts_mode_time_share"] / factors.loc[targetable, "target_present_total"]
    )
    factors["volume_adj_after"] = factors["period_volume_before"] * factors["target_mode_time_share"]
    factors.loc[~positive, "volume_adj_after"] = factors.loc[~positive, "volume_adj"]
    factors["mode_time_factor"] = np.where(
        factors["volume_adj"] > 0,
        factors["volume_adj_after"] / factors["volume_adj"],
        1.0,
    )
    factors["period_volume_after"] = factors.groupby(["origin_msoa", "period_key"])["volume_adj_after"].transform("sum")
    factors["bt_mode_time_share_after"] = np.where(
        factors["period_volume_after"] > 0,
        factors["volume_adj_after"] / factors["period_volume_after"],
        0.0,
    )

    factor_cols = ["origin_msoa", "period_key", "mode_time_group", "mode_time_factor"]
    check_cols = [
        "origin_msoa",
        "period_key",
        "mode_time_group",
        "volume_adj",
        "volume_adj_after",
        "period_volume_before",
        "period_volume_after",
        "bt_mode_time_share_before",
        "bt_mode_time_share_after",
        "nts_mode_time_share",
        "target_mode_time_share",
        "mode_time_factor",
    ]
    return factors[factor_cols], factors[check_cols]


def apply_mode_time_constraint(
    trips_dd: dd.DataFrame,
    mode_time_targets: pd.DataFrame,
    split_road_mode: bool,
) -> tuple[dd.DataFrame, pd.DataFrame, pd.DataFrame]:
    meta = trips_dd._meta.assign(
        period_key=pd.Series([], dtype="string"),
        mode_time_group=pd.Series([], dtype="string"),
    )
    trips_dd = trips_dd.map_partitions(
        _add_mode_time_columns,
        split_road_mode=split_road_mode,
        meta=meta,
    )
    trips_dd["origin_msoa"] = trips_dd["origin_msoa"].astype("string")
    trips_dd["period_key"] = trips_dd["period_key"].astype("string")
    trips_dd["mode_time_group"] = trips_dd["mode_time_group"].astype("string")
    trips_dd["volume_adj"] = dd.to_numeric(trips_dd["volume_adj"], errors="coerce").fillna(0.0)

    mode_time_volumes = (
        trips_dd.groupby(["origin_msoa", "period_key", "mode_time_group"])["volume_adj"]
        .sum()
        .compute()
        .reset_index()
    )
    mode_time_factors, mode_time_check = calculate_mode_time_factors(mode_time_volumes, mode_time_targets)
    trips_dd = trips_dd.merge(mode_time_factors, on=["origin_msoa", "period_key", "mode_time_group"], how="left")
    trips_dd["mode_time_factor"] = dd.to_numeric(trips_dd["mode_time_factor"], errors="coerce").fillna(1.0)
    trips_dd["volume_adj"] = trips_dd["volume_adj"] * trips_dd["mode_time_factor"]
    if "adj_factor" in trips_dd.columns:
        trips_dd["adj_factor"] = dd.to_numeric(trips_dd["adj_factor"], errors="coerce").fillna(1.0) * trips_dd[
            "mode_time_factor"
        ]
    trips_dd = trips_dd.drop(columns=["mode_time_factor"])
    return trips_dd, mode_time_factors, mode_time_check


def _drop_existing_columns(trips_dd: dd.DataFrame, columns: list[str]) -> dd.DataFrame:
    existing = [col for col in columns if col in trips_dd.columns]
    if not existing:
        return trips_dd
    return trips_dd.drop(columns=existing)


def run_reassign(config: ReassignConfig, legacy_output_root: Path | None = None) -> Path:
    required_files = [config.bt_parquet, config.msoa_geojson, config.nts_file]
    if config.msoa_filter_csv is not None:
        required_files.append(config.msoa_filter_csv)
    if config.msoa_region_lookup_csv is not None:
        required_files.append(config.msoa_region_lookup_csv)
    if config.apply_child_origin_uplift:
        required_files.extend([config.pop_lsoa_internal_csv, config.lsoa_msoa_lookup_csv])
    if config.constrain_mode_time_share:
        required_files.extend(
            [
                config.pop_lsoa_internal_csv,
                config.tfn_area_type_lsoa_csv,
                config.lsoa_msoa_lookup_csv,
                config.nts_mode_time_split_csv,
            ]
        )
    assert_files_exist(required_files)

    all_trips_dd = dd.read_parquet(config.bt_parquet, columns=BT_REQUIRED_COLUMNS, split_row_groups=True)
    assert_dask_columns(all_trips_dd, BT_REQUIRED_COLUMNS, "BT parquet")

    if config.msoa_filter_csv is not None:
        filter_list = pd.read_csv(config.msoa_filter_csv)
        assert_dataframe_columns(filter_list, ["MSOA21CD"], "MSOA filter list csv")
        filter_set = set(filter_list["MSOA21CD"].astype(str))
        trips_dd = all_trips_dd[
            all_trips_dd["origin_msoa"].isin(filter_set) & all_trips_dd["destination_msoa"].isin(filter_set)
        ]
    else:
        filter_set = None
        trips_dd = all_trips_dd

    msoa = gpd.read_file(config.msoa_geojson).to_crs(epsg=27700)
    assert_dataframe_columns(msoa, ["MSOA21CD", "geometry"], "MSOA geojson")
    if filter_set is not None:
        msoa = msoa[msoa["MSOA21CD"].astype(str).isin(filter_set)].copy()

    region_col = None
    for candidate in REGION_COLUMN_CANDIDATES:
        if candidate in msoa.columns:
            region_col = candidate
            break

    if region_col is None and config.msoa_region_lookup_csv is not None:
        region_lookup = pd.read_csv(config.msoa_region_lookup_csv)
        assert_dataframe_columns(region_lookup, ["MSOA21CD"], "MSOA region lookup csv")
        lookup_region_col = None
        for candidate in REGION_COLUMN_CANDIDATES:
            if candidate in region_lookup.columns:
                lookup_region_col = candidate
                break
        if lookup_region_col is None:
            raise ValueError(
                "MSOA region lookup csv must include a region column, e.g. 'Region of residence' or 'RGN21NM'."
            )
        msoa = msoa.merge(
            region_lookup[["MSOA21CD", lookup_region_col]].rename(columns={lookup_region_col: "origin_region"}),
            on="MSOA21CD",
            how="left",
        )
        region_col = "origin_region"

    if region_col is None and config.region is None:
        raise ValueError(
            "Could not resolve MSOA region from geojson. Provide --msoa-region-lookup-path or --region fallback."
        )

    if region_col is None and config.region is not None:
        msoa["origin_region"] = config.region
        region_col = "origin_region"

    if region_col != "origin_region":
        msoa["origin_region"] = msoa[region_col]
    msoa["origin_region"] = msoa["origin_region"].map(normalize_region_name)

    cent = _build_msoa_centroids(msoa)

    cent_o = cent.rename(columns={"MSOA21CD": "origin_msoa", "x": "ox", "y": "oy"})
    cent_d = cent[["MSOA21CD", "x", "y"]].rename(columns={"MSOA21CD": "destination_msoa", "x": "dx", "y": "dy"})
    trips_dd = trips_dd.merge(cent_o, on="origin_msoa", how="left")
    trips_dd = trips_dd.merge(cent_d, on="destination_msoa", how="left")

    meta = trips_dd._meta.assign(distance_m=np.float32(), distance_miles=np.float32())
    trips_dd = trips_dd.map_partitions(_add_distances, meta=meta)
    trips_dd = trips_dd.drop(columns=["ox", "oy", "dx", "dy"])
    if config.region is not None:
        trips_dd["origin_region"] = trips_dd["origin_region"].fillna(config.region)

    trips_dd["volume"] = dd.to_numeric(trips_dd["volume"], errors="coerce").fillna(5).astype("int32")
    uplift_factors = None
    if config.apply_child_origin_uplift:
        uplift_factors = build_child_origin_uplift_factors(
            config.pop_lsoa_internal_csv,
            config.lsoa_msoa_lookup_csv,
        )
    trips_dd, child_uplift_diagnostics = apply_child_origin_uplift(
        trips_dd,
        uplift_factors,
        enabled=config.apply_child_origin_uplift,
    )

    nts_df = load_nts_trips_region(config.nts_file)
    labels = [x for x in nts_df["Trip length"].dropna().astype(str).str.strip().unique().tolist() if x != "All lengths"]
    trips_dd = add_distance_bands(trips_dd, labels)
    trips_dd["distance_band"] = trips_dd["distance_band"].astype("string")

    nts_band_mode = build_nts_mode_shares_by_region(nts_df, year=config.year)
    nts_band_mode["distance_band"] = nts_band_mode["distance_band"].astype("string")
    factors = calculate_factors(
        trips_dd,
        nts_band_mode,
        config.factor_min,
        config.factor_max,
        value_col="volume_all_age_base",
    )

    nts_band_mode_for_check = nts_band_mode
    trips_dd = trips_dd.merge(
        factors.rename(columns={"bt_mode": "mode_of_transport"}),
        on=["origin_region", "distance_band", "mode_of_transport"],
        how="left",
    )
    trips_dd["factor"] = trips_dd["factor"].fillna(1.0)
    trips_dd["volume_adj"] = trips_dd["volume_all_age_base"] * trips_dd["factor"]
    trips_dd["adj_factor"] = trips_dd["child_origin_uplift_factor"] * trips_dd["factor"]
    trips_dd = trips_dd.drop(columns=["factor"])

    if config.split_road_mode:
        # Increase partition count before row expansion to reduce peak per-partition memory.
        target_parts = max(trips_dd.npartitions, 48)
        trips_dd = trips_dd.repartition(npartitions=target_parts)
        road_shares = build_road_split_shares_by_region(nts_df, year=config.year)
        road_shares["origin_region"] = road_shares["origin_region"].astype("string")
        road_shares["distance_band"] = road_shares["distance_band"].astype("string")
        nts_band_mode_for_check = expand_nts_mode_shares_to_split_road_modes(nts_band_mode, road_shares)
        nts_band_mode_for_check["origin_region"] = nts_band_mode_for_check["origin_region"].astype("string")
        nts_band_mode_for_check["distance_band"] = nts_band_mode_for_check["distance_band"].astype("string")
        road_total_before = (
            dd.to_numeric(trips_dd[trips_dd["mode_of_transport"] == "ROAD"]["volume_adj"], errors="coerce")
            .fillna(0.0)
            .sum()
            .compute()
        )
        split_meta = trips_dd._meta.copy()
        split_meta["volume"] = split_meta["volume"].astype("float64")
        split_meta["volume_adj"] = split_meta["volume_adj"].astype("float64")
        trips_dd = trips_dd.map_partitions(_split_road_modes_partition, road_shares=road_shares, meta=split_meta)
        road_total_after = (
            dd.to_numeric(
                trips_dd[trips_dd["mode_of_transport"].isin(["CYCLE", "PRIVATE_CAR", "MOTORCYCLE", "BUS"])]["volume_adj"],
                errors="coerce",
            )
            .fillna(0.0)
            .sum()
            .compute()
        )
        if not np.isclose(road_total_after, road_total_before, rtol=1e-10, atol=1e-6):
            raise ValueError(
                "ROAD split conservation check failed: sum(split ROAD volume_adj) != pre-split ROAD volume_adj."
            )

    mode_time_factors = None
    mode_time_check = None
    if config.constrain_mode_time_share:
        mode_time_targets = build_msoa_mode_time_shares(config)
        trips_dd, mode_time_factors, mode_time_check = apply_mode_time_constraint(
            trips_dd,
            mode_time_targets,
            split_road_mode=config.split_road_mode,
        )

    check = (
        trips_dd.groupby(["origin_region", "distance_band", "mode_of_transport"])[
            ["volume", "volume_all_age_base", "volume_adj"]
        ]
        .sum()
        .compute()
        .reset_index()
    )
    check["base_share_before"] = check.groupby(["origin_region", "distance_band"])["volume_all_age_base"].transform(
        lambda s: s / s.sum()
    )
    check["bt_share_after"] = check.groupby(["origin_region", "distance_band"])["volume_adj"].transform(
        lambda s: s / s.sum()
    )
    check = check.rename(columns={"mode_of_transport": "bt_mode"})
    check = check.merge(nts_band_mode_for_check, on=["origin_region", "distance_band", "bt_mode"], how="left")
    share_check = check.rename(columns={"volume": "volume_bt_adult"})[
        [
            "origin_region",
            "distance_band",
            "bt_mode",
            "volume_bt_adult",
            "volume_all_age_base",
            "volume_adj",
            "base_share_before",
            "bt_share_after",
            "nts_share",
        ]
    ].sort_values(["origin_region", "distance_band", "bt_mode"])

    reassign_out = config.outputs_root / "reassign"
    ensure_dir(reassign_out)
    write_csv(factors.sort_values(["origin_region", "distance_band", "bt_mode"]), reassign_out / "adjustment_factors.csv")
    write_csv(
        child_uplift_diagnostics.sort_values("origin_msoa"),
        reassign_out / "child_origin_uplift_factors.csv",
    )
    if mode_time_factors is not None and mode_time_check is not None:
        write_csv(
            mode_time_factors.sort_values(["origin_msoa", "period_key", "mode_time_group"]),
            reassign_out / "mode_time_adjustment_factors.csv",
        )
        write_csv(
            mode_time_check.sort_values(["origin_msoa", "period_key", "mode_time_group"]),
            reassign_out / "mode_time_share_check.csv",
        )
    write_csv(share_check, reassign_out / "share_check.csv")

    ensure_parent(config.adjusted_parquet)
    print(f"[reassign] writing adjusted parquet -> {config.adjusted_parquet}")
    parquet_dd = _drop_existing_columns(trips_dd, ["distance_m", "distance_band", "period_key", "mode_time_group"])
    parquet_dd.to_parquet(config.adjusted_parquet)

    if config.estimate_purpose:
        print(f"[reassign] estimating purpose volumes -> {config.purpose_parquet}")
        run_purpose_estimation(config, adjusted_parquet=config.adjusted_parquet)

    if legacy_output_root is not None:
        legacy_out = legacy_output_root / "trips_adjusted.parquet"
        ensure_parent(legacy_out)
        parquet_dd.to_parquet(legacy_out)

    return config.adjusted_parquet
