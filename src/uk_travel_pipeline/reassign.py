from __future__ import annotations

from pathlib import Path
import re

import dask.dataframe as dd
import geopandas as gpd
import numpy as np
import pandas as pd

from .config import ReassignConfig
from .io import assert_dataframe_columns, assert_dask_columns, assert_files_exist, ensure_dir, ensure_parent, write_csv

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


def _add_distances(pdf: pd.DataFrame) -> pd.DataFrame:
    dx = pdf["ox"].to_numpy() - pdf["dx"].to_numpy()
    dy = pdf["oy"].to_numpy() - pdf["dy"].to_numpy()
    pdf["distance_m"] = np.sqrt(dx * dx + dy * dy).astype("float32")
    pdf["distance_miles"] = (pdf["distance_m"] / MILE_IN_M).astype("float32")
    return pdf


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


def calculate_factors(
    trips_dd: dd.DataFrame, nts_band_mode: pd.DataFrame, factor_min: float, factor_max: float
) -> pd.DataFrame:
    bt_band_mode = (
        trips_dd.groupby(["origin_region", "distance_band", "mode_of_transport"])["volume"]
        .sum()
        .compute()
        .reset_index()
        .rename(columns={"mode_of_transport": "bt_mode", "volume": "bt_volume"})
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


def run_reassign(config: ReassignConfig, legacy_output_root: Path | None = None) -> Path:
    required_files = [config.bt_parquet, config.msoa_geojson, config.nts_file]
    if config.msoa_filter_csv is not None:
        required_files.append(config.msoa_filter_csv)
    if config.msoa_region_lookup_csv is not None:
        required_files.append(config.msoa_region_lookup_csv)
    assert_files_exist(required_files)

    all_trips_dd = dd.read_parquet(config.bt_parquet, columns=BT_REQUIRED_COLUMNS)
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

    msoa["centroid"] = msoa.geometry.centroid
    cent = pd.DataFrame(
        {
            "MSOA21CD": msoa["MSOA21CD"].astype(str).values,
            "x": msoa["centroid"].x.values.astype("float32"),
            "y": msoa["centroid"].y.values.astype("float32"),
            "origin_region": msoa["origin_region"].astype(str).values,
        }
    )

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

    nts_df = load_nts_trips_region(config.nts_file)
    labels = [x for x in nts_df["Trip length"].dropna().astype(str).str.strip().unique().tolist() if x != "All lengths"]
    trips_dd = add_distance_bands(trips_dd, labels)
    trips_dd["distance_band"] = trips_dd["distance_band"].astype(str)

    nts_band_mode = build_nts_mode_shares_by_region(nts_df, year=config.year)
    nts_band_mode["distance_band"] = nts_band_mode["distance_band"].astype(str)
    factors = calculate_factors(trips_dd, nts_band_mode, config.factor_min, config.factor_max)

    trips_dd = trips_dd.merge(
        factors.rename(columns={"bt_mode": "mode_of_transport"}),
        on=["origin_region", "distance_band", "mode_of_transport"],
        how="left",
    )
    trips_dd["factor"] = trips_dd["factor"].fillna(1.0)
    trips_dd["volume_adj"] = (trips_dd["volume"] * trips_dd["factor"]).round().astype("int64")
    trips_dd = trips_dd.rename(columns={"factor": "adj_factor"})

    check = (
        trips_dd.groupby(["origin_region", "distance_band", "mode_of_transport"])[["volume", "volume_adj"]]
        .sum()
        .compute()
        .reset_index()
    )
    check["bt_share_before"] = check.groupby(["origin_region", "distance_band"])["volume"].transform(lambda s: s / s.sum())
    check["bt_share_after"] = check.groupby(["origin_region", "distance_band"])["volume_adj"].transform(
        lambda s: s / s.sum()
    )
    check = check.rename(columns={"mode_of_transport": "bt_mode"})
    check = check.merge(nts_band_mode, on=["origin_region", "distance_band", "bt_mode"], how="left")

    reassign_out = config.outputs_root / "reassign"
    ensure_dir(reassign_out)
    write_csv(factors.sort_values(["origin_region", "distance_band", "bt_mode"]), reassign_out / "adjustment_factors.csv")
    write_csv(
        check[
            [
                "origin_region",
                "distance_band",
                "bt_mode",
                "volume",
                "volume_adj",
                "bt_share_before",
                "bt_share_after",
                "nts_share",
            ]
        ].sort_values(["origin_region", "distance_band", "bt_mode"]),
        reassign_out / "share_check.csv",
    )

    ensure_parent(config.adjusted_parquet)
    trips_dd.drop(columns=["distance_m", "distance_band"]).to_parquet(config.adjusted_parquet)

    if legacy_output_root is not None:
        legacy_out = legacy_output_root / "trips_adjusted.parquet"
        ensure_parent(legacy_out)
        trips_dd.drop(columns=["distance_m", "distance_band"]).to_parquet(legacy_out)

    return config.adjusted_parquet
