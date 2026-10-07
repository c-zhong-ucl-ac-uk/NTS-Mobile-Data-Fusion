from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import dask.dataframe as dd
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow.dataset as pa_ds
from shapely.geometry import LineString

from .config import (
    DEFAULT_ADJUSTED_PARQUET,
    DEFAULT_LOCAL_AUTHORITY_OUTPUT_DIR,
    DEFAULT_MSOA_GEOJSON,
    DEFAULT_OUTPUTS_ROOT,
    DEFAULT_PURPOSE_PARQUET,
)
from .io import assert_dask_columns, assert_files_exist, ensure_dir, write_csv


DEFAULT_MSOA_LAD_LOOKUP = Path("data/raw/lookups/OA21_LAD22_LSOA21_MSOA21_LEP22_EN_LU_V2_6716459600479702985.csv")
DEFAULT_EEH_MSOA_LOOKUP = Path("data/raw/lookups/EEH-MSOACDs.csv")

LUTON_LAD_NAME = "Luton"
CENTRAL_BEDFORDSHIRE_LAD_NAME = "Central Bedfordshire"

DUNSTABLE_TOWN_CENTRE_BNG = (501_780.0, 221_790.0)
LUTON_AIRPORT_BNG = (512_100.0, 220_800.0)

DUNSTABLE_CORE_KM = 3.0
DUNSTABLE_WIDER_KM = 5.0
AIRPORT_CORE_KM = 3.0
AIRPORT_WIDER_KM = 5.0

TRIPS_COL = "typical_week_trips"
ADDRESSABLE_MODES = ("BUS", "PRIVATE_CAR", "MOTORCYCLE")
WORKER_PURPOSES = ("Commuting", "Employer Business")
NEAR_ZERO_TRIPS = 1.0

PURPOSE_REQUIRED_COLUMNS = [
    "origin_msoa",
    "destination_msoa",
    "mode_of_transport",
    "time_period",
    "weekend_flag",
    "days_used",
    "purpose",
    "purpose_desc",
    "volume_adj_purpose",
]
ADJUSTED_REQUIRED_COLUMNS = [
    "origin_msoa",
    "destination_msoa",
    "mode_of_transport",
    "time_period",
    "weekend_flag",
    "days_used",
    "volume_adj",
]

MODE_LABELS = {
    "PRIVATE_CAR": "Private car",
    "BUS": "Bus",
    "CYCLE": "Cycle",
    "MOTORCYCLE": "Motorcycle",
    "RAIL": "Rail",
    "SUBWAY": "Subway",
    "WALKING": "Walking",
    "ROAD": "Road",
}
PERIOD_ORDER = {
    "weekday_AM": 1,
    "weekday_PM": 2,
    "weekday_off_peak": 3,
    "weekend": 4,
}
PERIOD_LABELS = {
    "weekday_AM": "Weekday AM",
    "weekday_PM": "Weekday PM",
    "weekday_off_peak": "Weekday off-peak",
    "weekend": "Weekend",
}
MOVEMENT_ORDER = {
    "Inbound": 1,
    "Outbound": 2,
    "Internal": 3,
}
CATCHMENT_ORDER = {
    "Rest of Luton": 1,
    "Dunstable / wider Central Bedfordshire": 2,
    "Rest of EEH": 3,
    "Rest of England and Wales": 4,
    "Airport area": 5,
    "Unknown / not in lookup": 99,
}


@dataclass(frozen=True)
class LocalAuthorityUseCaseConfig:
    purpose_parquet: Path = DEFAULT_PURPOSE_PARQUET
    adjusted_parquet: Path = DEFAULT_ADJUSTED_PARQUET
    msoa_geojson: Path = DEFAULT_MSOA_GEOJSON
    msoa_lad_lookup_csv: Path = DEFAULT_MSOA_LAD_LOOKUP
    eeh_msoa_lookup_csv: Path | None = DEFAULT_EEH_MSOA_LOOKUP
    output_dir: Path = DEFAULT_LOCAL_AUTHORITY_OUTPUT_DIR
    matrix_dir: Path | None = DEFAULT_OUTPUTS_ROOT / "matrices" / "typical_week_by_mode"
    dunstable_core_km: float = DUNSTABLE_CORE_KM
    dunstable_wider_km: float = DUNSTABLE_WIDER_KM
    airport_core_km: float = AIRPORT_CORE_KM
    airport_wider_km: float = AIRPORT_WIDER_KM
    top_n: int = 20
    map_flow_limit: int = 100
    near_zero_threshold: float = NEAR_ZERO_TRIPS
    dunstable_easting: float = DUNSTABLE_TOWN_CENTRE_BNG[0]
    dunstable_northing: float = DUNSTABLE_TOWN_CENTRE_BNG[1]
    airport_easting: float = LUTON_AIRPORT_BNG[0]
    airport_northing: float = LUTON_AIRPORT_BNG[1]


def _as_list(values: Iterable[str]) -> list[str]:
    return sorted(str(value) for value in values)


def _safe_share(df: pd.DataFrame, group_cols: list[str], value_col: str = TRIPS_COL) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out["share"] = pd.Series(dtype="float64")
        return out
    if group_cols:
        denominator = out.groupby(group_cols)[value_col].transform("sum")
    else:
        denominator = pd.Series(out[value_col].sum(), index=out.index)
    out["share"] = (out[value_col] / denominator).where(denominator > 0, 0.0)
    return out


def _add_combined_direction(
    df: pd.DataFrame,
    group_cols: list[str],
    value_col: str = TRIPS_COL,
    direction_col: str = "direction",
) -> pd.DataFrame:
    if df.empty or direction_col not in df.columns:
        return df
    combined_group_cols = [col for col in group_cols if col != direction_col]
    if combined_group_cols:
        combined = df.groupby(combined_group_cols, as_index=False)[value_col].sum()
    else:
        combined = pd.DataFrame({value_col: [df[value_col].sum()]})
    combined[direction_col] = "Combined"
    return pd.concat([df, combined[df.columns]], ignore_index=True)


def _sort_with_order(df: pd.DataFrame, col: str, order: dict[str, int]) -> pd.DataFrame:
    if col not in df.columns:
        return df
    out = df.copy()
    out["_sort_order"] = out[col].map(order).fillna(999).astype(int)
    sort_cols = [c for c in ["area_scope", "direction", "movement", "_sort_order", col] if c in out.columns]
    out = out.sort_values(sort_cols).drop(columns="_sort_order").reset_index(drop=True)
    return out


def _read_msoa_metadata(config: LocalAuthorityUseCaseConfig) -> gpd.GeoDataFrame:
    geo = gpd.read_file(config.msoa_geojson)
    keep_geo = ["MSOA21CD", "MSOA21NM", "BNG_E", "BNG_N", "LAT", "LONG", "geometry"]
    missing_geo = [col for col in keep_geo if col not in geo.columns]
    if missing_geo:
        raise ValueError(f"MSOA geojson missing required columns: {missing_geo}")
    geo = geo[keep_geo].copy()

    lookup = pd.read_csv(
        config.msoa_lad_lookup_csv,
        usecols=["MSOA21CD", "MSOA21NM", "LAD22CD", "LAD22NM"],
    ).drop_duplicates("MSOA21CD")
    meta = geo.merge(lookup, on="MSOA21CD", how="left", suffixes=("", "_lookup"))
    meta["MSOA21NM"] = meta["MSOA21NM_lookup"].fillna(meta["MSOA21NM"])
    meta = meta.drop(columns=["MSOA21NM_lookup"])

    if meta["BNG_E"].isna().any() or meta["BNG_N"].isna().any():
        projected = meta.to_crs("EPSG:27700")
        centroids = projected.geometry.centroid
        meta["BNG_E"] = meta["BNG_E"].fillna(centroids.x)
        meta["BNG_N"] = meta["BNG_N"].fillna(centroids.y)
    return meta


def _distance_km(easting: pd.Series, northing: pd.Series, point: tuple[float, float]) -> pd.Series:
    return np.hypot(easting.astype(float) - point[0], northing.astype(float) - point[1]) / 1000.0


def build_area_definitions(
    config: LocalAuthorityUseCaseConfig,
) -> tuple[pd.DataFrame, gpd.GeoDataFrame, dict[str, set[str]]]:
    meta = _read_msoa_metadata(config)
    dunstable_point = (config.dunstable_easting, config.dunstable_northing)
    airport_point = (config.airport_easting, config.airport_northing)

    meta["dunstable_distance_km"] = _distance_km(meta["BNG_E"], meta["BNG_N"], dunstable_point)
    meta["airport_distance_km"] = _distance_km(meta["BNG_E"], meta["BNG_N"], airport_point)

    luton = meta["LAD22NM"].eq(LUTON_LAD_NAME)
    central_bedfordshire = meta["LAD22NM"].eq(CENTRAL_BEDFORDSHIRE_LAD_NAME)
    dunstable_core = central_bedfordshire & meta["dunstable_distance_km"].le(config.dunstable_core_km)
    dunstable_wider = central_bedfordshire & meta["dunstable_distance_km"].le(config.dunstable_wider_km)
    airport_core = meta["airport_distance_km"].le(config.airport_core_km)
    airport_wider = meta["airport_distance_km"].le(config.airport_wider_km)

    area_specs = [
        ("luton_lad", "Luton LAD", luton, None, np.nan),
        (
            "dunstable_core",
            f"Dunstable core (<={config.dunstable_core_km:g} km)",
            dunstable_core,
            "dunstable_distance_km",
            config.dunstable_core_km,
        ),
        (
            "dunstable_wider",
            f"Dunstable wider (<={config.dunstable_wider_km:g} km)",
            dunstable_wider,
            "dunstable_distance_km",
            config.dunstable_wider_km,
        ),
        (
            "airport_core",
            f"Luton Airport core (<={config.airport_core_km:g} km)",
            airport_core,
            "airport_distance_km",
            config.airport_core_km,
        ),
        (
            "airport_wider",
            f"Luton Airport wider (<={config.airport_wider_km:g} km)",
            airport_wider,
            "airport_distance_km",
            config.airport_wider_km,
        ),
    ]

    rows: list[pd.DataFrame] = []
    area_sets: dict[str, set[str]] = {}
    base_cols = ["MSOA21CD", "MSOA21NM", "LAD22CD", "LAD22NM", "BNG_E", "BNG_N", "LAT", "LONG"]
    for area_key, area_label, mask, distance_col, threshold in area_specs:
        subset = meta.loc[mask, base_cols].copy()
        if distance_col is None:
            subset["anchor_distance_km"] = np.nan
        else:
            subset["anchor_distance_km"] = meta.loc[mask, distance_col].round(3).to_numpy()
        subset["area_key"] = area_key
        subset["area_label"] = area_label
        subset["threshold_km"] = threshold
        area_sets[area_key] = set(subset["MSOA21CD"].astype(str))
        rows.append(subset)

    definitions = pd.concat(rows, ignore_index=True)
    definitions = definitions[
        [
            "area_key",
            "area_label",
            "MSOA21CD",
            "MSOA21NM",
            "LAD22CD",
            "LAD22NM",
            "anchor_distance_km",
            "threshold_km",
            "BNG_E",
            "BNG_N",
            "LAT",
            "LONG",
        ]
    ].sort_values(["area_key", "anchor_distance_km", "MSOA21CD"], na_position="last")

    return definitions.reset_index(drop=True), meta, area_sets


def _add_trip_volume_columns(
    df,
    value_col: str,
    daily_col: str = "daily_trips",
    week_col: str = TRIPS_COL,
):
    out = df.copy()
    values = out[value_col].astype("float64")
    days = out["days_used"].astype("float64").replace(0, np.nan)
    weekend = out["weekend_flag"].astype("int8")
    out[daily_col] = (values / days).fillna(0.0)
    out[week_col] = out[daily_col] * (5 - 3 * weekend)
    return out


def _period_profile_partition(part: pd.DataFrame) -> pd.DataFrame:
    out = part.copy()
    period = out["time_period"].astype(str)
    weekend = out["weekend_flag"].astype(bool)
    profile = pd.Series("weekday_off_peak", index=out.index, dtype="object")
    profile.loc[weekend] = "weekend"
    profile.loc[~weekend & period.eq("AM_peak")] = "weekday_AM"
    profile.loc[~weekend & period.eq("PM_peak")] = "weekday_PM"
    out["period_profile"] = profile
    out["day_type"] = np.where(weekend, "weekend", "weekday")
    return out


def _prepare_purpose_data(path: Path) -> dd.DataFrame:
    data = dd.read_parquet(path)
    assert_dask_columns(data, PURPOSE_REQUIRED_COLUMNS, "Purpose parquet")
    data = _add_trip_volume_columns(data, "volume_adj_purpose", "purpose_daily_volume", TRIPS_COL)
    meta = data._meta.copy()
    meta["period_profile"] = pd.Series(dtype="object")
    meta["day_type"] = pd.Series(dtype="object")
    return data.map_partitions(_period_profile_partition, meta=meta)


def _prepare_adjusted_data(path: Path) -> dd.DataFrame:
    data = dd.read_parquet(path)
    assert_dask_columns(data, ADJUSTED_REQUIRED_COLUMNS, "Adjusted parquet")
    data = _add_trip_volume_columns(data, "volume_adj", "adjusted_daily_volume", TRIPS_COL)
    meta = data._meta.copy()
    meta["period_profile"] = pd.Series(dtype="object")
    meta["day_type"] = pd.Series(dtype="object")
    return data.map_partitions(_period_profile_partition, meta=meta)


def _load_local_touch_dataframe(path: Path, columns: list[str], local_codes: set[str], name: str) -> pd.DataFrame:
    dataset = pa_ds.dataset(str(path), format="parquet")
    missing = [col for col in columns if col not in dataset.schema.names]
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}")

    code_list = _as_list(local_codes)
    chunks: list[pd.DataFrame] = []
    local_filter = pa_ds.field("origin_msoa").isin(code_list) | pa_ds.field("destination_msoa").isin(code_list)
    scanner = dataset.scanner(columns=columns, filter=local_filter, batch_size=250_000, use_threads=False)
    for batch in scanner.to_batches():
        if batch.num_rows:
            chunks.append(batch.to_pandas())
    if not chunks:
        return pd.DataFrame(columns=columns)
    return pd.concat(chunks, ignore_index=True)


def _prepare_purpose_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = _add_trip_volume_columns(df, "volume_adj_purpose", "purpose_daily_volume", TRIPS_COL)
    return _period_profile_partition(out)


def _prepare_adjusted_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = _add_trip_volume_columns(df, "volume_adj", "adjusted_daily_volume", TRIPS_COL)
    return _period_profile_partition(out)


def _to_local_dask(df: pd.DataFrame) -> dd.DataFrame:
    npartitions = max(1, min(16, int(np.ceil(max(len(df), 1) / 100_000))))
    return dd.from_pandas(df, npartitions=npartitions)


def _corridor_data(
    data: dd.DataFrame,
    luton_codes: set[str],
    dunstable_codes: set[str],
    dunstable_label: str,
) -> dd.DataFrame:
    luton_list = _as_list(luton_codes)
    dunstable_list = _as_list(dunstable_codes)
    luton_to_dunstable = data[
        data["origin_msoa"].isin(luton_list) & data["destination_msoa"].isin(dunstable_list)
    ].assign(direction=f"Luton to {dunstable_label}")
    dunstable_to_luton = data[
        data["origin_msoa"].isin(dunstable_list) & data["destination_msoa"].isin(luton_list)
    ].assign(direction=f"{dunstable_label} to Luton")
    return dd.concat([luton_to_dunstable, dunstable_to_luton], interleave_partitions=True)


def _airport_data(data: dd.DataFrame, airport_codes: set[str], area_scope: str) -> dd.DataFrame:
    airport_list = _as_list(airport_codes)
    origin_airport = data["origin_msoa"].isin(airport_list)
    destination_airport = data["destination_msoa"].isin(airport_list)
    inbound = data[(~origin_airport) & destination_airport].assign(movement="Inbound", area_scope=area_scope)
    outbound = data[origin_airport & (~destination_airport)].assign(movement="Outbound", area_scope=area_scope)
    internal = data[origin_airport & destination_airport].assign(movement="Internal", area_scope=area_scope)
    return dd.concat([inbound, outbound, internal], interleave_partitions=True)


def _group_trips(data: dd.DataFrame, group_cols: list[str], value_col: str = TRIPS_COL) -> pd.DataFrame:
    if not group_cols:
        total = data[value_col].sum().compute()
        return pd.DataFrame({value_col: [float(total)]})
    grouped = data.groupby(group_cols)[value_col].sum().reset_index().compute()
    grouped[value_col] = pd.to_numeric(grouped[value_col], errors="coerce").fillna(0.0)
    return grouped


def _msoa_lookup(meta: pd.DataFrame) -> pd.DataFrame:
    return meta[
        ["MSOA21CD", "MSOA21NM", "LAD22CD", "LAD22NM", "LAT", "LONG", "BNG_E", "BNG_N"]
    ].drop_duplicates("MSOA21CD")


def _attach_msoa_columns(df: pd.DataFrame, meta: pd.DataFrame, column: str, prefix: str) -> pd.DataFrame:
    lookup = _msoa_lookup(meta).rename(
        columns={
            "MSOA21CD": column,
            "MSOA21NM": f"{prefix}_msoa_name",
            "LAD22CD": f"{prefix}_lad_code",
            "LAD22NM": f"{prefix}_lad_name",
            "LAT": f"{prefix}_lat",
            "LONG": f"{prefix}_lon",
            "BNG_E": f"{prefix}_easting",
            "BNG_N": f"{prefix}_northing",
        }
    )
    return df.merge(lookup, on=column, how="left")


def _add_mode_labels(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "mode_of_transport" in out.columns:
        out["mode_label"] = out["mode_of_transport"].map(MODE_LABELS).fillna(out["mode_of_transport"])
    return out


def _rank_within(df: pd.DataFrame, group_cols: list[str], value_col: str = TRIPS_COL) -> pd.DataFrame:
    out = df.copy().sort_values(group_cols + [value_col], ascending=[True] * len(group_cols) + [False])
    out["rank"] = out.groupby(group_cols)[value_col].rank(method="first", ascending=False).astype(int)
    return out


def _write_flow_geojson(df: pd.DataFrame, output_path: Path) -> None:
    if df.empty:
        empty = gpd.GeoDataFrame(df.copy(), geometry=[], crs="EPSG:4326")
        ensure_dir(output_path.parent)
        empty.to_file(output_path, driver="GeoJSON")
        return
    required = ["origin_lon", "origin_lat", "destination_lon", "destination_lat"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Flow table missing geometry columns: {missing}")
    geom = [
        LineString([(row.origin_lon, row.origin_lat), (row.destination_lon, row.destination_lat)])
        for row in df.itertuples(index=False)
    ]
    gdf = gpd.GeoDataFrame(df.copy(), geometry=geom, crs="EPSG:4326")
    ensure_dir(output_path.parent)
    gdf.to_file(output_path, driver="GeoJSON")


def _write_centroid_geojson(definitions: pd.DataFrame, output_path: Path) -> None:
    unique = definitions.drop_duplicates("MSOA21CD").copy()
    gdf = gpd.GeoDataFrame(
        unique,
        geometry=gpd.points_from_xy(unique["LONG"], unique["LAT"]),
        crs="EPSG:4326",
    )
    ensure_dir(output_path.parent)
    gdf.to_file(output_path, driver="GeoJSON")


def _headline_with_combined(df: pd.DataFrame, direction_col: str = "direction") -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out
    combined = pd.DataFrame(
        {
            direction_col: ["Combined"],
            TRIPS_COL: [out[TRIPS_COL].sum()],
        }
    )
    return pd.concat([out, combined], ignore_index=True)


def _build_corridor_outputs(
    purpose: dd.DataFrame,
    meta: pd.DataFrame,
    area_sets: dict[str, set[str]],
    output_dir: Path,
    config: LocalAuthorityUseCaseConfig,
) -> dict[str, pd.DataFrame]:
    corridor_dir = output_dir / "luton_dunstable"
    ensure_dir(corridor_dir)

    luton_codes = area_sets["luton_lad"]
    core = _corridor_data(purpose, luton_codes, area_sets["dunstable_core"], "Dunstable core")
    wider = _corridor_data(purpose, luton_codes, area_sets["dunstable_wider"], "Dunstable wider")

    direction = _group_trips(core, ["direction"])
    direction = _headline_with_combined(direction)
    direction = direction.sort_values("direction").reset_index(drop=True)
    write_csv(direction, corridor_dir / "bidirectional_demand.csv")

    mode = _group_trips(core, ["direction", "mode_of_transport"])
    mode = _add_combined_direction(mode, ["direction", "mode_of_transport"])
    mode = _safe_share(mode, ["direction"])
    mode = _add_mode_labels(mode)
    mode = mode.sort_values(["direction", TRIPS_COL], ascending=[True, False]).reset_index(drop=True)
    write_csv(mode, corridor_dir / "mode_shares.csv")

    purpose_share = _group_trips(core, ["direction", "purpose", "purpose_desc"])
    purpose_share = _add_combined_direction(purpose_share, ["direction", "purpose", "purpose_desc"])
    purpose_share = _safe_share(purpose_share, ["direction"])
    purpose_share = purpose_share.sort_values(["direction", "purpose"]).reset_index(drop=True)
    write_csv(purpose_share, corridor_dir / "purpose_shares.csv")

    time_profile = _group_trips(core, ["direction", "period_profile"])
    time_profile = _add_combined_direction(time_profile, ["direction", "period_profile"])
    time_profile = _safe_share(time_profile, ["direction"])
    time_profile = _sort_with_order(time_profile, "period_profile", PERIOD_ORDER)
    write_csv(time_profile, corridor_dir / "time_period_profile.csv")

    od_pairs = _group_trips(core, ["direction", "origin_msoa", "destination_msoa"])
    od_pairs = _rank_within(od_pairs, ["direction"])
    od_pairs = od_pairs[od_pairs["rank"].le(config.top_n)].copy()
    od_pairs = _attach_msoa_columns(od_pairs, meta, "origin_msoa", "origin")
    od_pairs = _attach_msoa_columns(od_pairs, meta, "destination_msoa", "destination")
    od_pairs = od_pairs.sort_values(["direction", "rank"]).reset_index(drop=True)
    write_csv(od_pairs, corridor_dir / "top_od_pairs.csv")

    top_origins = _group_trips(core, ["direction", "origin_msoa"])
    top_origins = _rank_within(top_origins, ["direction"])
    top_origins = top_origins[top_origins["rank"].le(config.top_n)].copy()
    top_origins = _attach_msoa_columns(top_origins, meta, "origin_msoa", "origin")
    write_csv(top_origins.sort_values(["direction", "rank"]), corridor_dir / "top_origin_msoas.csv")

    top_destinations = _group_trips(core, ["direction", "destination_msoa"])
    top_destinations = _rank_within(top_destinations, ["direction"])
    top_destinations = top_destinations[top_destinations["rank"].le(config.top_n)].copy()
    top_destinations = _attach_msoa_columns(top_destinations, meta, "destination_msoa", "destination")
    write_csv(
        top_destinations.sort_values(["direction", "rank"]),
        corridor_dir / "top_destination_msoas.csv",
    )

    mode_totals = _group_trips(core, ["direction", "mode_of_transport"])
    all_totals = mode_totals.groupby("direction", as_index=False)[TRIPS_COL].sum().rename(
        columns={TRIPS_COL: "corridor_total_trips"}
    )
    bus = (
        mode_totals[mode_totals["mode_of_transport"].eq("BUS")]
        .groupby("direction", as_index=False)[TRIPS_COL]
        .sum()
        .rename(columns={TRIPS_COL: "current_bus_trips"})
    )
    car_motorcycle = (
        mode_totals[mode_totals["mode_of_transport"].isin(["PRIVATE_CAR", "MOTORCYCLE"])]
        .groupby("direction", as_index=False)[TRIPS_COL]
        .sum()
        .rename(columns={TRIPS_COL: "car_motorcycle_trips"})
    )
    addressable = all_totals.merge(bus, on="direction", how="left").merge(car_motorcycle, on="direction", how="left")
    for col in ["current_bus_trips", "car_motorcycle_trips"]:
        addressable[col] = pd.to_numeric(addressable[col], errors="coerce").fillna(0.0)
    addressable["addressable_opportunity_trips"] = (
        addressable["current_bus_trips"] + addressable["car_motorcycle_trips"]
    )
    addressable["addressable_share_of_corridor"] = (
        addressable["addressable_opportunity_trips"] / addressable["corridor_total_trips"]
    ).where(addressable["corridor_total_trips"] > 0, 0.0)
    combined_addressable = pd.DataFrame(
        {
            "direction": ["Combined"],
            "corridor_total_trips": [addressable["corridor_total_trips"].sum()],
            "current_bus_trips": [addressable["current_bus_trips"].sum()],
            "car_motorcycle_trips": [addressable["car_motorcycle_trips"].sum()],
            "addressable_opportunity_trips": [addressable["addressable_opportunity_trips"].sum()],
        }
    )
    combined_addressable["addressable_share_of_corridor"] = (
        combined_addressable["addressable_opportunity_trips"] / combined_addressable["corridor_total_trips"]
    ).where(combined_addressable["corridor_total_trips"] > 0, 0.0)
    addressable = pd.concat([addressable, combined_addressable], ignore_index=True)
    addressable["interpretation_note"] = (
        "Current bus plus private car and motorcycle demand; opportunity evidence, not a mode-shift forecast."
    )
    write_csv(addressable.sort_values("direction"), corridor_dir / "addressable_demand.csv")

    sensitivity_rows = []
    for area_scope, data, dunstable_key in [
        ("Dunstable core", core, "dunstable_core"),
        ("Dunstable wider", wider, "dunstable_wider"),
    ]:
        totals = _group_trips(data, ["direction"])
        totals["area_scope"] = area_scope
        totals["dunstable_msoa_count"] = len(area_sets[dunstable_key])
        sensitivity_rows.append(totals)
    sensitivity = pd.concat(sensitivity_rows, ignore_index=True)
    sensitivity = sensitivity[["area_scope", "dunstable_msoa_count", "direction", TRIPS_COL]]
    write_csv(sensitivity.sort_values(["area_scope", "direction"]), corridor_dir / "sensitivity_summary.csv")

    all_pairs = _group_trips(core, ["origin_msoa", "destination_msoa"])
    near_zero = all_pairs[(all_pairs[TRIPS_COL] > 0) & (all_pairs[TRIPS_COL] < config.near_zero_threshold)].copy()
    near_zero = _attach_msoa_columns(near_zero, meta, "origin_msoa", "origin")
    near_zero = _attach_msoa_columns(near_zero, meta, "destination_msoa", "destination")
    near_zero = near_zero.sort_values(TRIPS_COL).head(config.top_n)
    write_csv(near_zero, corridor_dir / "near_zero_od_pairs.csv")

    map_flows = _group_trips(core, ["direction", "origin_msoa", "destination_msoa"])
    map_flows = map_flows.sort_values(TRIPS_COL, ascending=False).head(config.map_flow_limit)
    map_flows = _attach_msoa_columns(map_flows, meta, "origin_msoa", "origin")
    map_flows = _attach_msoa_columns(map_flows, meta, "destination_msoa", "destination")
    write_csv(map_flows, corridor_dir / "corridor_flows_map.csv")
    _write_flow_geojson(map_flows, corridor_dir / "corridor_flows_map.geojson")

    od_mode = _group_trips(core, ["origin_msoa", "destination_msoa", "mode_of_transport"])
    od_mode = od_mode.sort_values(TRIPS_COL, ascending=False).reset_index(drop=True)
    write_csv(od_mode.head(config.map_flow_limit), corridor_dir / "matrix_spot_check_candidates.csv")

    return {
        "direction": direction,
        "mode": mode,
        "purpose": purpose_share,
        "time_profile": time_profile,
        "addressable": addressable,
        "sensitivity": sensitivity,
        "near_zero": near_zero,
        "od_mode": od_mode,
    }


def _read_eeh_codes(config: LocalAuthorityUseCaseConfig) -> set[str]:
    if config.eeh_msoa_lookup_csv is None or not config.eeh_msoa_lookup_csv.exists():
        return set()
    df = pd.read_csv(config.eeh_msoa_lookup_csv)
    if "MSOA21CD" not in df.columns:
        return set()
    return set(df["MSOA21CD"].dropna().astype(str))


def _catchment_lookup(
    meta: pd.DataFrame,
    area_sets: dict[str, set[str]],
    airport_codes: set[str],
    eeh_codes: set[str],
) -> dict[str, str]:
    lookup = {}
    luton_rest = area_sets["luton_lad"] - airport_codes
    central_beds = set(
        meta.loc[meta["LAD22NM"].eq(CENTRAL_BEDFORDSHIRE_LAD_NAME), "MSOA21CD"].dropna().astype(str)
    )
    for code in meta["MSOA21CD"].dropna().astype(str):
        if code in airport_codes:
            label = "Airport area"
        elif code in luton_rest:
            label = "Rest of Luton"
        elif code in central_beds:
            label = "Dunstable / wider Central Bedfordshire"
        elif code in eeh_codes:
            label = "Rest of EEH"
        else:
            label = "Rest of England and Wales"
        lookup[code] = label
    return lookup


def _add_counterparty_catchment(
    df: pd.DataFrame,
    meta: pd.DataFrame,
    area_sets: dict[str, set[str]],
    airport_area_sets: dict[str, set[str]],
    eeh_codes: set[str],
) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out["catchment"] = pd.Series(dtype="object")
        return out
    catchments = {}
    for scope, codes in airport_area_sets.items():
        catchments[scope] = _catchment_lookup(meta, area_sets, codes, eeh_codes)

    labels = []
    for row in out.itertuples(index=False):
        scope = getattr(row, "area_scope")
        movement = getattr(row, "movement")
        if movement == "Inbound":
            code = getattr(row, "origin_msoa")
        elif movement == "Outbound":
            code = getattr(row, "destination_msoa")
        else:
            labels.append("Airport area")
            continue
        labels.append(catchments.get(scope, {}).get(str(code), "Unknown / not in lookup"))
    out["catchment"] = labels
    return out


def _build_airport_outputs(
    purpose: dd.DataFrame,
    meta: pd.DataFrame,
    area_sets: dict[str, set[str]],
    output_dir: Path,
    config: LocalAuthorityUseCaseConfig,
) -> dict[str, pd.DataFrame]:
    airport_dir = output_dir / "luton_airport"
    ensure_dir(airport_dir)
    airport_area_sets = {
        "Airport core": area_sets["airport_core"],
        "Airport wider": area_sets["airport_wider"],
    }
    airport_data = dd.concat(
        [
            _airport_data(purpose, airport_area_sets["Airport core"], "Airport core"),
            _airport_data(purpose, airport_area_sets["Airport wider"], "Airport wider"),
        ],
        interleave_partitions=True,
    )

    movement_summary = _group_trips(airport_data, ["area_scope", "movement"])
    movement_summary = _safe_share(movement_summary, ["area_scope"])
    movement_summary = _sort_with_order(movement_summary, "movement", MOVEMENT_ORDER)
    write_csv(movement_summary, airport_dir / "movement_summary.csv")

    mode = _group_trips(airport_data, ["area_scope", "movement", "mode_of_transport"])
    mode = _safe_share(mode, ["area_scope", "movement"])
    mode = _add_mode_labels(mode)
    mode = mode.sort_values(["area_scope", "movement", TRIPS_COL], ascending=[True, True, False])
    write_csv(mode, airport_dir / "mode_shares.csv")

    purpose_share = _group_trips(airport_data, ["area_scope", "movement", "purpose", "purpose_desc"])
    purpose_share = _safe_share(purpose_share, ["area_scope", "movement"])
    purpose_share = purpose_share.sort_values(["area_scope", "movement", "purpose"]).reset_index(drop=True)
    write_csv(purpose_share, airport_dir / "purpose_shares.csv")

    time_profile = _group_trips(airport_data, ["area_scope", "movement", "period_profile"])
    time_profile = _safe_share(time_profile, ["area_scope", "movement"])
    time_profile = _sort_with_order(time_profile, "period_profile", PERIOD_ORDER)
    write_csv(time_profile, airport_dir / "time_period_profile.csv")

    day_type = _group_trips(airport_data, ["area_scope", "movement", "day_type"])
    day_type = _safe_share(day_type, ["area_scope", "movement"])
    write_csv(day_type.sort_values(["area_scope", "movement", "day_type"]), airport_dir / "weekday_weekend_profile.csv")

    inbound = airport_data[airport_data["movement"] == "Inbound"]
    top_origins = _group_trips(inbound, ["area_scope", "origin_msoa"])
    top_origins = _rank_within(top_origins, ["area_scope"])
    top_origins = top_origins[top_origins["rank"].le(config.top_n)].copy()
    top_origins["movement"] = "Inbound"
    top_origins = _attach_msoa_columns(top_origins, meta, "origin_msoa", "origin")
    write_csv(top_origins.sort_values(["area_scope", "rank"]), airport_dir / "top_20_origins_to_airport.csv")

    outbound = airport_data[airport_data["movement"] == "Outbound"]
    top_destinations = _group_trips(outbound, ["area_scope", "destination_msoa"])
    top_destinations = _rank_within(top_destinations, ["area_scope"])
    top_destinations = top_destinations[top_destinations["rank"].le(config.top_n)].copy()
    top_destinations["movement"] = "Outbound"
    top_destinations = _attach_msoa_columns(top_destinations, meta, "destination_msoa", "destination")
    write_csv(
        top_destinations.sort_values(["area_scope", "rank"]),
        airport_dir / "top_20_destinations_from_airport.csv",
    )

    eeh_codes = _read_eeh_codes(config)
    counterparty_by_zone = _group_trips(
        airport_data,
        ["area_scope", "movement", "origin_msoa", "destination_msoa"],
    )
    counterparty_by_zone = _add_counterparty_catchment(
        counterparty_by_zone,
        meta,
        area_sets,
        airport_area_sets,
        eeh_codes,
    )
    catchment = (
        counterparty_by_zone.groupby(["area_scope", "movement", "catchment"], as_index=False)[TRIPS_COL].sum()
    )
    catchment = _safe_share(catchment, ["area_scope", "movement"])
    catchment["_catchment_order"] = catchment["catchment"].map(CATCHMENT_ORDER).fillna(99).astype(int)
    catchment = catchment.sort_values(["area_scope", "movement", "_catchment_order"]).drop(columns="_catchment_order")
    write_csv(catchment, airport_dir / "catchment_summary.csv")

    worker = airport_data[airport_data["purpose_desc"].isin(list(WORKER_PURPOSES))]
    worker_summary = _group_trips(worker, ["area_scope", "movement", "purpose", "purpose_desc", "mode_of_transport"])
    worker_summary = _safe_share(worker_summary, ["area_scope", "movement", "purpose_desc"])
    worker_summary = _add_mode_labels(worker_summary)
    worker_summary = worker_summary.sort_values(
        ["area_scope", "movement", "purpose", TRIPS_COL],
        ascending=[True, True, True, False],
    )
    write_csv(worker_summary, airport_dir / "worker_access_summary.csv")

    flow_map = _group_trips(airport_data, ["area_scope", "movement", "origin_msoa", "destination_msoa"])
    flow_map = flow_map[flow_map["movement"].isin(["Inbound", "Outbound"])]
    flow_map = flow_map.sort_values(TRIPS_COL, ascending=False).head(config.map_flow_limit)
    flow_map = _attach_msoa_columns(flow_map, meta, "origin_msoa", "origin")
    flow_map = _attach_msoa_columns(flow_map, meta, "destination_msoa", "destination")
    write_csv(flow_map, airport_dir / "airport_flows_map.csv")
    _write_flow_geojson(flow_map, airport_dir / "airport_flows_map.geojson")

    return {
        "movement_summary": movement_summary,
        "mode": mode,
        "purpose": purpose_share,
        "time_profile": time_profile,
        "day_type": day_type,
        "catchment": catchment,
        "worker": worker_summary,
    }


def _study_codes_in_parquet(
    purpose_parquet: Path,
    study_codes: set[str],
) -> set[str]:
    dataset = pa_ds.dataset(str(purpose_parquet), format="parquet")
    columns = ["origin_msoa", "destination_msoa"]
    missing = [col for col in columns if col not in dataset.schema.names]
    if missing:
        raise ValueError(f"Purpose parquet missing required zone columns: {missing}")
    remaining = set(str(code) for code in study_codes)
    found: set[str] = set()
    code_list = _as_list(remaining)
    local_filter = pa_ds.field("origin_msoa").isin(code_list) | pa_ds.field("destination_msoa").isin(code_list)
    scanner = dataset.scanner(columns=columns, filter=local_filter, batch_size=500_000, use_threads=False)
    for batch in scanner.to_batches():
        chunk = batch.to_pandas()
        origin_matches = set(chunk.loc[chunk["origin_msoa"].astype(str).isin(remaining), "origin_msoa"].astype(str))
        destination_matches = set(
            chunk.loc[chunk["destination_msoa"].astype(str).isin(remaining), "destination_msoa"].astype(str)
        )
        found.update(origin_matches)
        found.update(destination_matches)
        remaining -= found
        if not remaining:
            break
    return found


def _zone_presence_checks(
    purpose_parquet: Path,
    area_sets: dict[str, set[str]],
    output_dir: Path,
    matrix_dir: Path | None,
) -> pd.DataFrame:
    study_codes = set().union(*area_sets.values())
    parquet_zones = _study_codes_in_parquet(purpose_parquet, study_codes)

    matrix_zones: set[str] = set()
    matrix_file = None
    if matrix_dir is not None and matrix_dir.exists():
        candidates = sorted(matrix_dir.glob("OD_matrix_*_adjusted.csv"))
        if candidates:
            matrix_file = candidates[0]
            header = pd.read_csv(matrix_file, nrows=0).columns.tolist()
            matrix_zones.update(str(col) for col in header if col != "origin_msoa")
            matrix_origins = pd.read_csv(matrix_file, usecols=["origin_msoa"])["origin_msoa"].dropna().astype(str)
            matrix_zones.update(matrix_origins.tolist())

    rows = []
    for code in sorted(study_codes):
        rows.append(
            {
                "MSOA21CD": code,
                "in_purpose_parquet_zone_list": code in parquet_zones,
                "in_matrix_zone_list": code in matrix_zones if matrix_zones else np.nan,
                "matrix_file_checked": str(matrix_file) if matrix_file else "",
            }
        )
    checks = pd.DataFrame(rows)
    write_csv(checks, output_dir / "qa" / "study_area_zone_check.csv")
    return checks


def _purpose_vs_base_reconciliation(
    purpose: dd.DataFrame,
    adjusted: dd.DataFrame,
    area_sets: dict[str, set[str]],
    output_dir: Path,
) -> pd.DataFrame:
    local_codes = set().union(
        area_sets["luton_lad"],
        area_sets["dunstable_wider"],
        area_sets["airport_wider"],
    )
    local_list = _as_list(local_codes)
    keys = ["origin_msoa", "destination_msoa", "mode_of_transport", "time_period", "weekend_flag"]
    purpose_local = purpose[
        purpose["origin_msoa"].isin(local_list) | purpose["destination_msoa"].isin(local_list)
    ]
    adjusted_local = adjusted[
        adjusted["origin_msoa"].isin(local_list) | adjusted["destination_msoa"].isin(local_list)
    ]

    purpose_grouped = (
        purpose_local.groupby(keys)[TRIPS_COL]
        .sum()
        .reset_index()
        .rename(columns={TRIPS_COL: "purpose_typical_week_trips"})
        .compute()
    )
    base_grouped = (
        adjusted_local.groupby(keys)[TRIPS_COL]
        .sum()
        .reset_index()
        .rename(columns={TRIPS_COL: "base_typical_week_trips"})
        .compute()
    )
    merged = base_grouped.merge(purpose_grouped, on=keys, how="outer")
    for col in ["base_typical_week_trips", "purpose_typical_week_trips"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
    merged["absolute_difference"] = merged["purpose_typical_week_trips"] - merged["base_typical_week_trips"]
    merged["absolute_difference_abs"] = merged["absolute_difference"].abs()
    total_base = merged["base_typical_week_trips"].sum()
    total_purpose = merged["purpose_typical_week_trips"].sum()
    summary = pd.DataFrame(
        [
            {
                "check": "local_touch_purpose_vs_base_typical_week",
                "base_typical_week_trips": total_base,
                "purpose_typical_week_trips": total_purpose,
                "absolute_difference": total_purpose - total_base,
                "relative_difference": (total_purpose - total_base) / total_base if total_base else 0.0,
                "max_row_absolute_difference": merged["absolute_difference_abs"].max() if not merged.empty else 0.0,
                "row_count": len(merged),
            }
        ]
    )
    write_csv(summary, output_dir / "qa" / "purpose_vs_base_reconciliation.csv")
    large_diffs = merged[merged["absolute_difference_abs"] > 1e-6].sort_values("absolute_difference_abs", ascending=False)
    write_csv(large_diffs.head(1000), output_dir / "qa" / "purpose_vs_base_largest_row_differences.csv")
    return summary


def _matrix_spot_checks(
    corridor_tables: dict[str, pd.DataFrame],
    output_dir: Path,
    matrix_dir: Path | None,
) -> pd.DataFrame:
    if matrix_dir is None or not matrix_dir.exists():
        checks = pd.DataFrame(
            [{"status": "skipped", "reason": "Matrix directory not found", "matrix_dir": str(matrix_dir or "")}]
        )
        write_csv(checks, output_dir / "qa" / "matrix_spot_checks.csv")
        return checks

    od_mode = corridor_tables.get("od_mode", pd.DataFrame()).head(20)
    if od_mode.empty:
        checks = pd.DataFrame([{"status": "skipped", "reason": "No mode-specific OD candidates available"}])
        write_csv(checks, output_dir / "qa" / "matrix_spot_checks.csv")
        return checks

    rows = []
    for mode, candidates in od_mode.groupby("mode_of_transport"):
        matrix_file = matrix_dir / f"OD_matrix_{mode}_adjusted.csv"
        if not matrix_file.exists():
            rows.append({"mode_of_transport": mode, "status": "skipped", "reason": "matrix file not found"})
            continue
        needed_destinations = sorted(candidates["destination_msoa"].dropna().astype(str).unique())
        usecols = ["origin_msoa", *needed_destinations]
        try:
            matrix = pd.read_csv(matrix_file, usecols=lambda col: col in set(usecols))
        except ValueError as exc:
            rows.append({"mode_of_transport": mode, "status": "skipped", "reason": str(exc)})
            continue
        for row in candidates.itertuples(index=False):
            origin = str(getattr(row, "origin_msoa"))
            destination = str(getattr(row, "destination_msoa"))
            value = 0.0
            if destination in matrix.columns:
                found = matrix.loc[matrix["origin_msoa"].astype(str).eq(origin), destination]
                if not found.empty:
                    value = float(pd.to_numeric(found.iloc[0], errors="coerce"))
            extracted = float(getattr(row, TRIPS_COL))
            rows.append(
                {
                    "mode_of_transport": mode,
                    "origin_msoa": origin,
                    "destination_msoa": destination,
                    "extracted_typical_week_trips": extracted,
                    "matrix_typical_week_trips": value,
                    "absolute_difference": extracted - value,
                    "status": "checked",
                    "matrix_file": str(matrix_file),
                }
            )
    checks = pd.DataFrame(rows)
    write_csv(checks, output_dir / "qa" / "matrix_spot_checks.csv")
    return checks


def _format_trips(value: float) -> str:
    return f"{value:,.0f}"


def _format_share(value: float) -> str:
    return f"{value:.1%}"


def _summary_value(value: float, units: str) -> str:
    if units == "share":
        return _format_share(value)
    if units == "MSOAs":
        return f"{int(value):,}"
    return _format_trips(value)


def _first_metric(df: pd.DataFrame, filters: dict[str, object], value_col: str) -> float:
    data = df
    for col, value in filters.items():
        data = data[data[col].eq(value)]
    if data.empty:
        return 0.0
    return float(pd.to_numeric(data[value_col], errors="coerce").fillna(0.0).iloc[0])


def _top_metric(
    df: pd.DataFrame,
    filters: dict[str, object],
    label_col: str,
    value_col: str = TRIPS_COL,
) -> tuple[str, float, float]:
    data = df.copy()
    for col, value in filters.items():
        data = data[data[col].eq(value)]
    if data.empty:
        return "", 0.0, 0.0
    row = data.sort_values(value_col, ascending=False).iloc[0]
    share = float(row["share"]) if "share" in row else 0.0
    return str(row[label_col]), float(row[value_col]), share


def _display_period(period_key: str) -> str:
    return PERIOD_LABELS.get(period_key, period_key)


def _add_summary_row(
    rows: list[dict[str, object]],
    section: str,
    metric: str,
    value: float,
    units: str,
    interpretation: str,
) -> None:
    rows.append(
        {
            "section": section,
            "metric": metric,
            "value": value,
            "units": units,
            "display_value": _summary_value(value, units),
            "interpretation": interpretation,
        }
    )


def _build_summary_table(
    area_definitions: pd.DataFrame,
    corridor: dict[str, pd.DataFrame],
    airport: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    area_counts = area_definitions.groupby("area_key")["MSOA21CD"].nunique().to_dict()
    area_labels = {
        "luton_lad": "Luton LAD study area",
        "dunstable_core": "Dunstable core study area",
        "dunstable_wider": "Dunstable wider sensitivity",
        "airport_core": "Luton Airport core study area",
        "airport_wider": "Luton Airport wider sensitivity",
    }
    for key, label in area_labels.items():
        _add_summary_row(
            rows,
            "Study areas",
            label,
            float(area_counts.get(key, 0)),
            "MSOAs",
            "MSOA proxy count used for this evidence extract.",
        )

    direction = corridor["direction"]
    addressable = corridor["addressable"]
    corridor_total = _first_metric(direction, {"direction": "Combined"}, TRIPS_COL)
    _add_summary_row(
        rows,
        "Luton-Dunstable corridor",
        "Combined corridor demand",
        corridor_total,
        "typical-week trips",
        "All adjusted synthetic trips between Luton LAD MSOAs and Dunstable-core MSOAs.",
    )
    for direction_name in ["Luton to Dunstable core", "Dunstable core to Luton"]:
        _add_summary_row(
            rows,
            "Luton-Dunstable corridor",
            direction_name,
            _first_metric(direction, {"direction": direction_name}, TRIPS_COL),
            "typical-week trips",
            "Directional corridor demand.",
        )
    _add_summary_row(
        rows,
        "Luton-Dunstable corridor",
        "Addressable opportunity demand",
        _first_metric(addressable, {"direction": "Combined"}, "addressable_opportunity_trips"),
        "typical-week trips",
        "Current bus plus private car and motorcycle demand; opportunity evidence, not a mode-shift forecast.",
    )
    _add_summary_row(
        rows,
        "Luton-Dunstable corridor",
        "Addressable share of corridor demand",
        _first_metric(addressable, {"direction": "Combined"}, "addressable_share_of_corridor"),
        "share",
        "Share of corridor demand represented by current bus, private car, and motorcycle trips.",
    )
    mode_label, mode_trips, mode_share = _top_metric(corridor["mode"], {"direction": "Combined"}, "mode_label")
    _add_summary_row(
        rows,
        "Luton-Dunstable corridor",
        f"Largest mode: {mode_label}",
        mode_share,
        "share",
        f"{_format_trips(mode_trips)} typical-week trips in the largest mode.",
    )
    purpose_label, purpose_trips, purpose_share = _top_metric(
        corridor["purpose"],
        {"direction": "Combined"},
        "purpose_desc",
    )
    _add_summary_row(
        rows,
        "Luton-Dunstable corridor",
        f"Largest purpose: {purpose_label}",
        purpose_share,
        "share",
        f"{_format_trips(purpose_trips)} typical-week trips in the largest purpose category.",
    )
    period_label, period_trips, period_share = _top_metric(
        corridor["time_profile"],
        {"direction": "Combined"},
        "period_profile",
    )
    _add_summary_row(
        rows,
        "Luton-Dunstable corridor",
        f"Largest time period: {_display_period(period_label)}",
        period_share,
        "share",
        f"{_format_trips(period_trips)} typical-week trips in the largest time-period bucket.",
    )

    movement = airport["movement_summary"]
    for area_scope in ["Airport core", "Airport wider"]:
        area_total = movement.loc[movement["area_scope"].eq(area_scope), TRIPS_COL].sum()
        _add_summary_row(
            rows,
            "Luton Airport surface access",
            f"{area_scope} total movement demand",
            float(area_total),
            "typical-week trips",
            "Inbound, outbound, and internal adjusted synthetic trips touching the airport-area MSOA set.",
        )
    for area_scope, movement_name in [
        ("Airport core", "Inbound"),
        ("Airport core", "Outbound"),
        ("Airport core", "Internal"),
        ("Airport wider", "Inbound"),
        ("Airport wider", "Outbound"),
        ("Airport wider", "Internal"),
    ]:
        _add_summary_row(
            rows,
            "Luton Airport surface access",
            f"{area_scope} {movement_name.lower()} trips",
            _first_metric(movement, {"area_scope": area_scope, "movement": movement_name}, TRIPS_COL),
            "typical-week trips",
            f"{movement_name} component of {area_scope.lower()} movement demand.",
        )

    for movement_name in ["Inbound", "Outbound"]:
        catchment_label, catchment_trips, catchment_share = _top_metric(
            airport["catchment"],
            {"area_scope": "Airport core", "movement": movement_name},
            "catchment",
        )
        _add_summary_row(
            rows,
            "Luton Airport surface access",
            f"Airport core largest {movement_name.lower()} catchment: {catchment_label}",
            catchment_share,
            "share",
            f"{_format_trips(catchment_trips)} typical-week {movement_name.lower()} trips.",
        )
        mode_label, mode_trips, mode_share = _top_metric(
            airport["mode"],
            {"area_scope": "Airport core", "movement": movement_name},
            "mode_label",
        )
        _add_summary_row(
            rows,
            "Luton Airport surface access",
            f"Airport core largest {movement_name.lower()} mode: {mode_label}",
            mode_share,
            "share",
            f"{_format_trips(mode_trips)} typical-week {movement_name.lower()} trips by this mode.",
        )

    worker = airport["worker"]
    for movement_name in ["Inbound", "Outbound"]:
        worker_trips = worker.loc[
            worker["area_scope"].eq("Airport core") & worker["movement"].eq(movement_name),
            TRIPS_COL,
        ].sum()
        _add_summary_row(
            rows,
            "Luton Airport surface access",
            f"Airport core {movement_name.lower()} worker-access proxy",
            float(worker_trips),
            "typical-week trips",
            "Commuting plus employer-business trips; closest available signal for worker-related access.",
        )

    return pd.DataFrame(rows)


def _build_purpose_summary_table(
    corridor: dict[str, pd.DataFrame],
    airport: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    corridor_purpose = corridor["purpose"].copy()
    for row in corridor_purpose.sort_values(["direction", "purpose"]).itertuples(index=False):
        rows.append(
            {
                "case_study": "Luton-Dunstable corridor",
                "area_scope": "Dunstable core",
                "movement": row.direction,
                "purpose": int(row.purpose),
                "purpose_desc": row.purpose_desc,
                "typical_week_trips": float(getattr(row, TRIPS_COL)),
                "share": float(row.share),
                "display_trips": _format_trips(float(getattr(row, TRIPS_COL))),
                "display_share": _format_share(float(row.share)),
                "interpretation": "Purpose split for trips between Luton LAD and Dunstable-core MSOAs.",
            }
        )

    airport_purpose = airport["purpose"].copy()
    for row in airport_purpose.sort_values(["area_scope", "movement", "purpose"]).itertuples(index=False):
        rows.append(
            {
                "case_study": "Luton Airport surface access",
                "area_scope": row.area_scope,
                "movement": row.movement,
                "purpose": int(row.purpose),
                "purpose_desc": row.purpose_desc,
                "typical_week_trips": float(getattr(row, TRIPS_COL)),
                "share": float(row.share),
                "display_trips": _format_trips(float(getattr(row, TRIPS_COL))),
                "display_share": _format_share(float(row.share)),
                "interpretation": (
                    "Purpose split for inbound, outbound, or internal trips touching the airport-area MSOA set."
                ),
            }
        )

    return pd.DataFrame(rows)


def _markdown_table(df: pd.DataFrame) -> list[str]:
    lines = ["| Section | Metric | Result | Units | What it means |", "|---|---|---:|---|---|"]
    for row in df.itertuples(index=False):
        lines.append(
            f"| {row.section} | {row.metric} | {row.display_value} | {row.units} | {row.interpretation} |"
        )
    return lines


def _purpose_markdown_table(df: pd.DataFrame, filters: dict[str, object]) -> list[str]:
    data = df.copy()
    for col, value in filters.items():
        data = data[data[col].eq(value)]
    data = data.sort_values("purpose")
    lines = ["| Purpose | Trips | Share |", "|---|---:|---:|"]
    for row in data.itertuples(index=False):
        lines.append(f"| {row.purpose_desc} | {row.display_trips} | {row.display_share} |")
    return lines


def _write_results_explanation(
    output_dir: Path,
    summary: pd.DataFrame,
    purpose_summary: pd.DataFrame,
    corridor: dict[str, pd.DataFrame],
    airport: dict[str, pd.DataFrame],
    qa_reconciliation: pd.DataFrame,
) -> None:
    corridor_total = _first_metric(corridor["direction"], {"direction": "Combined"}, TRIPS_COL)
    addressable_trips = _first_metric(corridor["addressable"], {"direction": "Combined"}, "addressable_opportunity_trips")
    addressable_share = _first_metric(corridor["addressable"], {"direction": "Combined"}, "addressable_share_of_corridor")
    corridor_mode, _, corridor_mode_share = _top_metric(corridor["mode"], {"direction": "Combined"}, "mode_label")
    corridor_purpose, _, corridor_purpose_share = _top_metric(corridor["purpose"], {"direction": "Combined"}, "purpose_desc")
    corridor_period, _, corridor_period_share = _top_metric(corridor["time_profile"], {"direction": "Combined"}, "period_profile")

    core_total = airport["movement_summary"].loc[
        airport["movement_summary"]["area_scope"].eq("Airport core"),
        TRIPS_COL,
    ].sum()
    wider_total = airport["movement_summary"].loc[
        airport["movement_summary"]["area_scope"].eq("Airport wider"),
        TRIPS_COL,
    ].sum()
    inbound_catchment, _, inbound_catchment_share = _top_metric(
        airport["catchment"],
        {"area_scope": "Airport core", "movement": "Inbound"},
        "catchment",
    )
    outbound_catchment, _, outbound_catchment_share = _top_metric(
        airport["catchment"],
        {"area_scope": "Airport core", "movement": "Outbound"},
        "catchment",
    )
    inbound_mode, _, inbound_mode_share = _top_metric(
        airport["mode"],
        {"area_scope": "Airport core", "movement": "Inbound"},
        "mode_label",
    )
    outbound_mode, _, outbound_mode_share = _top_metric(
        airport["mode"],
        {"area_scope": "Airport core", "movement": "Outbound"},
        "mode_label",
    )

    qa_row = qa_reconciliation.iloc[0] if not qa_reconciliation.empty else None
    qa_line = "The purpose and non-purpose totals were not available for reconciliation."
    if qa_row is not None:
        qa_line = (
            "The purpose rows reconcile to the non-purpose adjusted local-touch total with "
            f"{qa_row['absolute_difference']:.6f} trips/week difference."
        )

    lines = [
        "# Results Explanation",
        "",
        "## Summary Table",
        "",
        *_markdown_table(summary),
        "",
        "## How To Read These Results",
        "",
        (
            "All demand values are adjusted synthetic domestic person trips reported as typical-week "
            "volumes. Each source row is converted by dividing by `days_used`, then applying a five-day "
            "weekday or two-day weekend multiplier."
        ),
        "",
        "## Luton-Dunstable Corridor",
        "",
        (
            f"The Dunstable-core corridor contains {_format_trips(corridor_total)} typical-week trips. "
            "The two directions are almost balanced, which suggests the extract is capturing a broad "
            "two-way urban corridor rather than a single dominant commute direction."
        ),
        (
            f"The largest corridor mode is {corridor_mode} at {_format_share(corridor_mode_share)}. "
            f"The largest trip purpose is {corridor_purpose} at {_format_share(corridor_purpose_share)}, "
            f"and the largest time bucket is {_display_period(corridor_period)} at {_format_share(corridor_period_share)}."
        ),
        (
            f"The addressable-demand measure is {_format_trips(addressable_trips)} trips, or "
            f"{_format_share(addressable_share)} of corridor demand. This is useful opportunity evidence "
            "for busway or public-transport discussions, but it is not a forecast of mode shift."
        ),
        "",
        "### Corridor Purpose Profile",
        "",
        *_purpose_markdown_table(
            purpose_summary,
            {
                "case_study": "Luton-Dunstable corridor",
                "area_scope": "Dunstable core",
                "movement": "Combined",
            },
        ),
        "",
        "## Luton Airport Surface Access",
        "",
        (
            f"The airport-core MSOA set has {_format_trips(float(core_total))} typical-week trips that are "
            "inbound, outbound, or internal to the core area. The wider sensitivity set has "
            f"{_format_trips(float(wider_total))} trips, showing how much the result changes when the "
            "airport proxy is expanded from 3 km to 5 km."
        ),
        (
            f"For the airport core, the largest inbound catchment is {inbound_catchment} "
            f"({_format_share(inbound_catchment_share)}), and the largest outbound catchment is "
            f"{outbound_catchment} ({_format_share(outbound_catchment_share)})."
        ),
        (
            f"The largest airport-core inbound mode is {inbound_mode} ({_format_share(inbound_mode_share)}), "
            f"and the largest outbound mode is {outbound_mode} ({_format_share(outbound_mode_share)}). "
            "This describes existing domestic surface-access patterns around the airport area, including "
            "workers and residents; it does not identify air passengers."
        ),
        "",
        "### Airport-Core Purpose Profile",
        "",
        "Inbound:",
        "",
        *_purpose_markdown_table(
            purpose_summary,
            {
                "case_study": "Luton Airport surface access",
                "area_scope": "Airport core",
                "movement": "Inbound",
            },
        ),
        "",
        "Outbound:",
        "",
        *_purpose_markdown_table(
            purpose_summary,
            {
                "case_study": "Luton Airport surface access",
                "area_scope": "Airport core",
                "movement": "Outbound",
            },
        ),
        "",
        "Internal:",
        "",
        *_purpose_markdown_table(
            purpose_summary,
            {
                "case_study": "Luton Airport surface access",
                "area_scope": "Airport core",
                "movement": "Internal",
            },
        ),
        "",
        "## Caveats",
        "",
        "- Study areas are MSOA proxies and should be validated locally before publication.",
        "- The airport analysis describes trips around the airport MSOA area, not passenger survey records.",
        "- The worker-access proxy uses commuting and employer-business purposes because the source data does not label airport employees.",
        "- The addressable corridor demand is opportunity evidence, not a mode-shift forecast or scheme appraisal result.",
        f"- {qa_line}",
    ]
    (output_dir / "results_explanation.md").write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _write_summary_outputs(
    output_dir: Path,
    area_definitions: pd.DataFrame,
    corridor: dict[str, pd.DataFrame],
    airport: dict[str, pd.DataFrame],
    qa_reconciliation: pd.DataFrame,
) -> None:
    summary = _build_summary_table(area_definitions, corridor, airport)
    purpose_summary = _build_purpose_summary_table(corridor, airport)
    write_csv(summary, output_dir / "summary_table.csv")
    write_csv(purpose_summary, output_dir / "purpose_summary_table.csv")
    _write_results_explanation(output_dir, summary, purpose_summary, corridor, airport, qa_reconciliation)


def _write_officer_brief(
    output_dir: Path,
    area_definitions: pd.DataFrame,
    corridor: dict[str, pd.DataFrame],
    airport: dict[str, pd.DataFrame],
    qa_reconciliation: pd.DataFrame,
) -> None:
    combined_corridor = corridor["direction"].loc[corridor["direction"]["direction"].eq("Combined"), TRIPS_COL]
    corridor_total = float(combined_corridor.iloc[0]) if not combined_corridor.empty else 0.0
    addressable = corridor["addressable"].loc[corridor["addressable"]["direction"].eq("Combined")]
    addressable_trips = float(addressable["addressable_opportunity_trips"].iloc[0]) if not addressable.empty else 0.0
    addressable_share = float(addressable["addressable_share_of_corridor"].iloc[0]) if not addressable.empty else 0.0

    airport_core_total = airport["movement_summary"].loc[
        airport["movement_summary"]["area_scope"].eq("Airport core"),
        TRIPS_COL,
    ].sum()
    airport_wider_total = airport["movement_summary"].loc[
        airport["movement_summary"]["area_scope"].eq("Airport wider"),
        TRIPS_COL,
    ].sum()

    area_counts = area_definitions.groupby("area_key")["MSOA21CD"].nunique().to_dict()
    qa_row = qa_reconciliation.iloc[0] if not qa_reconciliation.empty else None
    qa_line = ""
    if qa_row is not None:
        qa_line = (
            f"Purpose rows reconcile to the non-purpose adjusted local-touch total with "
            f"{qa_row['absolute_difference']:.6f} trips/week difference "
            f"({qa_row['relative_difference']:.6%})."
        )

    lines = [
        "# Luton, Dunstable and Luton Airport Travel Demand Evidence Brief",
        "",
        "## Scope",
        "",
        (
            "This brief uses the adjusted synthetic domestic person-trip demand in "
            "`data/processed/reassign/trips_adjusted_by_purpose.parquet`. "
            "Volumes are reported as typical-week trips after dividing row volumes by `days_used` "
            "and applying a five-day weekday or two-day weekend multiplier."
        ),
        "",
        "Study-area definitions use MSOA proxies for council validation:",
        "",
        f"- Luton LAD: {area_counts.get('luton_lad', 0)} MSOAs.",
        f"- Dunstable core: {area_counts.get('dunstable_core', 0)} MSOAs.",
        f"- Dunstable wider sensitivity: {area_counts.get('dunstable_wider', 0)} MSOAs.",
        f"- Luton Airport core: {area_counts.get('airport_core', 0)} MSOAs.",
        f"- Luton Airport wider sensitivity: {area_counts.get('airport_wider', 0)} MSOAs.",
        "",
        "## Luton-Dunstable Busway Evidence",
        "",
        (
            f"The Dunstable-core corridor has {_format_trips(corridor_total)} typical-week trips "
            "between Luton LAD MSOAs and Dunstable-core MSOAs in the adjusted synthetic demand."
        ),
        (
            f"The addressable-demand evidence measure is {_format_trips(addressable_trips)} typical-week trips "
            f"({addressable_share:.1%} of corridor demand). This is current bus demand plus current "
            "private-car and motorcycle demand, and should not be read as a mode-shift forecast."
        ),
        "",
        "Headline CSVs are in `luton_dunstable/`: bidirectional demand, mode shares, purpose shares, "
        "time-period profile, ranked OD pairs, origin/destination MSOAs, addressable demand, and map-ready flows.",
        "",
        "## Luton Airport Surface Access Evidence",
        "",
        (
            f"The airport-core MSOA set has {_format_trips(float(airport_core_total))} typical-week trips "
            "that are inbound, outbound, or internal to the airport-core area."
        ),
        (
            f"The airport-wider sensitivity set has {_format_trips(float(airport_wider_total))} typical-week trips "
            "that are inbound, outbound, or internal to the wider airport area."
        ),
        "",
        "Airport CSVs are in `luton_airport/`: movement totals, catchments, mode, purpose, weekday/weekend, "
        "time-period profiles, top origins/destinations, worker-access proxy summaries, and map-ready flows.",
        "",
        "## QA Notes",
        "",
        qa_line,
        (
            "The analysis describes existing synthetic domestic person-trip demand. It does not identify air "
            "passengers, distinguish airport employees from other workers, model route assignment, or forecast "
            "demand under Luton Airport expansion scenarios."
        ),
    ]
    (output_dir / "officer_brief.md").write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def run_local_authority_use_case(config: LocalAuthorityUseCaseConfig) -> None:
    assert_files_exist(
        [
            config.purpose_parquet,
            config.adjusted_parquet,
            config.msoa_geojson,
            config.msoa_lad_lookup_csv,
        ]
    )
    ensure_dir(config.output_dir)
    ensure_dir(config.output_dir / "qa")

    area_definitions, meta, area_sets = build_area_definitions(config)
    write_csv(area_definitions, config.output_dir / "area_msoa_definitions.csv")
    _write_centroid_geojson(area_definitions, config.output_dir / "area_msoa_centroids.geojson")

    local_codes = set().union(
        area_sets["luton_lad"],
        area_sets["dunstable_wider"],
        area_sets["airport_wider"],
    )
    purpose_pd = _prepare_purpose_dataframe(
        _load_local_touch_dataframe(
            config.purpose_parquet,
            PURPOSE_REQUIRED_COLUMNS,
            local_codes,
            "Purpose parquet",
        )
    )
    adjusted_pd = _prepare_adjusted_dataframe(
        _load_local_touch_dataframe(
            config.adjusted_parquet,
            ADJUSTED_REQUIRED_COLUMNS,
            local_codes,
            "Adjusted parquet",
        )
    )
    purpose = _to_local_dask(purpose_pd)
    adjusted = _to_local_dask(adjusted_pd)

    corridor = _build_corridor_outputs(purpose, meta, area_sets, config.output_dir, config)
    airport = _build_airport_outputs(purpose, meta, area_sets, config.output_dir, config)
    zone_checks = _zone_presence_checks(config.purpose_parquet, area_sets, config.output_dir, config.matrix_dir)
    reconciliation = _purpose_vs_base_reconciliation(purpose, adjusted, area_sets, config.output_dir)
    _matrix_spot_checks(corridor, config.output_dir, config.matrix_dir)

    missing_zone_rows = zone_checks[
        zone_checks["in_purpose_parquet_zone_list"].eq(False)
        | zone_checks["in_matrix_zone_list"].eq(False)
    ]
    if not missing_zone_rows.empty:
        write_csv(missing_zone_rows, config.output_dir / "qa" / "study_area_zone_missing.csv")

    _write_officer_brief(config.output_dir, area_definitions, corridor, airport, reconciliation)
    _write_summary_outputs(config.output_dir, area_definitions, corridor, airport, reconciliation)
