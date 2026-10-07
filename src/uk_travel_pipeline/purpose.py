from __future__ import annotations

import re
from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from .config import ReassignConfig
from .io import assert_dataframe_columns, assert_files_exist, ensure_dir, ensure_parent, write_csv

LSOA_CODE_RE = re.compile(r"^[A-Z]010\d{5}$")

PURPOSE_CALIBRATION_MODE_TIME_SPLIT = "mode_time_split"
PURPOSE_CALIBRATION_NTS0502 = "nts0502"
PURPOSE_CALIBRATIONS = {PURPOSE_CALIBRATION_MODE_TIME_SPLIT, PURPOSE_CALIBRATION_NTS0502}

NTS_PERIOD_TO_KEY = {
    1: "weekday_AM",
    2: "other",
    3: "other",
    4: "other",
    5: "other",
    6: "other",
}

NTS_PERIOD_TO_PURPOSE_KEY = {
    1: "weekday_AM",
    2: "weekday_off_peak",
    3: "weekday_PM",
    4: "weekday_off_peak",
    5: "weekend",
    6: "weekend",
}

WEEKDAY_NTS0502_PURPOSE_PERIOD_KEYS = ("weekday_AM", "weekday_PM", "weekday_off_peak")

PURPOSE_TO_NTS0502_CONTROL_GROUP = {
    1: "commuting",
    2: "business",
    3: "education",
    4: "shopping",
    5: "personal_business",
    6: "social_vfr",
    7: "social_vfr",
    8: "holiday",
}

NTS0502_CONTROL_GROUPS = set(PURPOSE_TO_NTS0502_CONTROL_GROUP.values())
NTS0502_CONTROL_COLUMNS = ["purpose_period_key", "control_group", "control_share"]


def _mode_mapping(split_road_mode: bool) -> dict[int, str]:
    if split_road_mode:
        return {
            1: "WALKING",
            2: "CYCLE",
            3: "PRIVATE_CAR",
            4: "PRIVATE_CAR",
            5: "BUS",
            6: "RAIL",
            7: "SUBWAY",
        }
    return {
        1: "WALKING",
        2: "ROAD",
        3: "ROAD",
        4: "ROAD",
        5: "ROAD",
        6: "RAIL",
        7: "SUBWAY",
    }


def _bt_period_key(pdf: pd.DataFrame) -> pd.DataFrame:
    out = pdf.copy()
    normalized_period = out["time_period"].astype("string").str.strip().str.lower()
    weekend_flag = pd.to_numeric(out["weekend_flag"], errors="coerce").fillna(0).astype("int8")
    out["period_key"] = "other"
    out.loc[(weekend_flag == 0) & (normalized_period == "am_peak"), "period_key"] = "weekday_AM"
    out["period_key"] = out["period_key"].astype("string")
    return out


def _bt_purpose_period_key(pdf: pd.DataFrame) -> pd.DataFrame:
    out = pdf.copy()
    normalized_period = (
        out["time_period"]
        .astype("string")
        .str.strip()
        .str.lower()
        .str.replace(r"[\s-]+", "_", regex=True)
    )
    weekend_flag = pd.to_numeric(out["weekend_flag"], errors="coerce").fillna(0).astype("int8")
    out["purpose_period_key"] = "weekday_off_peak"
    out.loc[(weekend_flag == 0) & (normalized_period == "am_peak"), "purpose_period_key"] = "weekday_AM"
    out.loc[(weekend_flag == 0) & (normalized_period == "pm_peak"), "purpose_period_key"] = "weekday_PM"
    out.loc[weekend_flag == 1, "purpose_period_key"] = "weekend"
    out["purpose_period_key"] = out["purpose_period_key"].astype("string")
    return out


def _bt_period_columns(pdf: pd.DataFrame) -> pd.DataFrame:
    return _bt_purpose_period_key(_bt_period_key(pdf))


def _lsoa_population_columns(columns: pd.Index) -> list[str]:
    return [c for c in columns if LSOA_CODE_RE.match(str(c))]


def _read_lsoa_population_by_hh_type(path: Path) -> pd.DataFrame:
    columns = pd.read_csv(path, nrows=0).columns
    if "hh_type" not in columns:
        raise ValueError(f"Population file {path} must contain `hh_type`.")

    lsoa_cols = _lsoa_population_columns(columns)
    if not lsoa_cols:
        raise ValueError(f"Population file {path} does not contain LSOA21CD population columns.")

    pop_lsoa_internal = pd.read_csv(path, usecols=["hh_type", *lsoa_cols])
    pop_lsoa_internal["hh_type"] = pd.to_numeric(pop_lsoa_internal["hh_type"], errors="coerce")
    pop_lsoa_internal = pop_lsoa_internal.dropna(subset=["hh_type"])
    pop_lsoa_internal["hh_type"] = pop_lsoa_internal["hh_type"].astype("int16")
    pop_lsoa_hh_wide = pop_lsoa_internal.groupby("hh_type", as_index=False)[lsoa_cols].sum()

    pop_lsoa_long = pd.melt(
        pop_lsoa_hh_wide,
        id_vars=["hh_type"],
        value_vars=lsoa_cols,
        var_name="LSOA21CD",
        value_name="pop",
    )
    pop_lsoa_long["pop"] = pd.to_numeric(pop_lsoa_long["pop"], errors="coerce").fillna(0.0)
    pop_lsoa_long = pop_lsoa_long[pop_lsoa_long["pop"] > 0]
    return pop_lsoa_long


def _build_msoa_population_by_hh_type_and_area(config: ReassignConfig) -> pd.DataFrame:
    pop_lsoa_long = _read_lsoa_population_by_hh_type(config.pop_lsoa_internal_csv)

    tfn_at_lsoa = pd.read_csv(config.tfn_area_type_lsoa_csv)[["lsoa21_code", "tfn_area_type"]]
    tfn_at_lsoa = tfn_at_lsoa.rename(columns={"lsoa21_code": "LSOA21CD"})
    tfn_at_lsoa["tfn_area_type"] = pd.to_numeric(tfn_at_lsoa["tfn_area_type"], errors="coerce")
    pop_lsoa_long = pop_lsoa_long.merge(tfn_at_lsoa, on="LSOA21CD", how="left")

    pop_lsoa_hh = (
        pop_lsoa_long.groupby(["LSOA21CD", "tfn_area_type", "hh_type"], as_index=False)["pop"].sum()
    )

    lsoa_msoa = pd.read_csv(config.lsoa_msoa_lookup_csv, usecols=["LSOA21CD", "MSOA21CD"]).drop_duplicates()
    lsoa_msoa["LSOA21CD"] = lsoa_msoa["LSOA21CD"].astype("string")
    lsoa_msoa["MSOA21CD"] = lsoa_msoa["MSOA21CD"].astype("string")
    pop_msoa_hh = (
        pop_lsoa_hh.merge(lsoa_msoa, on="LSOA21CD", how="left")
        .dropna(subset=["MSOA21CD", "tfn_area_type"])
        .groupby(["MSOA21CD", "tfn_area_type", "hh_type"], as_index=False)["pop"]
        .sum()
    )
    return pop_msoa_hh


def _read_nts_mode_time_split(path: Path) -> pd.DataFrame:
    nts_split = pd.read_csv(path)
    required = ["hh_type", "tfn_at", "mode", "period", "purpose", "trips.est", "rho"]
    assert_dataframe_columns(nts_split, required, "NTS mode/time split csv")
    nts_split["hh_type"] = pd.to_numeric(nts_split["hh_type"], errors="coerce")
    nts_split["tfn_at"] = pd.to_numeric(nts_split["tfn_at"], errors="coerce")
    nts_split["mode"] = pd.to_numeric(nts_split["mode"], errors="coerce")
    nts_split["period"] = pd.to_numeric(nts_split["period"], errors="coerce")
    nts_split["purpose"] = pd.to_numeric(nts_split["purpose"], errors="coerce")
    nts_split["trips.est"] = pd.to_numeric(nts_split["trips.est"], errors="coerce")
    nts_split["rho"] = pd.to_numeric(nts_split["rho"], errors="coerce")
    return nts_split


def _add_purpose_share_by_tfn_hh(nts_split: pd.DataFrame) -> pd.DataFrame:
    purpose_totals = (
        nts_split.groupby(["tfn_at", "hh_type", "purpose"], as_index=False)["trips.est"]
        .sum()
        .rename(columns={"trips.est": "purpose_trips_est"})
    )
    purpose_totals["purpose_trips_est"] = pd.to_numeric(
        purpose_totals["purpose_trips_est"], errors="coerce"
    ).fillna(0.0)
    tfn_hh_total = purpose_totals.groupby(["tfn_at", "hh_type"])["purpose_trips_est"].transform("sum")
    purpose_totals["purpose_share_by_tfn_hh"] = (
        purpose_totals["purpose_trips_est"] / tfn_hh_total
    ).where(tfn_hh_total > 0, 0.0)

    return nts_split.merge(
        purpose_totals[["tfn_at", "hh_type", "purpose", "purpose_share_by_tfn_hh"]],
        on=["tfn_at", "hh_type", "purpose"],
        how="left",
    )


def _build_nts_weighted_trips(config: ReassignConfig) -> pd.DataFrame:
    pop_msoa_hh = _build_msoa_population_by_hh_type_and_area(config)
    nts_split = _add_purpose_share_by_tfn_hh(_read_nts_mode_time_split(config.nts_mode_time_split_csv))

    nts_with_pop = nts_split.merge(
        pop_msoa_hh,
        left_on=["tfn_at", "hh_type"],
        right_on=["tfn_area_type", "hh_type"],
        how="right",
    )
    nts_with_pop = nts_with_pop.dropna(
        subset=["rho", "mode", "period", "purpose", "purpose_share_by_tfn_hh", "MSOA21CD"]
    )
    nts_with_pop["trip_rho"] = (
        nts_with_pop["rho"] * nts_with_pop["pop"] * nts_with_pop["purpose_share_by_tfn_hh"]
    )
    nts_with_pop["mode_time_group"] = nts_with_pop["mode"].astype(int).map(_mode_mapping(config.split_road_mode))
    nts_with_pop["period_key"] = nts_with_pop["period"].astype(int).map(NTS_PERIOD_TO_KEY)
    nts_with_pop = nts_with_pop.dropna(subset=["mode_time_group", "period_key"])
    nts_with_pop["mode_time_group"] = nts_with_pop["mode_time_group"].astype("string")
    nts_with_pop["period_key"] = nts_with_pop["period_key"].astype("string")
    return nts_with_pop


def _purpose_descriptions(config: ReassignConfig) -> pd.DataFrame:
    purposes = pd.read_csv(config.purposes_csv)
    purposes = purposes.rename(columns={"Purpose": "purpose", "Description": "purpose_desc"})
    purposes["purpose"] = pd.to_numeric(purposes["purpose"], errors="coerce")
    purposes = purposes.dropna(subset=["purpose"])
    purposes["purpose"] = purposes["purpose"].astype("int16")
    purposes["purpose_desc"] = purposes["purpose_desc"].astype("string")
    return purposes[["purpose", "purpose_desc"]]


def _build_nts_joint_weighted_trips(config: ReassignConfig) -> pd.DataFrame:
    pop_msoa_hh = _build_msoa_population_by_hh_type_and_area(config)
    nts_split = _read_nts_mode_time_split(config.nts_mode_time_split_csv)
    nts_split = nts_split.dropna(subset=["hh_type", "tfn_at", "mode", "period", "purpose", "trips.est"])
    nts_split["trips.est"] = pd.to_numeric(nts_split["trips.est"], errors="coerce").fillna(0.0)
    total_by_tfn_hh = nts_split.groupby(["tfn_at", "hh_type"])["trips.est"].transform("sum")
    nts_split["joint_share_from_trips_est"] = (nts_split["trips.est"] / total_by_tfn_hh).where(
        total_by_tfn_hh > 0,
        0.0,
    )

    nts_with_pop = nts_split.merge(
        pop_msoa_hh,
        left_on=["tfn_at", "hh_type"],
        right_on=["tfn_area_type", "hh_type"],
        how="right",
    )
    nts_with_pop = nts_with_pop.dropna(
        subset=["joint_share_from_trips_est", "mode", "period", "purpose", "MSOA21CD"]
    )
    nts_with_pop["local_prior_score"] = (
        pd.to_numeric(nts_with_pop["pop"], errors="coerce").fillna(0.0)
        * nts_with_pop["joint_share_from_trips_est"]
    )
    nts_with_pop["mode_of_transport"] = nts_with_pop["mode"].astype(int).map(
        _mode_mapping(config.split_road_mode)
    )
    nts_with_pop["purpose_period_key"] = nts_with_pop["period"].astype(int).map(NTS_PERIOD_TO_PURPOSE_KEY)
    nts_with_pop = nts_with_pop.dropna(subset=["mode_of_transport", "purpose_period_key"])
    nts_with_pop["mode_of_transport"] = nts_with_pop["mode_of_transport"].astype("string")
    nts_with_pop["purpose_period_key"] = nts_with_pop["purpose_period_key"].astype("string")
    nts_with_pop["purpose"] = nts_with_pop["purpose"].astype("int16")
    return nts_with_pop


def _normalise_local_prior(
    scores: pd.DataFrame,
    group_cols: list[str],
) -> pd.DataFrame:
    out = scores.copy()
    totals = out.groupby(group_cols)["local_prior_score"].transform("sum")
    out["local_prior_share"] = (out["local_prior_score"] / totals).where(totals > 0, 0.0)
    return out


def _add_prior_descriptions(prior: pd.DataFrame, config: ReassignConfig) -> pd.DataFrame:
    out = prior.merge(_purpose_descriptions(config), on="purpose", how="left")
    out["purpose"] = out["purpose"].astype("int16")
    out["purpose_desc"] = out["purpose_desc"].astype("string")
    out["local_prior_share"] = pd.to_numeric(out["local_prior_share"], errors="coerce").fillna(0.0)
    return out


def _build_local_purpose_prior_tables(
    config: ReassignConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    nts_with_pop = _build_nts_joint_weighted_trips(config)
    score_cols = ["MSOA21CD", "mode_of_transport", "purpose_period_key", "purpose"]
    primary_scores = nts_with_pop.groupby(score_cols, as_index=False)["local_prior_score"].sum()
    primary = _normalise_local_prior(primary_scores, ["MSOA21CD", "mode_of_transport", "purpose_period_key"])

    if config.split_road_mode and "MOTORCYCLE" not in set(primary["mode_of_transport"].astype(str)):
        motorcycle = primary[primary["mode_of_transport"].astype(str) == "PRIVATE_CAR"].copy()
        motorcycle["mode_of_transport"] = "MOTORCYCLE"
        primary = pd.concat([primary, motorcycle], ignore_index=True)

    origin_period_scores = (
        primary_scores.groupby(["MSOA21CD", "purpose_period_key", "purpose"], as_index=False)["local_prior_score"]
        .sum()
    )
    origin_period = _normalise_local_prior(origin_period_scores, ["MSOA21CD", "purpose_period_key"])

    period_scores = primary_scores.groupby(["purpose_period_key", "purpose"], as_index=False)["local_prior_score"].sum()
    period = _normalise_local_prior(period_scores, ["purpose_period_key"])

    primary = _add_prior_descriptions(primary, config)
    origin_period = _add_prior_descriptions(origin_period, config)
    period = _add_prior_descriptions(period, config)
    return primary, origin_period, period


def build_msoa_mode_time_shares(config: ReassignConfig) -> pd.DataFrame:
    nts_with_pop = _build_nts_weighted_trips(config)
    mode_time_weights = (
        nts_with_pop.groupby(["MSOA21CD", "mode_time_group", "period_key"], as_index=False)["trip_rho"].sum()
    )
    mode_time_weights["nts_mode_time_share"] = mode_time_weights.groupby(["MSOA21CD", "period_key"])[
        "trip_rho"
    ].transform(lambda s: s / s.sum())
    mode_time_weights["MSOA21CD"] = mode_time_weights["MSOA21CD"].astype("string")
    mode_time_weights["mode_time_group"] = mode_time_weights["mode_time_group"].astype("string")
    mode_time_weights["period_key"] = mode_time_weights["period_key"].astype("string")
    mode_time_weights["nts_mode_time_share"] = pd.to_numeric(
        mode_time_weights["nts_mode_time_share"], errors="coerce"
    ).fillna(0.0)
    return mode_time_weights[["MSOA21CD", "mode_time_group", "period_key", "nts_mode_time_share"]]


def build_msoa_purpose_shares(config: ReassignConfig) -> pd.DataFrame:
    nts_with_pop = _build_nts_weighted_trips(config)
    nts_with_pop = nts_with_pop.rename(columns={"mode_time_group": "mode_of_transport"})

    purpose_weights = (
        nts_with_pop.groupby(["MSOA21CD", "mode_of_transport", "period_key", "purpose"], as_index=False)["trip_rho"].sum()
    )
    purpose_weights["purpose_share"] = purpose_weights.groupby(
        ["MSOA21CD", "mode_of_transport", "period_key"]
    )["trip_rho"].transform(lambda s: s / s.sum())
    if config.split_road_mode and "MOTORCYCLE" not in set(purpose_weights["mode_of_transport"].astype(str)):
        motorcycle_weights = purpose_weights[purpose_weights["mode_of_transport"].astype(str) == "PRIVATE_CAR"].copy()
        motorcycle_weights["mode_of_transport"] = "MOTORCYCLE"
        purpose_weights = pd.concat([purpose_weights, motorcycle_weights], ignore_index=True)

    purposes = pd.read_csv(config.purposes_csv)
    purposes = purposes.rename(columns={"Purpose": "purpose", "Description": "purpose_desc"})
    purposes["purpose"] = pd.to_numeric(purposes["purpose"], errors="coerce")

    out = purpose_weights.merge(purposes[["purpose", "purpose_desc"]], on="purpose", how="left")
    out["purpose"] = out["purpose"].astype("int16")
    out["purpose_share"] = pd.to_numeric(out["purpose_share"], errors="coerce").fillna(0.0)
    out["MSOA21CD"] = out["MSOA21CD"].astype("string")
    out["mode_of_transport"] = out["mode_of_transport"].astype("string")
    out["period_key"] = out["period_key"].astype("string")
    out["purpose_desc"] = out["purpose_desc"].astype("string")
    return out[["MSOA21CD", "mode_of_transport", "period_key", "purpose", "purpose_desc", "purpose_share"]]


def _add_purpose_descriptions(shares: pd.DataFrame, config: ReassignConfig) -> pd.DataFrame:
    purposes = pd.read_csv(config.purposes_csv)
    purposes = purposes.rename(columns={"Purpose": "purpose", "Description": "purpose_desc"})
    purposes["purpose"] = pd.to_numeric(purposes["purpose"], errors="coerce")
    out = shares.merge(purposes[["purpose", "purpose_desc"]], on="purpose", how="left")
    out["purpose"] = out["purpose"].astype("int16")
    out["purpose_share"] = pd.to_numeric(out["purpose_share"], errors="coerce").fillna(0.0)
    out["period_key"] = out["period_key"].astype("string")
    out["purpose_desc"] = out["purpose_desc"].astype("string")
    return out


def build_msoa_period_purpose_shares(config: ReassignConfig) -> pd.DataFrame:
    nts_with_pop = _build_nts_weighted_trips(config)
    weights = nts_with_pop.groupby(["MSOA21CD", "period_key", "purpose"], as_index=False)["trip_rho"].sum()
    weights["purpose_share"] = weights.groupby(["MSOA21CD", "period_key"])["trip_rho"].transform(lambda s: s / s.sum())
    out = _add_purpose_descriptions(weights, config)
    out["MSOA21CD"] = out["MSOA21CD"].astype("string")
    return out[["MSOA21CD", "period_key", "purpose", "purpose_desc", "purpose_share"]]


def build_period_purpose_shares(config: ReassignConfig) -> pd.DataFrame:
    nts_with_pop = _build_nts_weighted_trips(config)
    weights = nts_with_pop.groupby(["period_key", "purpose"], as_index=False)["trip_rho"].sum()
    weights["purpose_share"] = weights.groupby("period_key")["trip_rho"].transform(lambda s: s / s.sum())
    out = _add_purpose_descriptions(weights, config)
    return out[["period_key", "purpose", "purpose_desc", "purpose_share"]]


def _read_nts0502_period_purpose_controls(path: Path) -> pd.DataFrame:
    controls = pd.read_csv(path)
    assert_dataframe_columns(controls, NTS0502_CONTROL_COLUMNS, "NTS0502 period/purpose control csv")
    controls["purpose_period_key"] = controls["purpose_period_key"].astype("string").str.strip()
    controls["control_group"] = controls["control_group"].astype("string").str.strip()
    controls["control_share"] = pd.to_numeric(controls["control_share"], errors="coerce").fillna(0.0)

    invalid_periods = sorted(
        set(controls["purpose_period_key"].dropna().astype(str)) - set(WEEKDAY_NTS0502_PURPOSE_PERIOD_KEYS)
    )
    if invalid_periods:
        raise ValueError(f"NTS0502 controls contain unsupported period keys: {invalid_periods}")

    invalid_groups = sorted(set(controls["control_group"].dropna().astype(str)) - NTS0502_CONTROL_GROUPS)
    if invalid_groups:
        raise ValueError(f"NTS0502 controls contain unsupported control groups: {invalid_groups}")

    agg = {"control_share": "sum"}
    if "source_year" in controls.columns:
        agg["source_year"] = "first"
    controls = controls.groupby(["purpose_period_key", "control_group"], as_index=False).agg(agg)
    period_total = controls.groupby("purpose_period_key")["control_share"].transform("sum")
    if (period_total <= 0).any():
        bad = sorted(set(controls.loc[period_total <= 0, "purpose_period_key"].astype(str)))
        raise ValueError(f"NTS0502 controls have zero total shares for period keys: {bad}")
    controls["control_share"] = controls["control_share"] / period_total
    return controls


def _attach_local_priors_to_rows(
    row_totals: pd.DataFrame,
    primary: pd.DataFrame,
    origin_period: pd.DataFrame,
    period: pd.DataFrame,
) -> pd.DataFrame:
    base_cols = ["row_id", "origin_msoa", "mode_of_transport", "purpose_period_key", "row_volume"]
    rows = row_totals.copy()
    rows["origin_msoa"] = rows["origin_msoa"].astype("string")
    rows["mode_of_transport"] = rows["mode_of_transport"].astype("string")
    rows["purpose_period_key"] = rows["purpose_period_key"].astype("string")

    primary_lookup = primary.rename(columns={"MSOA21CD": "origin_msoa"}).copy()
    primary_lookup["origin_msoa"] = primary_lookup["origin_msoa"].astype("string")
    primary_lookup["mode_of_transport"] = primary_lookup["mode_of_transport"].astype("string")
    primary_lookup["purpose_period_key"] = primary_lookup["purpose_period_key"].astype("string")

    merged = rows.merge(
        primary_lookup,
        on=["origin_msoa", "mode_of_transport", "purpose_period_key"],
        how="left",
    )
    matched_primary = merged[~merged["local_prior_share"].isna()].copy()
    missing_primary = merged[merged["local_prior_share"].isna()][base_cols].drop_duplicates()

    origin_lookup = origin_period.rename(columns={"MSOA21CD": "origin_msoa"}).copy()
    origin_lookup["origin_msoa"] = origin_lookup["origin_msoa"].astype("string")
    origin_lookup["purpose_period_key"] = origin_lookup["purpose_period_key"].astype("string")
    origin_merged = missing_primary.merge(origin_lookup, on=["origin_msoa", "purpose_period_key"], how="left")
    matched_origin = origin_merged[~origin_merged["local_prior_share"].isna()].copy()
    missing_origin = origin_merged[origin_merged["local_prior_share"].isna()][base_cols].drop_duplicates()

    period_lookup = period.copy()
    period_lookup["purpose_period_key"] = period_lookup["purpose_period_key"].astype("string")
    matched_period = missing_origin.merge(period_lookup, on="purpose_period_key", how="left")

    out = pd.concat([matched_primary, matched_origin, matched_period], ignore_index=True)
    missing = out[out["local_prior_share"].isna() & (pd.to_numeric(out["row_volume"], errors="coerce").fillna(0.0) > 0)]
    if not missing.empty:
        examples = missing[base_cols].drop_duplicates().head(5).to_dict("records")
        raise ValueError(
            "NTS0502 purpose calibration could not find local prior shares for adjusted trip rows. "
            f"First missing origin/mode/period combinations: {examples}"
        )

    out = out[~out["local_prior_share"].isna()].copy()
    out["local_prior_share"] = pd.to_numeric(out["local_prior_share"], errors="coerce").fillna(0.0)
    out["row_volume"] = pd.to_numeric(out["row_volume"], errors="coerce").fillna(0.0)
    out["purpose"] = out["purpose"].astype("int16")
    out["purpose_desc"] = out["purpose_desc"].astype("string")
    out["purpose_control_group"] = out["purpose"].astype(int).map(PURPOSE_TO_NTS0502_CONTROL_GROUP)
    if out["purpose_control_group"].isna().any():
        bad = sorted(set(out.loc[out["purpose_control_group"].isna(), "purpose"].astype(int)))
        raise ValueError(f"Purposes are missing an NTS0502 control-group mapping: {bad}")
    out["purpose_control_group"] = out["purpose_control_group"].astype("string")
    return out


def _control_targets(row_totals: pd.DataFrame, controls: pd.DataFrame) -> pd.DataFrame:
    period_totals = (
        row_totals.groupby("purpose_period_key", as_index=False)["row_volume"]
        .sum()
        .rename(columns={"row_volume": "period_total_trips"})
    )
    targets = controls.merge(period_totals, on="purpose_period_key", how="left")
    targets["period_total_trips"] = pd.to_numeric(targets["period_total_trips"], errors="coerce").fillna(0.0)
    targets["target_trips"] = targets["control_share"] * targets["period_total_trips"]
    return targets


def _purpose_control_check(
    allocations: pd.DataFrame,
    row_totals: pd.DataFrame,
    controls: pd.DataFrame,
) -> pd.DataFrame:
    targets = _control_targets(row_totals, controls)
    actual = (
        allocations.groupby(["purpose_period_key", "purpose_control_group"], as_index=False)["allocated_trips"]
        .sum()
        .rename(columns={"purpose_control_group": "control_group", "allocated_trips": "actual_trips"})
    )
    check = targets.merge(actual, on=["purpose_period_key", "control_group"], how="left")
    check["actual_trips"] = pd.to_numeric(check["actual_trips"], errors="coerce").fillna(0.0)
    check["actual_control_share"] = (check["actual_trips"] / check["period_total_trips"]).where(
        check["period_total_trips"] > 0,
        0.0,
    )
    check = check.rename(columns={"control_share": "target_control_share"})
    check["diff_trips"] = check["actual_trips"] - check["target_trips"]
    check["diff_control_share"] = check["actual_control_share"] - check["target_control_share"]
    ordered_cols = [
        "purpose_period_key",
        "control_group",
        "period_total_trips",
        "target_control_share",
        "actual_control_share",
        "target_trips",
        "actual_trips",
        "diff_trips",
        "diff_control_share",
    ]
    if "source_year" in check.columns:
        ordered_cols.append("source_year")
    return check[ordered_cols].sort_values(["purpose_period_key", "control_group"])


def _rake_purpose_allocations(
    allocations: pd.DataFrame,
    row_totals: pd.DataFrame,
    controls: pd.DataFrame,
    max_iterations: int = 100,
    tolerance: float = 1e-6,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = allocations.copy()
    out["allocated_trips"] = out["row_volume"] * out["local_prior_share"]
    targets = _control_targets(row_totals, controls).rename(columns={"control_group": "purpose_control_group"})
    out = out.merge(
        targets[["purpose_period_key", "purpose_control_group", "target_trips"]],
        on=["purpose_period_key", "purpose_control_group"],
        how="left",
    )

    controlled = out["target_trips"].notna()
    initial_actual = (
        out[controlled]
        .groupby(["purpose_period_key", "purpose_control_group"], as_index=False)["allocated_trips"]
        .sum()
    )
    impossible = targets.merge(
        initial_actual,
        on=["purpose_period_key", "purpose_control_group"],
        how="left",
    )
    impossible["allocated_trips"] = pd.to_numeric(impossible["allocated_trips"], errors="coerce").fillna(0.0)
    impossible = impossible[(impossible["target_trips"] > tolerance) & (impossible["allocated_trips"] <= 0)]
    if not impossible.empty:
        examples = impossible[["purpose_period_key", "purpose_control_group", "target_trips"]].to_dict("records")
        raise ValueError(f"NTS0502 controls require purpose groups with zero local prior trips: {examples}")

    for _ in range(max_iterations):
        controlled = out["target_trips"].notna()
        current_control = out.loc[controlled].groupby(
            ["purpose_period_key", "purpose_control_group"]
        )["allocated_trips"].transform("sum")
        scale = (out.loc[controlled, "target_trips"] / current_control).where(current_control > 0, 1.0)
        out.loc[controlled, "allocated_trips"] = out.loc[controlled, "allocated_trips"] * scale

        current_row = out.groupby("row_id")["allocated_trips"].transform("sum")
        row_scale = (out["row_volume"] / current_row).where(current_row > 0, 1.0)
        out["allocated_trips"] = out["allocated_trips"] * row_scale

        check = _purpose_control_check(out, row_totals, controls)
        max_control_diff = check["diff_trips"].abs().max() if not check.empty else 0.0
        max_row_diff = (out.groupby("row_id")["allocated_trips"].sum() - out.groupby("row_id")["row_volume"].first()).abs().max()
        if max(max_control_diff, max_row_diff) <= tolerance:
            break

    out["purpose_share"] = (out["allocated_trips"] / out["row_volume"]).where(out["row_volume"] > 0, 0.0)
    share_cols = [
        "origin_msoa",
        "mode_of_transport",
        "purpose_period_key",
        "purpose",
        "purpose_desc",
        "purpose_control_group",
        "local_prior_share",
        "purpose_share",
    ]
    control_check = _purpose_control_check(out, row_totals, controls)
    return out[share_cols], control_check


def build_nts0502_raked_purpose_shares(
    config: ReassignConfig,
    row_totals: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if config.nts0502_period_purpose_csv is None:
        raise ValueError("NTS0502 purpose calibration requires --nts0502-period-purpose-csv.")
    controls = _read_nts0502_period_purpose_controls(config.nts0502_period_purpose_csv)
    rows = row_totals.copy()
    rows["row_volume"] = pd.to_numeric(rows["row_volume"], errors="coerce").fillna(0.0)
    rows = rows[rows["row_volume"] > 0].copy()
    rows["row_id"] = range(len(rows))
    primary, origin_period, period = _build_local_purpose_prior_tables(config)
    allocations = _attach_local_priors_to_rows(rows, primary, origin_period, period)
    return _rake_purpose_allocations(allocations, rows, controls)


def _read_adjusted_grouped(adjusted: Path) -> dd.DataFrame:
    trips_dd = dd.read_parquet(adjusted, split_row_groups=True)
    if "volume_adj" in trips_dd.columns:
        trips_dd["volume_adj"] = dd.to_numeric(trips_dd["volume_adj"], errors="coerce").fillna(0.0)
    elif "volume" in trips_dd.columns:
        trips_dd["volume_adj"] = dd.to_numeric(trips_dd["volume"], errors="coerce").fillna(5.0)
    else:
        raise ValueError("Adjusted parquet must contain `volume_adj` or `volume`.")

    agg_keys = [
        "origin_msoa",
        "destination_msoa",
        "mode_of_transport",
        "time_period",
        "weekend_flag",
        "days_used",
    ]
    return trips_dd.groupby(agg_keys)["volume_adj"].sum().reset_index()


def _run_nts0502_purpose_estimation(config: ReassignConfig, adjusted: Path) -> Path:
    trips_dd = _read_adjusted_grouped(adjusted)

    period_meta = trips_dd._meta.assign(
        period_key=pd.Series([], dtype="string"),
        purpose_period_key=pd.Series([], dtype="string"),
    )
    trips_dd = trips_dd.map_partitions(_bt_period_columns, meta=period_meta)
    trips_dd["origin_msoa"] = trips_dd["origin_msoa"].astype("string")
    trips_dd["destination_msoa"] = trips_dd["destination_msoa"].astype("string")
    trips_dd["mode_of_transport"] = trips_dd["mode_of_transport"].astype("string")
    trips_dd["period_key"] = trips_dd["period_key"].astype("string")
    trips_dd["purpose_period_key"] = trips_dd["purpose_period_key"].astype("string")

    row_totals = (
        trips_dd.groupby(["origin_msoa", "mode_of_transport", "purpose_period_key"])["volume_adj"]
        .sum()
        .compute()
        .reset_index()
        .rename(columns={"volume_adj": "row_volume"})
    )
    shares, control_check = build_nts0502_raked_purpose_shares(config, row_totals)
    shares["origin_msoa"] = shares["origin_msoa"].astype("string")
    shares["mode_of_transport"] = shares["mode_of_transport"].astype("string")
    shares["purpose_period_key"] = shares["purpose_period_key"].astype("string")

    out_dd = trips_dd.merge(
        shares,
        on=["origin_msoa", "mode_of_transport", "purpose_period_key"],
        how="left",
    )
    missing_summary = (
        out_dd[out_dd["purpose_share"].isna()]
        .groupby(["origin_msoa", "mode_of_transport", "purpose_period_key"])["volume_adj"]
        .sum()
        .reset_index()
        .compute()
    )
    missing_summary = missing_summary[pd.to_numeric(missing_summary["volume_adj"], errors="coerce").fillna(0.0) > 0]
    if not missing_summary.empty:
        examples = missing_summary.head(5).to_dict("records")
        raise ValueError(f"NTS0502 purpose shares are missing for adjusted trips: {examples}")

    out_dd = out_dd[~out_dd["purpose_share"].isna()]
    out_dd["purpose"] = out_dd["purpose"].astype("int16")
    out_dd["purpose_desc"] = out_dd["purpose_desc"].astype("string")
    out_dd["purpose_control_group"] = out_dd["purpose_control_group"].astype("string")
    out_dd["purpose_share"] = out_dd["purpose_share"].astype("float64")
    out_dd["local_prior_share"] = out_dd["local_prior_share"].astype("float64")
    out_dd["volume_adj_purpose"] = out_dd["volume_adj"] * out_dd["purpose_share"]
    out_dd["period_key"] = out_dd["period_key"].astype("string")
    out_dd["purpose_period_key"] = out_dd["purpose_period_key"].astype("string")

    reassign_out = config.outputs_root / "reassign"
    ensure_dir(reassign_out)
    write_csv(
        control_check.sort_values(["purpose_period_key", "control_group"]),
        reassign_out / "nts0502_purpose_control_check.csv",
    )

    ensure_parent(config.purpose_parquet)
    out_dd.to_parquet(config.purpose_parquet)
    return config.purpose_parquet


def run_purpose_estimation(config: ReassignConfig, adjusted_parquet: Path | None = None) -> Path:
    adjusted = adjusted_parquet or config.adjusted_parquet
    if config.purpose_calibration not in PURPOSE_CALIBRATIONS:
        raise ValueError(
            f"Unsupported purpose calibration {config.purpose_calibration!r}; "
            f"expected one of {sorted(PURPOSE_CALIBRATIONS)}."
        )

    required_files = [
        adjusted,
        config.pop_lsoa_internal_csv,
        config.tfn_area_type_lsoa_csv,
        config.lsoa_msoa_lookup_csv,
        config.nts_mode_time_split_csv,
        config.purposes_csv,
    ]
    if config.purpose_calibration == PURPOSE_CALIBRATION_NTS0502:
        if config.nts0502_period_purpose_csv is None:
            raise ValueError("NTS0502 purpose calibration requires --nts0502-period-purpose-csv.")
        required_files.append(config.nts0502_period_purpose_csv)
    assert_files_exist(
        required_files
    )

    if config.purpose_calibration == PURPOSE_CALIBRATION_NTS0502:
        return _run_nts0502_purpose_estimation(config, adjusted)

    shares = build_msoa_purpose_shares(config).rename(columns={"MSOA21CD": "origin_msoa"})
    msoa_period_shares = build_msoa_period_purpose_shares(config).rename(columns={"MSOA21CD": "origin_msoa"})
    period_shares = build_period_purpose_shares(config)

    # Pre-aggregate adjusted trips before purpose allocation to avoid large row expansion
    # (especially when ROAD mode has already been split into sub-modes).
    trips_dd = _read_adjusted_grouped(adjusted)

    period_meta = trips_dd._meta.assign(period_key=pd.Series([], dtype="string"))
    trips_dd = trips_dd.map_partitions(_bt_period_key, meta=period_meta)
    trips_dd["origin_msoa"] = trips_dd["origin_msoa"].astype("string")
    trips_dd["destination_msoa"] = trips_dd["destination_msoa"].astype("string")
    trips_dd["mode_of_transport"] = trips_dd["mode_of_transport"].astype("string")
    trips_dd["period_key"] = trips_dd["period_key"].astype("string")
    shares["origin_msoa"] = shares["origin_msoa"].astype("string")
    shares["mode_of_transport"] = shares["mode_of_transport"].astype("string")
    shares["period_key"] = shares["period_key"].astype("string")
    msoa_period_shares["origin_msoa"] = msoa_period_shares["origin_msoa"].astype("string")
    msoa_period_shares["period_key"] = msoa_period_shares["period_key"].astype("string")
    period_shares["period_key"] = period_shares["period_key"].astype("string")

    primary_dd = trips_dd.merge(
        shares,
        on=["origin_msoa", "mode_of_transport", "period_key"],
        how="left",
    )
    base_cols = [
        "origin_msoa",
        "destination_msoa",
        "mode_of_transport",
        "time_period",
        "weekend_flag",
        "days_used",
        "volume_adj",
        "period_key",
    ]
    matched_primary_dd = primary_dd[~primary_dd["purpose_share"].isna()]

    missing_primary_dd = primary_dd[primary_dd["purpose_share"].isna()][base_cols]
    fallback_origin_dd = missing_primary_dd.merge(
        msoa_period_shares,
        on=["origin_msoa", "period_key"],
        how="left",
    )
    matched_fallback_origin_dd = fallback_origin_dd[~fallback_origin_dd["purpose_share"].isna()]

    missing_origin_dd = fallback_origin_dd[fallback_origin_dd["purpose_share"].isna()][base_cols]
    fallback_period_dd = missing_origin_dd.merge(
        period_shares,
        on="period_key",
        how="left",
    )
    out_dd = dd.concat(
        [
            matched_primary_dd,
            matched_fallback_origin_dd,
            fallback_period_dd,
        ],
        interleave_partitions=True,
    )
    missing_summary = (
        out_dd[out_dd["purpose_share"].isna()]
        .groupby(["origin_msoa", "mode_of_transport", "period_key"])["volume_adj"]
        .sum()
        .reset_index()
        .compute()
    )
    missing_summary = missing_summary[pd.to_numeric(missing_summary["volume_adj"], errors="coerce").fillna(0.0) > 0]
    if not missing_summary.empty:
        examples = missing_summary.head(5).to_dict("records")
        raise ValueError(
            "Purpose shares are missing for adjusted trips. "
            "Check that the population, TFN area type, and LSOA-to-MSOA lookup files cover all trip origins. "
            f"First missing origin/mode/period combinations: {examples}"
        )

    out_dd = out_dd[~out_dd["purpose_share"].isna()]
    out_dd["purpose"] = out_dd["purpose"].astype("int16")
    out_dd["purpose_desc"] = out_dd["purpose_desc"].astype("string")
    out_dd["purpose_share"] = out_dd["purpose_share"].astype("float64")
    out_dd["volume_adj_purpose"] = out_dd["volume_adj"] * out_dd["purpose_share"]
    out_dd["period_key"] = out_dd["period_key"].astype("string")

    ensure_parent(config.purpose_parquet)
    out_dd.to_parquet(config.purpose_parquet)
    return config.purpose_parquet
