from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from .io import assert_dataframe_columns, ensure_dir
from .purpose import NTS_PERIOD_TO_KEY, _bt_period_key


MODE_SUMMARY_COLUMNS = [
    "mode_of_transport",
    "time_period",
    "weekend_flag",
    "volume_adj",
]
PURPOSE_SUMMARY_COLUMNS = [
    "purpose",
    "purpose_desc",
    "volume_adj_purpose",
]
NTS_PURPOSE_COLUMNS = [
    "period",
    "purpose",
    "trips.est",
]
PERIOD_BUCKETS = {
    "weekday_AM": "AM trips",
    "other": "Other trips",
}


def _with_proportion(df: pd.DataFrame, value_col: str = "trips") -> pd.DataFrame:
    total = pd.to_numeric(df[value_col], errors="coerce").fillna(0.0).sum()
    out = df.copy()
    out["proportion"] = out[value_col] / total if total else 0.0
    return out


def _mode_summary(df: dd.DataFrame, modes: pd.DataFrame, am_weekday_only: bool = False) -> pd.DataFrame:
    data = df
    if am_weekday_only:
        data = data[(data["weekend_flag"].astype("int8") == 0) & (data["time_period"] == "AM_peak")]

    out = (
        data.groupby("mode_of_transport")["volume_adj"]
        .sum()
        .reset_index()
        .rename(columns={"mode_of_transport": "mode", "volume_adj": "trips"})
        .compute()
    )
    out = modes.merge(out, on="mode", how="left")
    out["trips"] = pd.to_numeric(out["trips"], errors="coerce").fillna(0.0)
    out = out.sort_values("mode").reset_index(drop=True)
    return _with_proportion(out)


def _ensure_period_key(df: dd.DataFrame) -> dd.DataFrame:
    if "period_key" in df.columns:
        return df

    assert_dataframe_columns(
        df._meta,
        ["time_period", "weekend_flag"],
        "Purpose parquet without period_key",
    )
    meta = df._meta.assign(period_key=pd.Series([], dtype="string"))
    out = df.map_partitions(_bt_period_key, meta=meta)
    out["period_key"] = out["period_key"].astype("string")
    return out


def _purpose_summary(
    df: dd.DataFrame,
    purposes: pd.DataFrame,
    period_key: str | None = None,
) -> pd.DataFrame:
    data = df
    if period_key is not None:
        data = data[data["period_key"] == period_key]

    out = (
        data.groupby(["purpose", "purpose_desc"])["volume_adj_purpose"]
        .sum()
        .reset_index()
        .rename(columns={"volume_adj_purpose": "trips"})
        .compute()
    )
    out = purposes.merge(out[["purpose", "trips"]], on="purpose", how="left")
    out["purpose"] = out["purpose"].astype(int)
    out["trips"] = pd.to_numeric(out["trips"], errors="coerce").fillna(0.0)
    out = out.sort_values("purpose").reset_index(drop=True)
    return _with_proportion(out)


def _period_bucket_summary(df: dd.DataFrame) -> pd.DataFrame:
    grouped = (
        df[df["period_key"].isin(list(PERIOD_BUCKETS))]
        .groupby("period_key")["volume_adj_purpose"]
        .sum()
        .reset_index()
        .rename(columns={"volume_adj_purpose": "trips"})
        .compute()
    )
    trips_by_period = dict(zip(grouped["period_key"], grouped["trips"], strict=False))
    total = sum(float(trips_by_period.get(key, 0.0)) for key in PERIOD_BUCKETS)
    rows = [
        {
            "period_bucket": label,
            "trips": float(trips_by_period.get(key, 0.0)),
            "proportion": float(trips_by_period.get(key, 0.0)) / total if total else 0.0,
        }
        for key, label in PERIOD_BUCKETS.items()
    ]
    rows.append({"period_bucket": "Total", "trips": total, "proportion": 1.0 if total else 0.0})
    return pd.DataFrame(rows)


def _purpose_table_from_grouped(
    grouped: pd.DataFrame,
    purposes: pd.DataFrame,
    period_key: str | None = None,
) -> pd.DataFrame:
    data = grouped if period_key is None else grouped[grouped["period_key"] == period_key]
    if data.empty:
        out = purposes.copy()
        out["trips"] = 0.0
    else:
        totals = (
            data.groupby(["purpose", "purpose_desc"], as_index=False)["volume_adj_purpose"]
            .sum()
            .rename(columns={"volume_adj_purpose": "trips"})
        )
        out = purposes.merge(totals[["purpose", "trips"]], on="purpose", how="left")
    out["purpose"] = out["purpose"].astype(int)
    out["trips"] = pd.to_numeric(out["trips"], errors="coerce").fillna(0.0)
    out = out.sort_values("purpose").reset_index(drop=True)
    return _with_proportion(out)


def _period_bucket_summary_from_grouped(grouped: pd.DataFrame) -> pd.DataFrame:
    period_totals = (
        grouped[grouped["period_key"].isin(list(PERIOD_BUCKETS))]
        .groupby("period_key")["volume_adj_purpose"]
        .sum()
    )
    total = sum(float(period_totals.get(key, 0.0)) for key in PERIOD_BUCKETS)
    rows = [
        {
            "period_bucket": label,
            "trips": float(period_totals.get(key, 0.0)),
            "proportion": float(period_totals.get(key, 0.0)) / total if total else 0.0,
        }
        for key, label in PERIOD_BUCKETS.items()
    ]
    rows.append({"period_bucket": "Total", "trips": total, "proportion": 1.0 if total else 0.0})
    return pd.DataFrame(rows)


def _period_weights_from_grouped(grouped: pd.DataFrame) -> pd.DataFrame:
    period_totals = (
        grouped[grouped["period_key"].isin(list(PERIOD_BUCKETS))]
        .groupby("period_key", as_index=False)["volume_adj_purpose"]
        .sum()
        .rename(columns={"volume_adj_purpose": "output_period_trips"})
    )
    total = pd.to_numeric(period_totals["output_period_trips"], errors="coerce").fillna(0.0).sum()
    period_totals["output_period_proportion"] = (
        period_totals["output_period_trips"] / total if total else 0.0
    )
    return period_totals[["period_key", "output_period_proportion"]]


def _purpose_reweighted_by_output_period_table(
    grouped: pd.DataFrame,
    purpose_dims: pd.DataFrame,
    nts_mode_time_split_csv: Path,
) -> pd.DataFrame:
    nts = pd.read_csv(nts_mode_time_split_csv, usecols=NTS_PURPOSE_COLUMNS)
    assert_dataframe_columns(nts, NTS_PURPOSE_COLUMNS, "NTS mode/time split csv")
    nts["period_key"] = pd.to_numeric(nts["period"], errors="coerce").map(NTS_PERIOD_TO_KEY)
    nts["purpose"] = pd.to_numeric(nts["purpose"], errors="coerce")
    nts["trips.est"] = pd.to_numeric(nts["trips.est"], errors="coerce").fillna(0.0)
    nts = nts.dropna(subset=["period_key", "purpose"])
    nts["purpose"] = nts["purpose"].astype(int)

    raw_period = (
        nts.groupby(["period_key", "purpose"], as_index=False)["trips.est"]
        .sum()
        .rename(columns={"trips.est": "raw_period_trips_est"})
    )
    raw_period_total = raw_period.groupby("period_key")["raw_period_trips_est"].transform("sum")
    raw_period["raw_period_purpose_proportion"] = (
        raw_period["raw_period_trips_est"] / raw_period_total
    ).where(raw_period_total > 0, 0.0)

    raw_all = (
        raw_period.groupby("purpose", as_index=False)["raw_period_trips_est"]
        .sum()
        .rename(columns={"raw_period_trips_est": "raw_trips_est"})
    )
    raw_total = pd.to_numeric(raw_all["raw_trips_est"], errors="coerce").fillna(0.0).sum()
    raw_all["raw_proportion"] = raw_all["raw_trips_est"] / raw_total if raw_total else 0.0

    reweighted = raw_period.merge(_period_weights_from_grouped(grouped), on="period_key", how="left")
    reweighted["output_period_proportion"] = pd.to_numeric(
        reweighted["output_period_proportion"],
        errors="coerce",
    ).fillna(0.0)
    reweighted["weighted_raw_proportion"] = (
        reweighted["raw_period_purpose_proportion"] * reweighted["output_period_proportion"]
    )
    reweighted = (
        reweighted.groupby("purpose", as_index=False)["weighted_raw_proportion"]
        .sum()
        .rename(columns={"weighted_raw_proportion": "raw_reweighted_by_output_period_proportion"})
    )

    output = _purpose_table_from_grouped(grouped, purpose_dims).rename(
        columns={
            "trips": "output_trips",
            "proportion": "output_proportion",
        }
    )

    out = (
        purpose_dims.merge(raw_all, on="purpose", how="left")
        .merge(reweighted, on="purpose", how="left")
        .merge(output[["purpose", "output_trips", "output_proportion"]], on="purpose", how="left")
    )
    numeric_cols = [
        "raw_trips_est",
        "raw_proportion",
        "raw_reweighted_by_output_period_proportion",
        "output_trips",
        "output_proportion",
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["raw_minus_output_proportion"] = out["raw_proportion"] - out["output_proportion"]
    out["raw_reweighted_minus_output_proportion"] = (
        out["raw_reweighted_by_output_period_proportion"] - out["output_proportion"]
    )
    out["purpose"] = out["purpose"].astype(int)
    return out[
        [
            "purpose",
            "purpose_desc",
            "raw_trips_est",
            "raw_proportion",
            "raw_reweighted_by_output_period_proportion",
            "output_trips",
            "output_proportion",
            "raw_minus_output_proportion",
            "raw_reweighted_minus_output_proportion",
        ]
    ].sort_values("purpose")


def _purpose_summary_tables(
    purpose_parquet: Path,
    nts_mode_time_split_csv: Path | None = None,
    purposes_csv: Path | None = None,
) -> dict[str, pd.DataFrame]:
    purpose_meta = dd.read_parquet(purpose_parquet)._meta
    columns = PURPOSE_SUMMARY_COLUMNS.copy()
    if "period_key" in purpose_meta.columns:
        columns.append("period_key")
    else:
        columns.extend(["time_period", "weekend_flag"])

    purpose = dd.read_parquet(purpose_parquet, columns=columns)
    assert_dataframe_columns(purpose._meta, PURPOSE_SUMMARY_COLUMNS, "Purpose parquet")
    purpose = _ensure_period_key(purpose)
    purpose["volume_adj_purpose"] = dd.to_numeric(
        purpose["volume_adj_purpose"],
        errors="coerce",
    ).fillna(0.0)

    grouped = (
        purpose.groupby(["period_key", "purpose", "purpose_desc"])["volume_adj_purpose"]
        .sum()
        .reset_index()
        .compute()
    )
    grouped["period_key"] = grouped["period_key"].astype("string")
    grouped["purpose"] = grouped["purpose"].astype(int)
    grouped["purpose_desc"] = grouped["purpose_desc"].astype("string")
    grouped["volume_adj_purpose"] = pd.to_numeric(grouped["volume_adj_purpose"], errors="coerce").fillna(0.0)
    if purposes_csv is not None and purposes_csv.exists():
        purpose_dims = pd.read_csv(purposes_csv).rename(
            columns={"Purpose": "purpose", "Description": "purpose_desc"}
        )
        purpose_dims["purpose"] = pd.to_numeric(purpose_dims["purpose"], errors="coerce")
        purpose_dims = purpose_dims.dropna(subset=["purpose"])
        purpose_dims["purpose"] = purpose_dims["purpose"].astype(int)
        purpose_dims = purpose_dims[["purpose", "purpose_desc"]]
    else:
        purpose_dims = grouped[["purpose", "purpose_desc"]].drop_duplicates()
    purpose_dims = purpose_dims.sort_values("purpose").reset_index(drop=True)

    tables = {
        "all_week_purpose": _purpose_table_from_grouped(grouped, purpose_dims),
        "am_weekday_purpose": _purpose_table_from_grouped(grouped, purpose_dims, "weekday_AM"),
        "other_purpose": _purpose_table_from_grouped(grouped, purpose_dims, "other"),
        "period_bucket_trips": _period_bucket_summary_from_grouped(grouped),
    }
    if nts_mode_time_split_csv is not None and nts_mode_time_split_csv.exists():
        tables["purpose_share_reweighted_by_output_period"] = _purpose_reweighted_by_output_period_table(
            grouped,
            purpose_dims,
            nts_mode_time_split_csv,
        )
    return tables


def write_qa_summary_tables(
    adjusted_parquet: Path,
    output_dir: Path,
    purpose_parquet: Path | None = None,
    nts_mode_time_split_csv: Path | None = None,
    purposes_csv: Path | None = None,
) -> None:
    """Write run-level QA summary tables for adjusted mode and purpose trips."""

    ensure_dir(output_dir)

    adjusted = dd.read_parquet(adjusted_parquet)
    assert_dataframe_columns(adjusted._meta, MODE_SUMMARY_COLUMNS, "Adjusted parquet")
    adjusted["volume_adj"] = dd.to_numeric(adjusted["volume_adj"], errors="coerce").fillna(0.0)
    modes = (
        adjusted["mode_of_transport"]
        .dropna()
        .drop_duplicates()
        .compute()
        .astype(str)
        .sort_values()
        .reset_index(drop=True)
        .to_frame(name="mode")
    )

    tables: dict[str, pd.DataFrame] = {
        "all_week_mode": _mode_summary(adjusted, modes),
        "am_weekday_mode": _mode_summary(adjusted, modes, am_weekday_only=True),
    }

    if purpose_parquet is not None and purpose_parquet.exists():
        tables.update(_purpose_summary_tables(purpose_parquet, nts_mode_time_split_csv, purposes_csv))

    for name, table in tables.items():
        table.to_csv(output_dir / f"{name}.csv", index=False, float_format="%.10f")
