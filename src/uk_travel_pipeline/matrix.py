from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from .config import MatrixConfig
from .io import assert_dataframe_columns, assert_files_exist, ensure_dir


ADJUSTED_REQUIRED_COLUMNS = [
    "origin_msoa",
    "destination_msoa",
    "mode_of_transport",
    "time_period",
    "weekend_flag",
    "days_used",
]


def _write_dense_matrix_chunked(
    df: pd.DataFrame, out_path: Path, value_col: str, origin_chunk_size: int = 400
) -> None:
    if df.empty:
        pd.DataFrame(columns=["origin_msoa"]).to_csv(out_path, index=False)
        return

    destinations = sorted(df["destination_msoa"].dropna().astype(str).unique().tolist())
    origins = sorted(df["origin_msoa"].dropna().astype(str).unique().tolist())
    ensure_dir(out_path.parent)
    first = True
    for i in range(0, len(origins), origin_chunk_size):
        chunk_origins = origins[i : i + origin_chunk_size]
        chunk = df[df["origin_msoa"].isin(chunk_origins)]
        mat = chunk.pivot(index="origin_msoa", columns="destination_msoa", values=value_col)
        mat = mat.reindex(index=chunk_origins, columns=destinations, fill_value=0)
        mat.index.name = "origin_msoa"
        mat = mat.reset_index()
        mat.to_csv(out_path, index=False, mode="w" if first else "a", header=first)
        first = False


def _write_mode_matrices(df: pd.DataFrame, modes: tuple[str, ...], out_dir: Path, value_col: str) -> None:
    ensure_dir(out_dir)
    for mode in modes:
        mode_df = df[df["mode_of_transport"] == mode][["origin_msoa", "destination_msoa", value_col]]
        _write_dense_matrix_chunked(mode_df, out_dir / f"OD_matrix_{mode}_adjusted.csv", value_col=value_col)


def run_matrices(config: MatrixConfig, legacy_output_root: Path | None = None) -> None:
    assert_files_exist([config.adjusted_parquet])
    all_data_dd = dd.read_parquet(config.adjusted_parquet)
    assert_dataframe_columns(all_data_dd._meta, ADJUSTED_REQUIRED_COLUMNS, "Adjusted parquet")

    if "volume_adj" in all_data_dd.columns:
        base_vol = dd.to_numeric(all_data_dd["volume_adj"], errors="coerce").fillna(0)
    elif "volume" in all_data_dd.columns:
        base_vol = dd.to_numeric(all_data_dd["volume"], errors="coerce").fillna(5)
    else:
        raise ValueError("Adjusted parquet must contain `volume_adj` or `volume`.")

    days_used = dd.to_numeric(all_data_dd["days_used"], errors="coerce").replace(0, float("nan"))
    all_data_dd["volume_per_day"] = (base_vol / days_used).fillna(0.0)
    weekend_multiplier = 5 - 3 * all_data_dd["weekend_flag"].astype("int8")
    all_data_dd["volume_per_typical_week"] = all_data_dd["volume_per_day"] * weekend_multiplier

    by_typical_week = (
        all_data_dd.groupby(["origin_msoa", "destination_msoa", "mode_of_transport"])["volume_per_typical_week"]
        .sum()
        .reset_index()
        .compute()
    )
    by_am_peak = (
        all_data_dd[(all_data_dd["weekend_flag"] == 0) & (all_data_dd["time_period"] == "AM_peak")]
        .groupby(["origin_msoa", "destination_msoa", "mode_of_transport"])["volume_per_day"]
        .sum()
        .reset_index()
        .compute()
    )

    typical_dir = config.outputs_root / "matrices" / "typical_week_by_mode"
    ampeak_dir = config.outputs_root / "matrices" / "weekday_AMpeak_by_mode"
    _write_mode_matrices(by_typical_week, config.modes, typical_dir, "volume_per_typical_week")
    _write_mode_matrices(by_am_peak, config.modes, ampeak_dir, "volume_per_day")

    if legacy_output_root is not None:
        legacy_typical = legacy_output_root / "matrices" / "typical_week_by_mode"
        legacy_ampeak = legacy_output_root / "matrices" / "weekday_AMpeak_by_mode"
        _write_mode_matrices(by_typical_week, config.modes, legacy_typical, "volume_per_typical_week")
        _write_mode_matrices(by_am_peak, config.modes, legacy_ampeak, "volume_per_day")
